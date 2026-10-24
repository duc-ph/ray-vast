/*
 * rayvast_shim: LD_PRELOAD library that rewrites bind() and connect() for
 * Ray nodes that advertise a public IP which is not on any local interface.
 *
 * Environment:
 *   RAYVAST_PUBLIC_IP        IP that Ray advertises (not on the interface)
 *   RAYVAST_PRIVATE_IP       interface IP
 *   RAYVAST_LOCALHOST_PORTS  comma list of ports that must listen on 127.0.0.1
 *   RAYVAST_OWN_PORTS        optional comma list: the ports that vast maps to this
 *                            container (a worker sets it; the head does not)
 *   RAYVAST_SHIM_LOG         file; one line per rewrite
 *
 * Rules (IPv4 only; an AF_INET6 address is handled only when it is an
 * IPv4-mapped address ::ffff:a.b.c.d, or :: for rule 1, because gRPC makes
 * dual-stack sockets and passes IPv4 addresses in that form):
 *   1. bind(any, port in LOCALHOST_PORTS)      -> bind(127.0.0.1, port)
 *   2. bind(PUBLIC_IP, port)                   -> bind(PRIVATE_IP, port)
 *   3. connect(PUBLIC_IP, port in LOCALHOST)   -> connect(127.0.0.1, port)
 *   4. connect(PUBLIC_IP, port)                -> connect(PRIVATE_IP, port)
 *      When OWN_PORTS is set, only for a port in OWN_PORTS, or a port that a
 *      TCP socket in this network namespace listens on (/proc/net/tcp and
 *      /proc/net/tcp6). Another container can have the same public IP (two
 *      containers on one host, or two hosts behind one NAT IP), and it has
 *      its own network namespace. A connect to one of its ports goes to the
 *      public IP unchanged, and hairpin NAT takes it to that container.
 *   5. everything else passes through unchanged.
 *
 * A bind for a port in LOCALHOST_PORTS (rule 1, or a bind that is already to
 * 127.0.0.1) also gets SO_REUSEPORT. Ray's dashboard agent binds its HTTP
 * port twice, to the node IP and to 127.0.0.1. Rule 1 makes both binds
 * 127.0.0.1, and without SO_REUSEPORT the second one fails with EADDRINUSE.
 * Both sockets belong to the same aiohttp app, so the kernel can give a new
 * connection to either one.
 *
 * Build: gcc -shared -fPIC -O2 -o librayvast_shim.so rayvast_shim.c -ldl
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define MAX_LOCALHOST_PORTS 128
#define MAX_OWN_PORTS 128

typedef int (*sock_fn)(int, const struct sockaddr *, socklen_t);

/* Written once by init(). A race between two threads in init() only writes
 * the same values twice, so a plain flag is sufficient. */
static sock_fn real_bind;
static sock_fn real_connect;
static int have_public;
static int have_private;
static struct in_addr public_ip;
static struct in_addr private_ip;
static unsigned short localhost_ports[MAX_LOCALHOST_PORTS];
static int n_localhost_ports;
static int have_own_ports;
static unsigned short own_ports[MAX_OWN_PORTS];
static int n_own_ports;
/* A copy, not the getenv() pointer: setproctitle (used by Ray's Python
 * processes) overwrites the original environ strings. */
static char log_path[512];
static volatile int initialized;

/* Parse a comma list of ports into `ports`; return the count. */
static int parse_ports(const char *s, unsigned short *ports, int max)
{
    int n = 0;
    while (s && *s && n < max) {
        char *end;
        long p = strtol(s, &end, 10);
        if (end != s && p > 0 && p < 65536)
            ports[n++] = (unsigned short)p;
        s = strchr(end, ',');
        if (s)
            s++;
    }
    return n;
}

static int in_ports(unsigned short port, const unsigned short *ports, int n)
{
    for (int i = 0; i < n; i++)
        if (ports[i] == port)
            return 1;
    return 0;
}

/* 1 if a TCP socket in this network namespace is in LISTEN state on `port`.
 * A row of /proc/net/tcp{,6}: "sl: local_addr:PORT rem_addr:port st ...",
 * hex numbers; st 0A is LISTEN. Keeps errno. */
static int port_listening(unsigned short port)
{
    static const char *const tables[] = {"/proc/net/tcp", "/proc/net/tcp6"};
    int saved_errno = errno, found = 0;
    for (int t = 0; t < 2 && !found; t++) {
        int fd = open(tables[t], O_RDONLY | O_CLOEXEC);
        if (fd < 0)
            continue;
        char buf[8192];
        size_t have = 0;
        ssize_t n;
        while (!found && (n = read(fd, buf + have, sizeof buf - 1 - have)) > 0) {
            have += (size_t)n;
            buf[have] = 0;
            char *line = buf, *nl;
            while (!found && (nl = memchr(line, '\n', buf + have - line))) {
                *nl = 0;
                unsigned int local_port, st;
                if (sscanf(line, " %*d: %*[0-9A-Fa-f]:%x %*s %x", &local_port, &st) == 2
                    && st == 0x0A && local_port == port)
                    found = 1;
                line = nl + 1;
            }
            have = (size_t)(buf + have - line);
            memmove(buf, line, have);
            if (have == sizeof buf - 1)  /* a line longer than the buffer: drop it */
                have = 0;
        }
        close(fd);
    }
    errno = saved_errno;
    return found;
}

static void init(void)
{
    if (initialized)
        return;
    int saved_errno = errno;
    real_bind = (sock_fn)dlsym(RTLD_NEXT, "bind");
    real_connect = (sock_fn)dlsym(RTLD_NEXT, "connect");

    const char *s = getenv("RAYVAST_PUBLIC_IP");
    have_public = s && inet_pton(AF_INET, s, &public_ip) == 1;
    s = getenv("RAYVAST_PRIVATE_IP");
    have_private = s && inet_pton(AF_INET, s, &private_ip) == 1;

    n_localhost_ports = parse_ports(getenv("RAYVAST_LOCALHOST_PORTS"), localhost_ports, MAX_LOCALHOST_PORTS);
    s = getenv("RAYVAST_OWN_PORTS");
    have_own_ports = s != NULL;
    n_own_ports = parse_ports(s, own_ports, MAX_OWN_PORTS);

    s = getenv("RAYVAST_SHIM_LOG");
    log_path[0] = 0;
    if (s && strlen(s) < sizeof log_path)
        strcpy(log_path, s);
    __sync_synchronize();
    initialized = 1;
    errno = saved_errno;
}

__attribute__((constructor)) static void shim_constructor(void) { init(); }

/*
 * Decide the rewrite for one call. Returns 1 and fills *out if the address
 * must change, with *out_len set to the size of the struct that was copied
 * (so no uninitialized bytes are passed on). is_bind selects rules 1-2,
 * else rules 3-4. *loopback_bind is set to 1 for a bind to a LOCALHOST_PORTS
 * port, also when the address does not change.
 */
static int rewrite(const struct sockaddr *addr, socklen_t len, int is_bind,
                   struct sockaddr_storage *out, socklen_t *out_len,
                   int *loopback_bind)
{
    *loopback_bind = 0;
    if (!addr)
        return 0;

    struct in_addr *ip4;   /* points into *out */
    unsigned short port;
    int v6_any = 0;

    if (addr->sa_family == AF_INET && len >= sizeof(struct sockaddr_in)) {
        memcpy(out, addr, sizeof(struct sockaddr_in));
        *out_len = sizeof(struct sockaddr_in);
        struct sockaddr_in *sin = (struct sockaddr_in *)out;
        ip4 = &sin->sin_addr;
        port = ntohs(sin->sin_port);
    } else if (addr->sa_family == AF_INET6 && len >= sizeof(struct sockaddr_in6)) {
        memcpy(out, addr, sizeof(struct sockaddr_in6));
        *out_len = sizeof(struct sockaddr_in6);
        struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)out;
        v6_any = IN6_IS_ADDR_UNSPECIFIED(&sin6->sin6_addr);
        if (!IN6_IS_ADDR_V4MAPPED(&sin6->sin6_addr) && !v6_any)
            return 0;
        ip4 = (struct in_addr *)&sin6->sin6_addr.s6_addr[12];
        port = ntohs(sin6->sin6_port);
    } else {
        return 0;
    }

    int is_localhost_port = in_ports(port, localhost_ports, n_localhost_ports);
    int is_public = !v6_any && have_public && ip4->s_addr == public_ip.s_addr;

    struct in_addr target;
    if (is_bind && is_localhost_port) {                    /* rule 1 */
        target.s_addr = htonl(INADDR_LOOPBACK);
        *loopback_bind = 1;
    } else if (is_public && !is_bind && is_localhost_port) { /* rule 3 */
        target.s_addr = htonl(INADDR_LOOPBACK);
    } else if (is_public && have_private && (is_bind || !have_own_ports
                                             || in_ports(port, own_ports, n_own_ports)
                                             || port_listening(port))) {
        target = private_ip;                                 /* rules 2, 4 */
    } else {
        return 0;                                            /* rule 5 */
    }
    if (!v6_any && ip4->s_addr == target.s_addr)
        return 0;

    if (v6_any) {
        /* :: -> ::ffff:127.0.0.1 (only reached for rule 1) */
        struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)out;
        memset(sin6->sin6_addr.s6_addr, 0, 10);
        sin6->sin6_addr.s6_addr[10] = 0xff;
        sin6->sin6_addr.s6_addr[11] = 0xff;
    }
    *ip4 = target;
    return 1;
}

static void log_rewrite(const char *call, const struct sockaddr *old_addr,
                        const struct sockaddr_storage *new_addr)
{
    if (!log_path[0])
        return;
    int saved_errno = errno;

    char old_s[INET6_ADDRSTRLEN], new_s[INET6_ADDRSTRLEN];
    unsigned short port;
    const char *af;
    if (old_addr->sa_family == AF_INET) {
        const struct sockaddr_in *o = (const struct sockaddr_in *)old_addr;
        const struct sockaddr_in *n = (const struct sockaddr_in *)new_addr;
        inet_ntop(AF_INET, &o->sin_addr, old_s, sizeof old_s);
        inet_ntop(AF_INET, &n->sin_addr, new_s, sizeof new_s);
        port = ntohs(o->sin_port);
        af = "inet";
    } else {
        const struct sockaddr_in6 *o = (const struct sockaddr_in6 *)old_addr;
        const struct sockaddr_in6 *n = (const struct sockaddr_in6 *)new_addr;
        inet_ntop(AF_INET6, &o->sin6_addr, old_s, sizeof old_s);
        inet_ntop(AF_INET6, &n->sin6_addr, new_s, sizeof new_s);
        port = ntohs(o->sin6_port);
        af = "inet6";
    }

    char comm[32] = "?";
    int cfd = open("/proc/self/comm", O_RDONLY | O_CLOEXEC);
    if (cfd >= 0) {
        ssize_t n = read(cfd, comm, sizeof comm - 1);
        close(cfd);
        if (n > 0) {
            comm[n] = 0;
            char *nl = strchr(comm, '\n');
            if (nl)
                *nl = 0;
            for (char *c = comm; *c; c++)
                if (*c == ' ')
                    *c = '_';
        } else {
            strcpy(comm, "?");
        }
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm;
    gmtime_r(&ts.tv_sec, &tm);

    /* One write() per line on an O_APPEND fd: lines from many threads and
     * processes do not interleave. */
    char line[512];
    int len = snprintf(line, sizeof line,
                       "%04d-%02d-%02dT%02d:%02d:%02d.%03ldZ %d %s %s %s:%u -> %s:%u af=%s\n",
                       tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
                       tm.tm_min, tm.tm_sec, ts.tv_nsec / 1000000, (int)getpid(),
                       comm, call, old_s, port, new_s, port, af);
    int fd = open(log_path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
    if (fd >= 0) {
        if (len > 0) {
            ssize_t w = write(fd, line, len < (int)sizeof line ? (size_t)len : sizeof line - 1);
            (void)w;
        }
        close(fd);
    }
    errno = saved_errno;
}

int bind(int fd, const struct sockaddr *addr, socklen_t len)
{
    init();
    struct sockaddr_storage new_addr;
    socklen_t new_len;
    int loopback_bind;
    int changed = rewrite(addr, len, 1, &new_addr, &new_len, &loopback_bind);
    if (loopback_bind) {
        int saved_errno = errno, one = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
        errno = saved_errno;
    }
    if (changed) {
        log_rewrite(loopback_bind ? "bind+reuseport" : "bind", addr, &new_addr);
        return real_bind(fd, (const struct sockaddr *)&new_addr, new_len);
    }
    if (loopback_bind)
        log_rewrite("bind+reuseport", addr, (const struct sockaddr_storage *)addr);
    return real_bind(fd, addr, len);
}

int connect(int fd, const struct sockaddr *addr, socklen_t len)
{
    init();
    struct sockaddr_storage new_addr;
    socklen_t new_len;
    int loopback_bind;
    if (rewrite(addr, len, 0, &new_addr, &new_len, &loopback_bind)) {
        log_rewrite("connect", addr, &new_addr);
        return real_connect(fd, (const struct sockaddr *)&new_addr, new_len);
    }
    return real_connect(fd, addr, len);
}
