2026-10-12T22:05:20Z C0.setup1 PASS created sg-04966d2ef911b874d in vpc-0bf24a4ceed23db37 with tcp22 from 172.59.160.24/32 only
2026-10-12T22:07:51Z C0.setup2 PASS i-0c343fdb37b555119 t3.large ami-03d70b8354a16a8bc (ubuntu-noble-24.04-20261004; via describe-images, no SSM perm) launched 22:05:27Z, status ok 22:07:34Z; HEAD_IP=35.85.154.224 HEAD_PRIVATE_IP=172.31.31.46
2026-10-12T22:08:32Z C0.setup3 PASS docker.io server 29.1.3, tcpdump 4.99.4, jq 1.7, openssl 3.0.13 installed (24 s)
2026-10-12T22:08:32Z C0.setup4 PASS /opt/rayvast-test/{shim,results,secrets(700),logs} created
2026-10-12T22:10:15Z C0.setup5 PASS shim built in manylinux2014 (glibc 2.17, gcc 10.2.1), -Wall -Wextra clean; nm -D: T bind, T connect, U dlsym; max GLIBC_2.17; sha256 dc4288ef...
2026-10-12T22:10:15Z C0.setup6 PASS pull rayproject/ray:2.59.0-py311-cpu in 36.4 s on EC2; digest sha256:77366add...45c3; compressed amd64 951,297,021 B (9 layers); unpacked 3.54 GB; ray 2.59.0 py 3.11.16; PyPI latest still 2.59.0
2026-10-12T22:10:53Z C0.1.1 PASS no shim: "OSError: [Errno 99] Cannot assign requested address"
2026-10-12T22:10:53Z C0.1.2 PASS shim: bound to 127.0.0.1:5000, ss LISTEN 127.0.0.1:5000, 1 bind line
2026-10-12T22:10:53Z C0.1.3 PASS curl (C path) code 200; log: 1 connect line (curl 203.0.113.10:5000 -> 127.0.0.1:5000) + 1 bind line from http.server
2026-10-12T22:10:53Z C0.1.4 PASS LOCALHOST_PORTS=5001: ss LISTEN 127.0.0.1:5001 (not 0.0.0.0)
2026-10-12T22:10:53Z C0.1.extra PASS errno kept (ECONNREFUSED 111 through rewrite); v4-mapped v6 bind/connect rewritten; unrelated https passes with 0 log lines
2026-10-12T22:18:57Z C0.2.1 DIFF no shim: ray start did NOT reach Errno 99; CLI dialed GCS at 203.0.113.10:6379 (SYN-SENT, unroutable TEST-NET), hung ~5 min, then "RuntimeError: Failed to start GCS". Expected failure, different mode
2026-10-12T22:18:57Z C0.2.1b PASS variant: temp iptables DNAT 203.0.113.10->172.31.31.46 (removed after): ray start rc=0, runtime_env_agent "sock.bind(sockaddr) OSError: [Errno 99]", raylet "exited immediately because one Ray agent failed, agent_name = runtime_env_agent" -> [raylet] <defunct>
2026-10-12T22:19:48Z C0.2.2 PASS shim start: ray status 1 node (2 CPU); shim mapped in all 19 Ray procs incl. gcs_server, raylet, ray::DashboardA, ray::RuntimeEnv, 9 ray-dashboard-*; those 9 show no LD_PRELOAD in /proc/environ because setproctitle overwrote the environ area (forkserver children; shim mapped=yes)
2026-10-12T22:24:39Z C0.2.3 PASS (shim v1) driver: 20 tasks 0.5s, actor 10 calls 0.6s, pip emoji 2.12.1 4.7s, DRIVER OK
2026-10-12T22:24:39Z C0.2.4 DIFF (shim v1) dashboard agent binds :6384 twice (node IP, then 127.0.0.1); rule 1 makes both 127.0.0.1 -> EADDRINUSE x6, "HTTP service will be disabled"; extra check ray job submit -> "status code 500: No available agent to submit job"
2026-10-12T22:24:39Z C0.shim-v2 FIX shim v2: SO_REUSEPORT on binds to LOCALHOST_PORTS ports; C0.1 regression PASS + new dual-bind check PASS (v1 gives Errno 98)
2026-10-12T22:24:39Z C0.2.2-4 PASS (shim v2 rerun) 1 node 2 CPU; driver OK (0.5s/0.6s/4.7s); 174 rewrite lines, 0 Failed-to-bind; ss: 6384 on 127.0.0.1 x2, 8265 127.0.0.1, 6385 on 172.31.31.46; ray job submit SUCCEEDED
2026-10-12T22:24:39Z C0.teardown PASS ray stop --force; container rayvast-c0 stopped; shim + image kept. C0 PASS (with shim v2)
2026-10-12T22:25:36Z C1.setup1 PASS CA (CN rayvast-test-ca-20261012, 2 days), 32-byte hex token, head cert SAN IP:35.85.154.224,IP:127.0.0.1 verify OK; /opt/rayvast-test/secrets/* mode 600
2026-10-12T22:25:36Z C1.setup2 PASS /opt/rayvast-test/head.env written (TLS+token paths, LD_PRELOAD, RAYVAST_* PUBLIC=35.85.154.224 PRIVATE=172.31.31.46 LOCALHOST_PORTS=6384); added LD_PRELOAD (plan lists only shim vars)
2026-10-12T22:26:09Z C1.1 PASS rayvast-head container up (--network host, --env-file head.env); in-container ray 2.59.0 / py 3.11.16
2026-10-12T22:26:09Z C1.2 PASS ray status: 1 node (node_55141e36...), no CPU row (CPU 0 omitted), 4.99GiB mem; shim mapped in gcs_server, raylet, ray::DashboardA, ray::RuntimeEnv, 9 ray-dashboard-*, 6 python3.11
2026-10-12T22:26:18Z C1.3 PASS ss: 6384 127.0.0.1 (x2, SO_REUSEPORT), 8265 127.0.0.1, 6385 172.31.31.46, 6379/6380/6381/6383 *, 6382 0.0.0.0, 10001 ::ffff:172.31.31.46 + ::ffff:127.0.0.1; unlisted 44217 (monitor.py) 44227 (dashboard.py) on 0.0.0.0; no Errno 99, 0 defunct
2026-10-12T22:26:18Z C1.4 PASS curl 127.0.0.1:6384/api/jobs/ no token 401 (host curl + container urllib; image has no curl), wrong token 403, right token 404 (auth passed, not an agent route); private IP:6384 refused
2026-10-12T22:30:32Z C1.5 PASS (shim v2) driver num_cpus=0: tasks 1.0s, actor 0.4s, pip 4.7s; job raysubmit_88aVhtGDWpunQMS1 succeeded; 0 packets on ens5 with HEAD_IP; 209 connect lines to HEAD_IP all -> 172.31.31.46
2026-10-12T22:30:32Z C1.5 DIFF shim v2 log missing agent bind lines: shim kept the getenv() pointer for RAYVAST_SHIM_LOG; Ray's setproctitle overwrites environ, so renamed procs (ray::*, ray-dashboard-*) lost their log lines (rewrites still applied, ss confirms)
2026-10-12T22:30:32Z C0.shim-v3 FIX copy log path at init; new unit check extra e (ray._raylet.setproctitle, 3000-char title): v3 1 line, v2 0 lines; C0.1 all PASS on v3
2026-10-12T22:30:32Z C0.2.2-5 PASS (shim v3 rerun) 1 node 2 CPU; driver 0.5s/0.6s/4.6s; 225 log lines (v2: 174), ray-dashboard-* now logged; job raysubmit_76G8Pmt9eEMRqT9f succeeded; container stopped again
2026-10-12T22:32:30Z C1.1-5 PASS (shim v3 rerun) 1 node; ss same as before; 401/403/404; driver 1.0s/0.4s/4.6s; job raysubmit_knZnUqxGS3fb6Usp succeeded; 0 pkts on ens5 with HEAD_IP; 329 connect lines to HEAD_IP -> 172.31.31.46 (327) or 127.0.0.1 (2, ray-dashboard-J :6384); laptop->6379/8265/6384 filtered
2026-10-12T22:32:30Z C1.end KEPT RUNNING for C2: i-0c343fdb37b555119 (35.85.154.224), container rayvast-head with Ray head up; results copied to results/2026-10-12/remote (no secrets); state.json written
2026-10-12T22:33:34Z C1.reuseport-check PASS both 127.0.0.1:6384 listeners are pid 14429 (ray::DashboardA), fd 37 ino 55623 + fd 38 ino 55624, uid 1000; 20/20 no-token requests -> 401; agent log 'Dashboard agent http address: 127.0.0.1:6384', no bind failures
2026-10-12T22:46:35Z C2.credit PASS vast credit at C2 start = $12.0547 (stop if credit < $4.05); 0 vast instances; head rayvast-head up 1 node
2026-10-12T22:46:35Z C2.setup1 PASS offer query matched 9 offers (6 cheapest: 48743418 P100 TX $0.097 down924; 48190497 5060Ti DE $0.178 down881 static; 13273575 A5000 RO $0.233 down882 static; 19951401 A5000 RO(same host 1647); 50417801 5060Ti TX $0.269 down8164; 46528521 3060 CZ $0.272); user approval delegated by lead ('i trust your reasoning'); chose 3 cheapest on 3 different hosts (33081, 62819, 1647); c2_offers.json
2026-10-12T22:52:24Z C2.setup2 PASS rented 55632730 (offer 48743418, machine 3969, P100, TX, 143.55.45.86), 55632732 (48190497, m148075, 5060Ti, DE, 85.10.218.46), 55632733 (13273575, m29217, A5000, RO, 5.2.174.20); PUT /asks/<id>/ body {client_id:me,image,label,disk:20,runtype:args,args:[bash,-c,script],env:{'-p 700xx:700xx':'1' x64, RAYVAST_HEAD_SSH_PUBKEY}} -> c2/create_requests.jsonl
2026-10-12T22:52:24Z C2.setup2 DIFF SECURITY: the create response returns 'instance_api_key' (64 hex, per-instance vast key); it was written to c2/rent_console.txt + create_requests.jsonl and then redacted. Design must treat the create response as secret and never log it. Also: vast metadata shows extra_env + image_args (our args script) in clear text, and instance logs go to a public S3 URL (request_logs) - so the plan rule 'no secrets in env/args' is essential
2026-10-12T22:52:24Z C2.setup3 DIFF T8: create->running 64-65 s on all 3 (pull incl.; 0.95 GB image, inet_down 881-924), boot script start 22:48:16-32Z, sshd up 14-31 s after boot (apt install openssh-server); all within limits (pull limit 300 s, sshd 60 s). DIFF: c2_rent.py crashed at 'running' because the ports dict is keyed by host port P (identity), not '70040/tcp' -> the sshd port P(70040) is NOT in the metadata; found it by scanning the 64 ports for an SSH banner (730:42778, 732:37760, 733:22682). Ports not contiguous (e.g. 42002,42004,42035 in range 42002-42980). Exact ssh-up time for 730 is between 22:48:46 and 22:49:30Z (crash gap)
2026-10-12T22:52:24Z C2.setup4 PASS host checks all 3: uname x86_64, ray 2.59.0 py 3.11.16, identity mapping 64/64, head->63/63 ports connect, egress==PUBLIC_IPADDR, CapEff(ray)=0 root CapEff=CapBnd=0xa80405fb (NET_ADMIN bit12=0, NET_RAW bit13=0), hostname -I 172.17.0.x (own netns), /dev/shm empty. HAIRPIN: PASS on all 3 (no W-nohp). DIFF: SSH session PATH has no anaconda; must call /home/ray/anaconda3/bin/{python,ray} (system python3 is 3.10 w/o ray). files c2/hostcheck_*.json
2026-10-12T22:52:42Z C2.setup5 DIFF no host without hairpin in 3 -> keep 55632730 as W-hp (cheapest, $0.103/h), destroyed 55632732+55632733 (success). Re-search: only 2 NEW hosts match the query (host 152538 offer 49766585 TX $0.229; host 3497 offer 46528521 CZ $0.272); the rest are hosts already tested -> rent only these 2 (not 3). c2_rent.py fixed: sshd port found by banner scan; instance_api_key dropped before logging
2026-10-12T22:56:19Z C2.setup3b PASS T8 55633384 (m28701 5060Ti TX inet_down 7453): running 65 s, ssh 68 s; 55633386 (m32816 RTX3060 CZ inet_down 877): running 77 s, ssh 109 s (sshd 26 s after boot start)
2026-10-12T22:56:19Z C2.setup5 PASS host checks 55633384: all pass, hairpin PASS; 55633386 (185.62.108.226): all pass, HAIRPIN FAIL (ConnectionRefused 111 to own PUBLIC_IPADDR:P(70006)). W-hp=55632730 (143.55.45.86, m3969), W-nohp=55633386 (185.62.108.226, m32816). Destroyed 55633384
2026-10-12T22:58:20Z C2.1.2 PASS secret push over SSH (P(70040), user ray, head key ~/.ssh/rayvast_head): ca.crt node.crt node.key token worker.env mode 600, shim v3 sha 79055dea... + worker_start.sh to /opt/rayvast/ on W-hp and W-nohp; node certs verify OK, SAN IP:<worker ip>,IP:127.0.0.1. c2/c2_1_push.txt
2026-10-12T22:58:20Z C2.1.1 FAIL(blocked) SG ingress for 143.55.45.86/32 + 185.62.108.226/32 denied by the Claude Code auto-mode permission classifier ('Modify Shared Resources'); asked lead for permission; waiting
2026-10-12T22:59:32Z C2.1.1 PASS lead added SG rules sgr-05d5525b33fd34316 (143.55.45.86/32) + sgr-0b74d36c743a87b62 (185.62.108.226/32) after user approval. Wider W-nohp search not needed: W-nohp found in datacenter host 5 of 5 (hairpin 4/5 datacenter hosts)
2026-10-12T23:00:28Z C2.1.3-4 PASS ray start on both workers (LOCALHOST_PORTS=P(70004): W-hp 42838, W-nohp 45136; 57 worker ports each; PRIVATE 172.17.0.2/.3); ray status 3 nodes ALIVE, 16 CPU 2 GPU (P100, RTX-3060 auto-detected), obj store 10.45/9.31 GiB. c2/c2_1_join.txt
2026-10-12T23:00:28Z C2.1.5 PASS GET /instances/<id>/ for both workers (12.2 KB each): token 0 hits, node.key/ca.key bodies 0 hits, 'PRIVATE KEY' 0 hits. (Metadata does contain vast's own jupyter_token, extra_env with our pubkey, image_args script)
2026-10-12T23:03:14Z C2.2 PASS battery 133 s (both workers): 1) SPREAD 200 tasks W-hp 192 / W-nohp 8 (DIFF: very uneven, both ran tasks; 7.6 s); 2) actor RTT median/p95 W-hp(TX) 65.7/66.1 ms, W-nohp(CZ) 170.8/171.3 ms; 3) pip emoji 2.12.1 W-hp 9.5 s W-nohp 10.8 s; 4) 50MB MB/s head->hp 27.8, hp->head 3.2(!), head->nohp 5.8, nohp->head 12.4, hp->nohp 13.1, nohp->hp 13.5; 5) ray list actors/tasks rc 0; 6) job raysubmit_ggrLE2PsgSfpGYnJ succeeded (battery's own SUCCEEDED check was case-wrong, fixed). c2/c2_2_battery.txt
2026-10-12T23:03:14Z C2.2 T1 PASS shim log W-nohp: raylet connect to own PUBLIC_IP rewritten to 172.17.0.3 for 19 distinct ports (worker ports + runtime-env agent 45832), each also as ::ffff: form; W-hp same (172.17.0.2, 16 ports); RuntimeEnv bind public->private; DashboardA job port bind+reuseport -> 127.0.0.1 x2; 0 Errno 99/Failed to bind in logs. DIFF: image has no 'ss' (iproute2) -> installing it with sudo apt for C2.4/C2.5. c2/c2_2_shimlog.txt
2026-10-12T23:04:12Z C2.3 PASS both workers, from head host: 1) plaintext h2 to raylet P(70000) curl rc=56 (reset); also object mgr + agent gRPC rc=56; 2) TLS: openssl s_client prints 'New, TLSv1.3' for no-cert and rogue-CA (DIFF: TLS1.3 checks client cert after client Finished, so s_client is not decisive) -> added c2_tls_probe.py (h2 preface after handshake): no cert -> server closes, 0 bytes; rogue CA -> closes, 0 bytes; control with valid head cert -> SETTINGS frame; 3) runtime-env P(70005) no token 401, wrong 403; 4) job port P(70004) from head: connection refused (curl rc 7); 5) worker 127.0.0.1:P(70004) python: no token 401, wrong 403; 6) metrics P(70002) 200 with no auth (587/610 lines, ~380 ray_ series) - known gap. c2/c2_3_negative.txt
2026-10-12T23:07:03Z C2.4 PASS T4: battery rerun with humanize==4.10.0 (W-hp 8.8 s, W-nohp 10.2 s, version 4.10.0). head-hop.pcap (any, tcp 6384/6385): 42 packets, all 'lo In'; token in 4 packets, all on lo. head-ens5.pcap (all non-22 TCP on ens5, 268 MB): 0 token matches, 0 'Bearer'. Workers ss -tnp 1 Hz: raylet->RuntimeEnv only 172.17.0.2:43978->172.17.0.2:42975 (W-hp) and 172.17.0.3:50140->172.17.0.3:45832 (W-nohp); never PUBLIC_IPADDR. NOTE: head-hop.pcap contains the token (lo) -> not copied to repo. c2/c2_4_hop.txt
2026-10-12T23:10:40Z C2.5 PASS T2 (both ports): ray stop/start in place with LOCALHOST_PORTS=P(70004),P(70005); ss: W-hp 127.0.0.1:42838 x2 + 127.0.0.1:42975, W-nohp 127.0.0.1:45136 x2 + 127.0.0.1:45832; RuntimeEnv 'bind+reuseport public->127.0.0.1'; raylet connect -> 127.0.0.1:P(70005); nodes ALIVE (new ids; old ids DEAD from ray stop). Battery 127 s all pass: SPREAD 192/8, RTT med/p95 hp 66.3/68.1 nohp 170.8/171.1, pip 9.8/10.3 s, MB/s head->hp 21.4 hp->head 4.4 head->nohp 6.3 nohp->head 12.4 hp->nohp 14.3 nohp->hp 16.5, list rc 0, job succeeded. From head: curl W:P(70005) refused (rc 7) on both. c2/c2_5_*.txt
2026-10-12T23:13:45Z C2.6.1-2 DIFF T11 tasks: W-hp restarted with --worker-port-list of 4 ports + RAY_num_workers_soft_limit=4. 8 tasks (num_cpus=1, sleep 60, pinned): 4 started at +4 s, other 4 WAITED and ran at +64 s; all 8 OK (124 s). No task failed. Extra worker processes died: 'Failed to register worker to Raylet: Invalid: No available ports. Please specify a wider port range' (~13 occurrences), raylet 'Some workers ... have not registered within the timeout. The process is dead'. Raylet stayed ALIVE. Expected 'fail after 5 retries' -> real: tasks queue until a port frees. c2/c2_6_tasks.txt
2026-10-12T23:24:18Z C2.6.3 DIFF T11 actors: 8 actors (num_cpus=0, pinned W-hp, 4 ports): #0-3 ready at +64 s (not at once: the 4 idle task workers from step 2 held the 4 ports for ~60 s idle time); #4-7 FAILED at +360 s with ActorUnschedulableError 'Could not create the actor because worker startup repeatedly failed. Failed to startup worker after retrying 5 times. This is most often caused by errors in the worker's Python environment setup...' (misleading text; real cause in worker log = 'No available ports'). Raylet ALIVE throughout. c2/c2_6_actors_driver.txt
2026-10-12T23:24:18Z C2.6.4 PASS T11 driver: with 4 actors holding all 4 ports, 'ray.init(address=auto)' on W-hp connects to GCS, then fails in 2 s: core_worker_process.cc:247 'Failed to register worker to Raylet: Invalid: No available ports' rc=1 -> drivers use the same port pool. Raylet ALIVE
2026-10-12T23:26:13Z C2.6.5 PASS restore W-hp: ray stop/start with 57 ports + LOCALHOST_PORTS=P(70004),P(70005); node 9b30b3c3 ALIVE; ray_driver.py: 20 tasks 5.3 s, actor 2.6 s, pip 6.9 s, DRIVER OK
2026-10-12T23:26:13Z C2.teardown PASS W-nohp logs saved (c2/wnohp/wnohp_logs.tgz: shim logs + ray session logs), W-nohp 55633386 destroyed 23:26:00Z (success); SG rule sgr-0b74d36c743a87b62 revoke requested from lead. Credit 11.7731 -> C2 vast cost so far $0.28 (incl. 4 destroyed candidates). W-hp 55632730 + head kept for C3
2026-10-12T23:26:23Z C3.setup PASS credit 11.7731 at C3 start. W-loss offer 44053751 (m142607, 5060Ti, DE, $0.178, inet_down 6620); skipped the cheaper 48743420 because it is on W-hp's machine 3969 (same public IP -> same SG rule). c3_offers.json
2026-10-12T23:29:42Z C3.setup PASS W-loss 55638058 (85.10.218.46, same NAT IP as C2 candidate 55632732, host 62819): running 54 s, ssh 77 s (T8, inet_down 6620); host checks all pass, hairpin PASS; secret push OK (cert SAN IP:85.10.218.46). SG rule requested from lead
2026-10-12T23:32:36Z C3.1.1 DIFF+FIX scoped key: the docs format ('api.instance.destroy': {constraints}) is REFUSED by POST /api/v0/auth/apikeys/ ('You are not authorized to create an API key with these permissions'), also with string id / method nesting / key_params. ACCEPTED format: {'api':{'instance_read':{'api.instance':{'GET':{'constraints':{'id':{'eq':55632730}}}}},'instance_write':{'api.instance':{'DELETE':{'constraints':{'id':{'eq':55632730}}}}}}}. Key id 32488282 (name rayvast-test-20261012-whp). 4 probe keys made while finding the format were deleted (32488184/85/217/259, 'Successfully Deleted API Key'). c3/scoped_key_request.json
2026-10-12T23:32:36Z C3.1.1 DIFF SECURITY every created key also gets a fixed '*' group: api.users.current GET, api.instances GET (list ALL account instances), api.auth.apikeys GET, api.auth.sessions GET, api.ssh.keys (ALL methods -> can add an SSH key to the account), api.team POST, api.template GET, etc. It cannot be removed by the request
2026-10-12T23:32:36Z C3.1.2-3 DIFF scoped key (from laptop): GET own 200; GET W-loss 401 'constraint on id'; DELETE W-loss 401 (W-loss still running); GET users/current 200 (credit+email visible; plan expected refused); offer search 200 (also 200 with NO auth: public endpoint); GET /instances/ 200 lists both instances; GET auth/apikeys 200 lists 5 keys, 0 key values; GET /ssh/ 200 (2 keys). c3/c3_1_scoped_key.txt
2026-10-12T23:32:36Z C3.1.x PASS(mostly) per-instance key: vast puts it in the container env as CONTAINER_API_KEY (len 64, readable by any uid-1000 process via /proc/1/environ, i.e. by any Ray task); also JUPYTER_TOKEN, OPEN_BUTTON_TOKEN, PUBLIC_KEY/SSH_PUBLIC_KEY (account ssh pubkey) in env. From W-hp with CONTAINER_API_KEY: GET own 200; GET W-loss 401; DELETE W-loss 401; users/current 401; auth/apikeys 401 (needs user_read); /ssh/ 401; offer search 200 (public); GET /instances/ 200 LISTS ALL account instances (leak of topology/metadata incl. other instances' extra_env). Tighter than our scoped key -> watchdog uses CONTAINER_API_KEY first (scoped key = fallback). c3/c3_1_instance_key.txt
2026-10-12T23:33:03Z C3.setup PASS SG: lead revoked sgr-0b74d36c743a87b62 (W-nohp) and added sgr-0125855ab4e02800e (85.10.218.46/32) for W-loss
2026-10-12T23:33:47Z C3.setup PASS machine ids from API: W-loss 55638058 machine 142607 host 62819 local 192.168.0.112; C2 candidate 55632732 machine 148075 host 62819 -> two machine_ids share public IP 85.10.218.46 (same host_id 62819, datacenter NAT). Blocklist by machine_id + SG rule by IP must handle a shared IP. W-loss joined: node 2527adf0 ALIVE (3 nodes)
2026-10-12T23:35:08Z C3.2 PASS T7: DELETE W-loss 55638058 (full key) t0=23:34:00.821Z; ray list nodes 5 s poll: ALIVE at 23:34:36.75Z, DEAD at 23:34:43.60Z; GCS end_time 23:34:42.215Z -> t1-t0 = 41.4 s (expected ~30 s; includes vast's destroy latency, unknown). state_message 'Unexpected termination: health check failed due to missing too many heartbeats' (gcs_health_check_manager.cc:88). SG revoke sgr-0125855ab4e02800e requested from lead. results/c3_2_poll.txt
2026-10-12T23:40:35Z C3.3.1-2 PASS scoped key pushed to W-hp /opt/rayvast/vast_key (mode 600, via head SSH, no log); watchdog.sh started 23:35:15Z (15 s TCP check to 35.85.154.224:6379, limit 180 s; DELETE with CONTAINER_API_KEY first, scoped key fallback). After 5 min: 21 checks, 0 fail, W-hp running. W-hp logs saved before the test (c2/whp/whp_logs.tgz)
2026-10-12T23:44:27Z C3.3.3-4 PASS T10 watchdog: head 'ray stop --force' ~23:41:00Z + docker stop rayvast-head, t0=23:41:10.555Z (0 listeners on 6379); vast list every 15 s: W-hp 55632730 running at 23:43:46Z, GONE at 23:44:02Z -> self-destroy 2 min 36 s - 2 min 52 s after t0 (plan expected 3-4 min; diff because the 180 s count starts at the last OK check, which is up to 15 s before t0, and ray stop came ~10 s before t0). DIFF: watchdog.log was on the destroyed instance, so WHICH key did the DELETE (CONTAINER_API_KEY or scoped fallback) is not known; vast key list has no usage data (num_auth_used None). Design: the watchdog must send its log off the box before the destroy. c3/c3_3_poll.txt
2026-10-12T23:45:47Z C3.1.d PASS extra rental 55640303 (offer 48743420, machine 3969 = W-hp's machine, image cached: running 22 s, ssh 42 s): from inside, DELETE own instance with CONTAINER_API_KEY -> HTTP 200 success, gone from list in <2 s. So the per-instance key alone is enough for the watchdog (no scoped key needed); cost ~$0.003
2026-10-12T23:46:37Z TD.1 PASS label sweep: 0 instances with label rayvast-test-20261012 (HTTP 200); TD.3 PASS scoped key 32488282 deleted ('Successfully Deleted API Key'); key list = 4 pre-existing keys only; local scoped key file removed
2026-10-12T23:46:37Z TD.4 PASS copied head results/c2 (+ worker logs, head Ray logs) to remote/c2 (4.7 MB); EXCLUDED head-hop.pcap (contains the token on lo) and the rogue test CA/client keys. Scan of results/2026-10-12: token 0 files, ca/head/node key bodies 0, full vast key 0, instance_api_key values 0 (3 redacted), CONTAINER_API_KEY/JUPYTER_TOKEN values 0

## C2/C3 details

Run by agent `rayvast-c2c3`. Raw files: `c2/`, `c3/`, `remote/c2/` (head results, worker logs, head Ray logs), `c2_offers*.json`, `c3_offers*.json`. Scripts: `tests/manual/c2_rent.py`, `c2_hostcheck.sh`, `c2_hostcheck_worker.py`, `c2_push.sh`, `worker_start.sh`, `battery.py`, `battery_worker_check.py`, `c2_negative.sh`, `c2_tls_probe.py`, `c2_hop.sh`, `c2_ports.py`, `c3_key_probe.py`, `watchdog.sh`.

### Host checks per candidate

All are x86_64, ray 2.59.0 and Python 3.11.16, with 64/64 identity ports. From the head, 63/63 ports connect. Egress IP equals `PUBLIC_IPADDR`. For root, CapEff = CapBnd = 0xa80405fb (no NET_ADMIN and no NET_RAW; for user ray, CapEff is 0). Each one has its own netns (172.17.0.x). /dev/shm is empty. Image pull = create→running (0.95 GB image).

| instance | offer | machine / host | GPU | location | dc | inet_down | $/h | running | sshd up | hairpin |
|---|---|---|---|---|---|---|---|---|---|---|
| 55632730 W-hp | 48743418 | 3969 / 33081 | P100 | Texas US | yes | 924 | 0.103 | 64 s | 22 s after boot (exact gap unknown, script crash) | PASS |
| 55632732 | 48190497 | 148075 / 62819 | 5060 Ti | Germany | yes | 881 | 0.178 | 65 s | +14 s after boot | PASS |
| 55632733 | 13273575 | 29217 / 1647 | A5000 | Romania | yes | 882 | 0.233 | 64 s | +31 s after boot | PASS |
| 55633384 | 49766585 | 28701 / 152538 | 5060 Ti | Texas US | yes | 7453 | 0.229 | 65 s | 68 s | PASS |
| 55633386 W-nohp | 46528521 | 32816 / 3497 | RTX 3060 | Czechia | yes | 877 | 0.272 | 77 s | 109 s | FAIL (ECONNREFUSED) |
| 55638058 W-loss | 44053751 | 142607 / 62819 | 5060 Ti | Germany | yes | 6620 | 0.178 | 54 s | 77 s | PASS |
| 55640303 key test | 48743420 | 3969 / 33081 | P100 | Texas US | yes | 924 | 0.10 | 22 s (image cached) | 42 s | not run |

- The hairpin rate is 5 of 6 distinct datacenter hosts (83%). The wider non-datacenter search was not necessary, so there are no non-datacenter data.
- Pull time does not depend on inet_down at this image size: 54–77 s at 877–7453 Mbps. The time is mostly vast setup, not transfer. The plan's limit of 300 s has a large margin.
- Two machine_ids (148075 and 142607, same host_id 62819) share the public IP 85.10.218.46. One public IP is not one host.
- Only 2 new hosts matched the query after the first 3 (the plan assumed 3 more).

### Per-step differences

- vast create: the API takes `env` as a dict `{"-p 70000:70000": "1", ..., "NAME": "value"}` (CLI style). The docs show a string, but the dict works and avoids quoting the pubkey. runtype `args` + `args: ["bash","-c",script]` overrides CMD. The image runs as ray with sudo, so the script uses `sudo apt-get install openssh-server` (12–31 s).
- **SECURITY:** the create response has `instance_api_key`. The first 3 were written to the logs and then redacted. vast also injects it into the container as `CONTAINER_API_KEY`. Any process with uid 1000 can read it (`/proc/1/environ`), so every Ray task and job can read it. Other vast env values: `JUPYTER_TOKEN`, `OPEN_BUTTON_TOKEN`, `PUBLIC_KEY`/`SSH_PUBLIC_KEY`. The instance metadata (GET /instances/) shows `extra_env`, `image_args` and `jupyter_token` in clear text. `request_logs` gives an unauthenticated S3 URL with the container stdout. So nothing secret may be in env, args or stdout (`set -x`!).
- The sshd port P(70040) is not in the instance metadata. `ports` is keyed by host port P, because of the identity mapping, and the ports are not contiguous. Found with a banner scan of the 64 ports. Provider: scan for `SSH-`, or read `$VAST_TCP_PORT_70040` another way.
- In the SSH session, PATH has no anaconda. Use `/home/ray/anaconda3/bin/{python,ray}`. Also, the image has no `ss` and no `curl` (I installed iproute2 for C2.4/C2.5).
- C2.1.1: the agent's SG calls were blocked by the permission classifier. The lead ran them.
- C2.2 SPREAD: 192/8 split (W-hp/W-nohp) in both runs. Both workers ran tasks, but SPREAD did not balance them (W-nohp RTT 171 ms). Unknown cause.
- C2.3.2: `openssl s_client` cannot show a client-cert failure on TLS 1.3. c2_tls_probe.py (h2 preface after the handshake) shows a server close for no cert or a rogue cert, and SETTINGS for a valid cert.
- C2.6 (T11): tasks do NOT fail. Extra tasks queue until a port frees (4 at +4 s, 4 at +64 s). Actors over the limit FAIL after 360 s with `ActorUnschedulableError ... Failed to startup worker after retrying 5 times ... Python environment setup` (misleading; worker log: `No available ports`). A driver fails in 2 s (`No available ports`). The raylet stays ALIVE. Idle task workers hold ports for about 60 s (actors #0–3 had to wait).
- C3.1 scoped key: the docs format is refused. Working format: `{"api":{"instance_read":{"api.instance":{"GET":{"constraints":{"id":{"eq":ID}}}}},"instance_write":{"api.instance":{"DELETE":{"constraints":{"id":{"eq":ID}}}}}}}` via `POST /api/v0/auth/apikeys/` (delete: `DELETE /auth/apikeys/<id>/`). vast adds a fixed `*` group to every key: users.current GET, instances GET (all), auth.apikeys GET (no values), ssh.keys ALL methods, team POST. So the scoped key can read the email and credit, list all instances, and add an account SSH key.
- C3.1 instance key (CONTAINER_API_KEY): own GET 200 and own DELETE 200. The other instance: GET/DELETE 401. users/current, apikeys and ssh: 401. GET /instances/ 200 lists ALL account instances. Offer search 200 (public, also with no auth). It is tighter than our scoped key.
- C3.3: watchdog.log was lost with the instance, so the key that did the destroy is unknown. A separate rental proved that CONTAINER_API_KEY can destroy its own instance.

### Battery numbers (C2.2 / C2.5)

| | W-hp (TX, 143.55.45.86) | W-nohp (CZ, 185.62.108.226) |
|---|---|---|
| actor RTT median/p95 ms | 65.7/66.1 ; 66.3/68.1 | 170.8/171.3 ; 170.8/171.1 |
| pip emoji (s) | 9.5 ; 9.8 (humanize 8.8) | 10.8 ; 10.3 (humanize 10.2) |
| 50 MB head→W MB/s | 27.8 ; 21.4 | 5.8 ; 6.3 |
| 50 MB W→head MB/s | 3.2 ; 4.4 | 12.4 ; 12.4 |
| W-hp→W-nohp / back MB/s | 13.1 / 13.5 ; 14.3 / 16.5 | |

Shim (W-nohp): the raylet's connects to its own public IP are rewritten to 172.17.0.3 for 19 ports (worker ports and P(70005)). Each connect also appears in the `::ffff:` form. 0 Errno 99.

### T11 behavior

- Tasks: they queue and then run. No error to the user, but there is log noise: about 13 worker processes die with `No available ports`.
- Actors: they fail after about 6 min with a misleading message.
- Driver: it fails at once, rc 1.
- Raylet: ALIVE.

### Watchdog timing

- Check every 15 s, limit 180 s, started 23:35:15Z. 5 min with the head up: 21 OK checks, 0 failed.
- Head down (ray stop ~23:41:00, container stop t0 = 23:41:10.6Z). Instance gone between 23:43:46Z and 23:44:02Z, which is 2:36–2:52 after t0. The count starts at the last OK check.

### T7

- DELETE at 23:34:00.8Z. GCS marks the node DEAD at 23:34:42.2Z (41.4 s), reason `health check failed due to missing too many heartbeats`.

### Costs

- vast: credit 12.0547 → 11.7064, so $0.35 for 8 rentals (7 in C2/C3 plus the key test).
- EC2 t3.large: from 22:05Z to termination (about 1.9 h), about $0.16.
- The $8 limit was never close.

### Recommended changes

Plan:
1. Use the dict `env` format, and find the sshd port by banner scan.
2. Use absolute anaconda paths over SSH.
3. Treat the create response as secret.
4. Use the h2-probe for the TLS 1.3 client-cert check.
5. Change the C3.1 expectations. `users/current` is 200 for any created key, and offer search is public.
6. Use the vast-docs constraint format above.
7. The T11 expectation: tasks queue, actors fail after about 6 min, drivers fail at once.
8. T7 is about 41 s, not 30 s.
9. Watchdog: expect `limit − 15 s … limit + 15 s` after the head goes down.
10. The datacenter hairpin rate is high (5/6), so finding W-nohp may need more rentals.

Design:
1. Use CONTAINER_API_KEY for the watchdog. It is tighter than any key we can create, and `ray-vast up` then needs no key management. But every Ray job on the worker can read it and destroy the worker, and can list all account instances. Document this risk. Use a separate vast account (or team) for clusters, so that a listing shows only the cluster's own instances.
2. Never put secrets in env, args or stdout. The logs URL is public.
3. Blocklist and SG logic must allow one public IP for several machine_ids (the same /32 rule; do not revoke while another worker uses that IP).
4. Set `RAY_num_workers_soft_limit` and warn on actors over the port limit. Document the misleading actor error.
5. The watchdog must send its log (or a final message) off the box before it destroys the instance.
6. The metrics port is still public without auth (200, about 380 ray_ series).
2026-10-12T23:47:45Z C3.2.3 PASS lead revoked sgr-0125855ab4e02800e (W-loss) -> True
2026-10-12T23:48:02Z TD.2/5/6/7 PASS (run by lead) revoked sgr-05d5525b33fd34316 (True), sgr-0125855ab4e02800e (True, earlier), sgr-0b74d36c743a87b62 (True, earlier); terminated i-0c343fdb37b555119 (waited); deleted sg-04966d2ef911b874d; verify: 0 rayvast-test EC2 instances, 0 groups rayvast-test-sg, vol-03061b5a39ddf335d NotFound; vast: 0 instances, scoped key deleted
2026-10-12T23:48:02Z TD.8 PASS total vast spend $0.35 (credit 12.0547 -> 11.7064, C2+C3, 8 rentals); EC2 t3.large 22:05Z to ~23:5xZ ~1.9 h ~$0.16. NOTHING LEFT RUNNING
