# C4
2026-10-13T00:02:20Z C4.credit PASS vast credit at C4 start = $11.7064; 0 vast instances; no rayvast-test-sg; only EC2 running is dev box i-0716c7f7865fa79d7 (not touched)
2026-10-13T00:03:35Z C4.setup1a PASS created sg-0becba115a18a2b18 rayvast-test-sg in vpc-0bf24a4ceed23db37 (tag rayvast-test=c4-20261013), inbound tcp22 from 172.59.160.24/32 only (sgr-00836e315ddae8c43)
2026-10-13T00:06:36Z C4.setup1b PASS i-083d3a81f624e9692 t3.large ami-03d70b8354a16a8bc (noble 20261004, describe-images), 40 GiB gp3 vol-02d868cadf67ea7b1, IMDSv2 required, launched 00:03:38Z, status ok 00:06:28Z; HEAD_IP=54.149.65.222 HEAD_PRIVATE_IP=172.31.26.62
2026-10-13T00:07:29Z C4.setup1c PASS docker.io server 29.1.3, tcpdump, jq, openssl installed (apt 34 s); /opt/rayvast-test/{shim,results/c4,secrets(700),tests,logs}
2026-10-13T00:08:52Z C4.setup1d DIFF image pull on EC2 46.8 s (C0: 36.4 s), digest sha256:77366add...45c3 (same), PyPI latest still 2.59.0. Image facts: NO /usr/bin/python3 (plan says 'system Python 3 of the image'; C2's python3 3.10 came from apt openssh-server Recommends) -> start script now apt-installs python3 with openssh-server (root-owned interpreter; anaconda is writable by ray). sudoers: 'ray ALL=NOPASSWD: ALL' is in /etc/sudoers (sudoers.d has only README), ray in group sudo, ray password locked (!), sudo 1.9.9, Defaults use_pty, Ubuntu 22.04.5
2026-10-13T00:09:39Z C4.setup1e PASS head: CA CN rayvast-test-ca-c4-20261013 (2 days), token, head cert SAN IP:54.149.65.222,IP:127.0.0.1 verify OK; container rayvast-head (shim v3 sha 79055dea...), ray start --head CPU 0: ray status 1 node, no CPU row, shim mapped in all Ray procs. c4/head/c4_head_setup.txt
2026-10-13T00:09:57Z C4.setup2a PASS offer query matched 13 offers (c4/c4_offers.json). Chosen (2 cheapest on 2 public IPs; approval given by lead in the task): W-kill = offer 48743420 (m3969 h33081, Tesla P100, $0.097/h, down 924 up 859, Texas US, static_ip False, 143.55.45.86); W-hb = offer 55448911 (m67941 h70142, RTX 5060 Ti, $0.161/h, down 1377 up 1831, Maryland US, static_ip True, 108.39.26.2)
2026-10-13T00:13:03Z C4.setup2b DIFF T8: 55643068 (offer 48743420, P100 TX): running 23 s after create (image cached on host 3969 from C2/C3), sshd up 54 s (boot 00:10:12Z, apt 26 s) -> OK. 55643070 (offer 55448911, 5060Ti MD, down 1377): pulled image (status loading at 37 s), running 64 s, but apt openssh-server+python3 took 79 s (boot 00:10:52Z -> sshd 00:12:11Z), so sshd was not up 60 s after running -> FAIL sshd limit (rent script) -> destroyed 55643070 per plan. Both: PID 1 = 'sudo -E /usr/bin/python3 /opt/rayvast/watchdog.py' (root), watchdog = child (root), no use_pty monitor process
2026-10-13T00:17:09Z C4.setup2c PASS W-hb replacement: 44053751 gone (PUT HTTP 400), re-search c4/c4_offers2.json; rented 55643474 (offer 44053847, m142609 h62819, 5060Ti DE, $0.178/h, down 5434, 85.10.218.46): loading at 11 s, running 64 s, sshd 71 s (port 36609). W-kill = 55643068 (143.55.45.86, sshd 42066)
2026-10-13T00:17:09Z C4.setup3 DIFF heartbeat loop (tests/heartbeat.sh on head) started EARLY, at 00:14:49Z (first touch rc 0 on both), before host checks/push, because W-kill's watchdog started 00:10:38Z and its 10 min grace would destroy it at 00:20:38Z before the setup is done. Loop period is ~40 s (30 s sleep + 2 ssh/scp rounds), not 30 s. Watchdog logs: heartbeat_age 14-30 s on both
2026-10-13T00:19:07Z C4.setup2d PASS host checks both: x86_64, ray 2.59.0 py 3.11.16 (anaconda), identity 64/64, head->63/63 connect, egress==PUBLIC_IPADDR, CapEff(ray)=0x0 (NET_ADMIN/NET_RAW 0), own netns (W-kill 172.17.0.2, W-hb 172.17.0.8), /dev/shm empty, hairpin PASS both; nproc W-kill 24, W-hb 112. c4/hostcheck_*.txt (on head)
2026-10-13T00:19:17Z C4.setup2e PASS SG rules added: sgr-0d1ef911746e438de (143.55.45.86/32, W-kill), sgr-0e67a3a9c911f0c84 (85.10.218.46/32, W-hb), tcp 0-65535, description rayvast-test
2026-10-13T00:22:43Z C4.setup2f PASS secret push (c2_push.sh: ca.crt node.crt node.key token worker.env mode 600, shim v3 79055dea...; certs SAN IP:<worker ip>,IP:127.0.0.1 verify OK) + ray start (worker_start.sh, LOCALHOST_PORTS=P(70004),P(70005), 57 worker ports) on both; ray list nodes: 3 ALIVE (head 54.149.65.222 no CPU; W-kill e614b5fe 143.55.45.86 CPU 8 GPU 1 P100; W-hb 341a36ab 85.10.218.46 CPU 8 GPU 1). c4/c4_push_join.txt
2026-10-13T00:22:43Z C4.setup2g DIFF root SSH login failed (publickey): vast REPLACES /root/.ssh/authorized_keys after the start script (owner uid 129 gid 1001, mode 644, 2 vast account ed25519 keys, our key 0 hits), and StrictModes rejects it anyway. FIX: as ray (sudo still on) wrote the head key to /root/.ssh/authorized_keys2 (root:root 600) -> root login OK on both
2026-10-13T00:22:43Z C4.setup2h PASS sudo removal (as root over SSH): 'sed -i "/^ray ALL=NOPASSWD: ALL$/d" /etc/sudoers' (line 55, backup /root/sudoers.bak), visudo -c parsed OK, 0 NOPASSWD lines in sudoers and sudoers.d. As ray: 'sudo -n true' -> 'sudo: a password is required' rc 1 (both). ray stays in group sudo (%sudo rule needs a password; ray's password is locked '!'). Extra: as ray, /proc/1/environ not readable (PID 1 = root sudo), 0 processes with CONTAINER_API_KEY readable by ray, ray's env has 0 CONTAINER_API_KEY. c4/c4_sudo_removal.txt
2026-10-13T00:23:02Z C4.1.0 PASS W-kill process table: PID 1 ppid 0 root 'sudo -E /usr/bin/python3 /opt/rayvast/watchdog.py'; PID 1509 ppid 1 root '/usr/bin/python3 /opt/rayvast/watchdog.py' (no use_pty monitor between them)
2026-10-13T00:23:05Z C4.1.1 DIFF as ray: 'kill -9 1509' -> 'Operation not permitted' rc 1 (PASS); 'kill -9 1' -> rc 0, NO error (expected EPERM). Cause: sudo is setuid, so PID 1 has real uid 1000 (ray) (/proc/1/status Uid: 1000 0 0 0) and the kernel lets ray signal it. SIGKILL to the PID-namespace init from inside is dropped by the kernel, so PID 1 survived. Watchdog still running
2026-10-13T00:23:06Z C4.1.2 PASS as ray: 'sudo -n kill -9 1509' -> 'sudo: a password is required' rc 1
2026-10-13T00:23:08Z C4.1.3 DIFF Ray task pinned to W-kill (uid 1000, c4_kill_task.py): os.kill(1509,9) -> PermissionError(1) (PASS); os.kill(1,9) -> NO error (signal sent, dropped by kernel; expected PermissionError); sudo -n kill -> password required; /proc/1/environ PermissionError(13); watchdog alive after
2026-10-13T00:24:30Z C4.1.extra DIFF PID 1 (sudo) catches SigCgt 0xb7a07 = HUP INT QUIT USR1 USR2 PIPE ALRM TERM CHLD CONT TSTP. Signals that sudo catches are delivered to the ns init and sudo relays them to the watchdog. 'kill -TSTP 1' as ray: rc 0, no visible effect (watchdog state S, logging continued). Not yet tested: SIGTERM/SIGINT from ray (would be relayed -> watchdog stops -> container stops; SIGINT -> KeyboardInterrupt -> watchdog destroys). ZOMBIES: 47 <defunct> sshd with ppid 1 after 13 min (sudo as PID 1 does not reap orphans; each SSH session leaves one) -> PID leak
2026-10-13T00:25:30Z C4.1.4 PASS as root (SSH root, authorized_keys2): 'kill -9 1509' rc 0 at 00:25:30.85Z -> sudo (PID 1) exited -> container stopped: sshd refused at 00:25:35Z; vast actual_status 'exited' at the 00:25:47Z poll (previous poll 00:25:36Z running), cur_state/intended_status still 'running', dph_total unchanged 0.1026 (dph_base 0.0933 GPU + storage 0.0093); head: W-kill DEAD 'health check failed due to missing too many heartbeats'; heartbeat touch_rc 255
2026-10-13T00:27:33Z C4.1.4 DIFF after the root kill vast RESTARTS the container (intended_status stays 'running'): actual_status flips exited/running every ~10-20 s (00:25:47 exited, 00:25:58 running, 00:26:08 exited, ...; 7 restarts in 2 min). Container log (request_logs, public S3 URL): each restart reruns the start script, every 'sudo' now fails ('sudo: a password is required', 101 lines) because the sudoers edit persists in the container fs, so sshd and the watchdog never start and the script exits. Billing: dph_total stayed 0.1026 $/h (GPU 0.0933 + storage 0.0093) the whole time, so the GPU cost does NOT stop. c4/c4_1_4_poll_wkill.jsonl, c4/c4_1_wkill_container_log.txt
2026-10-13T00:27:43Z C4.1.5 PASS destroyed W-kill 55643068 with the full key from the laptop (DELETE 200 at 00:27:37Z, gone at 00:27:43Z poll); revoked sgr-0d1ef911746e438de (143.55.45.86/32) -> True
2026-10-13T00:28:01Z C4.2.1 PASS W-hb heartbeat 00:14:53Z-00:28:01Z (13 min, > 3 min): 46+ checks, max heartbeat_age 40 s (< 45 s), 0 DESTROY lines
2026-10-13T00:30:19Z C4.2.2 PASS short pause: heartbeat loop killed after the 00:28:01Z touch, restarted (W-hb only) 00:30:17Z, first touch 00:30:19Z (gap 2:18, a bit over 2 min because of the timer). Watchdog ages rose 14->134 s in 15 s steps (largest logged 134 s at 00:30:15Z; expected ~150 s for a 2 min pause + 30 s period); no destroy, instance running
2026-10-13T00:30:46Z C4.2.3 PASS provider stop: heartbeat loop killed 00:30:46Z; t0 = last successful touch 00:30:19Z (watchdog age 41 s at 00:31:00Z agrees). Head GCS up (ray list nodes: head ALIVE, W-hb ALIVE). Copy-only loop (NO_TOUCH=1) started to keep watchdog.log copies; pre-destroy request_logs fetched (c4/c4_2_whb_container_log_before.txt, 67 check lines)
2026-10-13T00:35:42Z C4.2.4 PASS W-hb self-destroyed: last watchdog line copied off-box 00:35:15Z 'heartbeat_age=296s'; vast poll running at 00:35:27Z, gone at 00:35:42Z -> destroy between t0+5:08 and t0+5:23 (t0 00:30:19Z); by the 15 s check grid the DESTROY check was 00:35:30Z (age ~311 s) = t0+5:11. Head GCS stayed up; W-hb DEAD 'health check failed due to missing too many heartbeats'. The DESTROY/DELETE lines themselves were NOT captured (next copy at 00:35:53Z failed rc 255)
2026-10-13T00:36:15Z C4.2.5 DIFF request_logs after the destroy: API still answers success + result_url, but the file is 58 B 'Error response from daemon: No such container: C.55643474'. The pre-destroy URL is the same path per instance and the new request overwrote it (same 58 B), so whether the old upload would have survived is unknown. The watchdog's last lines (DESTROY reason, DELETE HTTP code) are lost. Last good copy: request_logs at 00:31:08Z (c4/c4_2_whb_container_log_before.txt) + copy loop to 00:35:15Z
2026-10-13T00:36:20Z C4.2.6 PASS revoked sgr-0e67a3a9c911f0c84 (85.10.218.46/32) -> True; heartbeat/copy loops stopped
2026-10-13T00:36:30Z C4.extra SKIPPED SIGTERM/SIGINT-from-ray test on a 3rd rental: asked the lead (extra rental not in the authorization), no answer before teardown -> not run
2026-10-13T00:37:19Z TD.1 PASS vast label sweep: 0 instances in the account, 0 with label rayvast-test-c4-*
2026-10-13T00:37:19Z TD.2 PASS worker SG rules already revoked (sgr-0d1ef911746e438de, sgr-0e67a3a9c911f0c84); only operator ssh rule left before delete
2026-10-13T00:37:19Z TD.3 PASS no scoped vast key was created in C4 (watchdog uses CONTAINER_API_KEY)
2026-10-13T00:36:50Z TD.4 PASS copied head results (c4/head/: heartbeat logs, watchdog logs, hostchecks, head shim log) to the laptop; secrets/ not copied; scan of results/2026-10-13 + tests/manual: 0 hits for the cluster token, 'PRIVATE KEY', the vast key prefix, instance_api_key values
2026-10-13T00:38:00Z TD.5 PASS terminated i-083d3a81f624e9692 (shutting-down 00:37:29Z, terminated 00:38:00Z); vol-02d868cadf67ea7b1 NotFound
2026-10-13T00:38:05Z TD.6 PASS deleted sg-0becba115a18a2b18 (rayvast-test-sg)
2026-10-13T00:38:30Z TD.7 PASS verify: 0 EC2 instances with tag rayvast-test (non-terminated), 0 groups rayvast-test-sg, 0 vast instances. Dev box 50.112.112.27 not touched
2026-10-13T00:38:30Z TD.8 PASS vast spend C4 = $0.106 (credit 11.7064 -> 11.6000; 3 rentals: W-kill 17 min, candidate 3 min, W-hb 22 min); EC2 t3.large 00:03:38Z-00:37:29Z = 34 min ~ $0.05. NOTHING LEFT RUNNING

## C4 details

Run by agent `rayvast-c4`. Raw files: `c4/` (rent, poll, kill, sudo, container logs), `c4/head/` (heartbeat and watchdog logs copied from the head). Scripts: `tests/manual/watchdog.py`, `heartbeat.sh`, `c4_rent.py` (copy of c2_rent.py with the C4 start script), `c4_poll.py`, `c4_kill_task.py`. Reused: `c1_head.sh`, `c2_hostcheck.sh`, `c2_hostcheck_worker.py`, `c2_push.sh`, `worker_start.sh`, shim v3.

### Results

| # | Result | Key numbers |
|---|---|---|
| T13 | partly fails | ray: kill of the watchdog EPERM, `sudo -n` "a password is required". But `kill -9 1` and `os.kill(1, 9)` as ray give NO error (expected EPERM). After the root kill the container stops, but vast restarts it in a loop and the GPU price stays on. |
| T14 | pass | max age normal 40 s (W-hb) / 39 s (W-kill); pause of 2:18 -> max 134 s, no destroy; stop -> destroyed at t0+5:11 (poll: between t0+5:08 and t0+5:23). Head GCS up. |
| T8 | pass | create->running: 23 s (P100 TX, image cached on host 3969), 64 s (5060Ti MD, down 1377, pulled), 64 s (5060Ti DE, down 5434, pulled). sshd: 54 s, FAIL (~120 s, apt 79 s), 71 s. EC2 pull 46.8 s. |

### Per step and differences

- Setup: head as in C0/C1 (c1_head.sh). 401/403/404 on 127.0.0.1:6384; 6384/8265 on 127.0.0.1. PyPI latest is still 2.59.0; same image digest.
- The image has **no /usr/bin/python3**. The "system Python 3" of the plan does not exist. In C2, python3 3.10 came from the openssh-server Recommends. The start script now installs `python3` with apt. This gives a root-owned interpreter. Do not run the root watchdog with anaconda: user ray can write to anaconda.
- One candidate (55643070, MD) failed the sshd limit: apt took 79 s. It was destroyed, and offer 44053751 was gone (HTTP 400), so I searched again and rented 44053847.
- The heartbeat started before the host checks (00:14:49Z). Reason: the watchdog starts at boot, and its 10 min grace runs during the setup. W-kill would have destroyed itself at 00:20:38Z. The loop period is ~40 s, not 30 s (30 s sleep + ssh/scp).
- **vast replaces /root/.ssh/authorized_keys** after the start script (uid 129:1001, mode 644, 2 vast account keys). Root login with our key fails. Fix: write the head key to `/root/.ssh/authorized_keys2` (root 600).
- Sudo removal: as root, `sed -i '/^ray ALL=NOPASSWD: ALL$/d' /etc/sudoers` (line 55; sudoers.d has only README), then `visudo -c` OK. As ray: `sudo -n true` -> "sudo: a password is required". Ray stays in group `sudo`, but the `%sudo` rule needs a password, and ray's password is locked (`!`).
- As ray, `/proc/1/environ` cannot be read, and no process with CONTAINER_API_KEY is readable. A root PID 1 thus also hides the vast key from Ray tasks (in C2, PID 1 was ray and the key was readable).

### PID 1 / watchdog process table (W-kill, 00:23:02Z)

```
PID PPID USER STAT COMMAND
  1    0 root Ss   sudo -E /usr/bin/python3 /opt/rayvast/watchdog.py
1509   1 root S    /usr/bin/python3 /opt/rayvast/watchdog.py
```
/proc/1/status `Uid: 1000 0 0 0`: sudo is setuid, so the real uid of PID 1 is ray. SigCgt 0xb7a07 (HUP INT QUIT USR1 USR2 PIPE ALRM TERM CHLD CONT TSTP). The watchdog catches only SIGINT (Python KeyboardInterrupt).

### Kill results

| who | action | result |
|---|---|---|
| ray (SSH) | kill -9 1509 | Operation not permitted |
| ray (SSH) | kill -9 1 | rc 0, no error. The kernel drops SIGKILL to the namespace init. |
| ray (SSH) | sudo -n kill -9 1509 | a password is required |
| Ray task | os.kill(1509, 9) | PermissionError(1) |
| Ray task | os.kill(1, 9) | no error (signal sent, dropped) |
| ray (SSH) | kill -TSTP 1 | rc 0, no visible effect |
| root (SSH) | kill -9 1509 | sudo exits, container stops in < 5 s |

Not tested (no approval for a 3rd rental): `kill -TERM 1` / `kill -INT 1` as ray. sudo catches and relays these signals. So a Ray task can probably stop the watchdog (TERM: container stops) or trigger its destroy (INT: KeyboardInterrupt -> destroy).

### Container status and billing after the root kill

- 00:25:30.85Z kill. 00:25:35Z sshd refused. 00:25:47Z poll: `actual_status=exited`, with `intended_status`/`cur_state` still `running`.
- vast **restarts** the container: exited/running every ~10–20 s, 7+ restarts in 2 min. Each restart reruns the start script. Every `sudo` fails (the sudoers edit persists in the container fs), so sshd and the watchdog never start.
- `dph_total` stayed at 0.1026 $/h (GPU 0.0933 + storage 0.0093) until the destroy at 00:27:37Z. **The GPU cost does not stop.**

### Heartbeat ages (watchdog log)

| phase | checks | min–max age |
|---|---|---|
| W-kill normal (00:14:50–00:25:30) | 41 | 0–39 s |
| W-hb normal (00:14:50–00:28:01) | 53 | 0–40 s |
| W-hb pause (last touch 00:28:01, next 00:30:19) | 9 | 14–134 s |
| W-hb resumed | 2 | 11–26 s |
| W-hb stop (t0 00:30:19) | 18 | 41–296 s, then destroy |

Destroy delay: t0+5:11 (check grid). The poll saw running at 00:35:27Z and gone at 00:35:42Z.

### Container log after the destroy

- request_logs still returns success and a URL. The file has 58 B: `Error response from daemon: No such container: C.55643474`.
- The URL path is the same for each instance, so the new request overwrote the old upload. Whether the old upload would have survived is unknown.
- The DESTROY and DELETE lines are lost everywhere. The last off-box copy (copy loop) ends at 00:35:15Z, age 296 s.

### Other findings

- **Zombies:** sudo as PID 1 does not reap orphans. There were 47 `<defunct>` sshd processes after 13 min (about 1 for each SSH session, and the heartbeat makes ~90/h). Over days, this can fill the pids limit.
- Two machine_ids again share 85.10.218.46 (host 62819).

### Costs

- vast $0.106 (credit 11.7064 -> 11.6000, 3 rentals).
- EC2 34 min ~ $0.05.
- Nothing left running.

### Recommended changes

Design:
1. Do not use `exec sudo` as PID 1. Its real uid stays ray, so ray can signal it, and it relays TERM/INT. Use a small root init that reaps orphans and ignores or blocks signals from uid 1000. Options: start the container as root (vast `--user`?), or `sudo setpriv --reuid=0 --regid=0 --init-groups` + `tini -s`, or a C wrapper that sets real uid 0 and reaps. Also install a SIGINT/SIGTERM handler in the watchdog that logs and ignores the signal.
2. PID 1 must reap zombies (tini, or the watchdog calls waitpid(-1, WNOHANG)).
3. A stopped container is not a stopped bill: vast restarts it with `intended_status=running` and charges the GPU price. The provider must destroy (not only notice) any instance whose actual_status is `exited`. The start script must also handle a restart: after the sudo removal it fails. Do the sudo removal from the root PID 1 path, or make the restart path destroy the instance.
4. The watchdog log is lost with the instance. Push the final DESTROY line off-box before the DELETE (for example, the head pulls it, or the watchdog POSTs it to the head). request_logs does not survive the destroy.
5. Root SSH needs authorized_keys2 (vast overwrites authorized_keys). Better: the provider needs no root SSH.
6. Put the watchdog files in a root-owned dir. /opt/rayvast is ray-owned: ray can symlink the log (O_NOFOLLOW added) and can touch the heartbeat itself to keep the instance alive. Heartbeat over root SSH to a root-only path closes that.
7. Ship python3 (root-owned) in the image, or install it in the start script. The image has none, and anaconda is ray-writable.

Plan:
1. Start the heartbeat (or extend the grace) before the setup. Setup takes ~10 min with a slow apt.
2. sshd limit: apt can take ~80 s on some hosts. Use 120 s, or bake openssh-server and python3 into the image.
3. Expected for T13: `kill -9 1` as ray gives no error (not EPERM), with the current sudo-PID-1 design. Add tests for `kill -TERM 1` and `kill -INT 1` as ray.
4. C4.1.4 expected: the container `exited` + a vast restart loop, and dph_total unchanged.
5. The heartbeat loop period is 30 s + the SSH time. The max age is ~40 s.
6. C4.2.5: request_logs after the destroy returns "No such container". Fetch the logs before.

### Lead messages received after the teardown (00:38Z)

- New sshd limit 3 min: not applied, because all rentals were done. With 3 min, candidate 55643070 (sshd ~56-60 s after running) would have passed.
- Start script order + /opt/rayvast/boot_times.txt for the W-hb replacement: not done. The replacement (55643474) used the first C4 script. Its only times: boot 00:14:13Z, apt done + sshd 00:14:29Z (16 s on that host). The failed candidate: boot 00:10:52Z, apt (update + openssh-server + python3, with Recommends) done 00:12:11Z (79 s). No split between update and each install.
- 10 min poll of the exited W-kill and the credit delta: not done. W-kill was destroyed at 00:27:37Z, 2 min after the root kill. The data that exists: restart loop for 2 min, dph_total unchanged. No credit split (GPU vs storage) was measured.
- TERM/INT test on W-hb: skipped, because C4.2 destroyed W-hb (as the lead said). W-sig test: skipped (lead: no).
