# Spend, 2026-10-14 (UTC)

## C5 (join and task)

| Item | Value |
|---|---|
| vast credit before | $11.5658 (04:41:49Z) |
| vast credit after | $11.5584 (04:50:02Z; the same at 04:50:21Z) |
| vast spend (credit) | $0.0074 (budget $0.30) |
| vast spend (estimate from dph_total) | about $0.010: 55866627 P100 $0.107/h, 04:44:55Z to 04:49:5xZ (5 min); 55866629 A5000 $0.237/h, 04:44:55Z to 04:45:30Z (35 s). vast can book the last minutes later. |
| Rentals | 2 (1 slot, k = 2); 1 enrolled, 1 destroyed unused |
| Peak sum of dph_total | $0.344/h (both candidates, 35 s) |
| EC2 | t3.large head i-0240c26ac434bba6a, about 04:43Z to 04:50Z (7 min) at $0.0832/h = about $0.010, plus 60 GiB gp3 for 7 min (< $0.001). Budget about $0.15. |
| Day total so far (vast) | $0.0074 from this cycle. The day limit is $8. |

## C6 (self-destroy paths)

| Item | Value |
|---|---|
| vast credit before | $11.5584 (05:15:32Z) |
| vast credit after | $11.3915 (05:37:58Z; the same at 05:38:14Z) |
| vast spend (credit) | $0.1670 (budget $1.00) |
| vast spend (estimate from dph_total in the poll) | about $0.149 |
| Rentals | 7: 55870639 P100 $0.107/h (05:18:23Z-05:19:37Z, stopped by vast), 55870649 A5000 $0.237/h (to 05:26:11Z, A), 55870661 A5000 $0.239/h (to 05:33:42Z, B), 55870811 P100 $0.107/h (05:19:39Z-05:35:04Z, C), 55871644 P100 $0.107/h, 55872600 P100 $0.107/h, 55872780 A5000 $0.237/h (replacements, to down at 05:37Z) |
| Peak sum of dph_total | $0.583/h |
| EC2 | t3.large head i-067cffda6c3128957, about 05:16Z to 05:37Z (22 min) at $0.0832/h = about $0.03, plus 60 GiB gp3 (< $0.002). Budget about $0.15. |
| Day total so far (vast) | $0.0074 (C5) + $0.1670 (C6) = $0.1744. The day limit is $8. |

## C8 (full scale test; stopped at `up` by a product bug)

| Item | Value |
|---|---|
| vast credit before | $11.3764 (05:40:29Z) |
| vast credit after | $11.3661 (05:51:48Z) |
| vast spend (credit) | $0.0104 (budget $3.00). C8 rented nothing. This is the late booking of the C6 instances. |
| Rentals | 0 (`up` failed before the head started) |
| Peak sum of dph_total | $0/h |
| EC2 | t3.large head i-093eb1461f73453b8, about 05:42Z to 05:51Z (10 min) at $0.0832/h = about $0.014, plus 60 GiB gp3 (< $0.001). Budget about $0.15. |
| Day total so far (vast) | credit 11.5658 → 11.3661 = $0.1997. The day limit is $8. |

## C5 re-run (join and task, HEAD 4441ab9)

| Item | Value |
|---|---|
| vast credit before | $11.3661 (07:26:36Z) |
| vast credit after | $11.3576 (07:34:58Z). vast can book the last minutes later. |
| vast spend (credit) | $0.0085 (budget $0.30) |
| vast spend (estimate from dph_total) | about $0.013: 55886613 P100 $0.107/h (07:29:49Z-07:30:31Z), 55886615 A5000 $0.239/h (07:29:49Z-07:31:08Z, stopped by vast), 55886788 P100 $0.107/h (07:31:09Z-07:33:5xZ), 55886789 A5000 $0.239/h (07:31:09Z-07:31:50Z) |
| Rentals | 4 (1 slot, k = 2, two waves): 2 enrolled, 1 refused at /enroll, 1 ended unused |
| Peak sum of dph_total | $0.346/h |
| EC2 | t3.large head i-0ee21492e2ec35f8e, about 07:28Z to 07:34Z (6 min) at $0.0832/h = about $0.009, plus 60 GiB gp3 (< $0.001). Budget about $0.15. |
| Day total so far (vast) | credit 11.5658 → 11.3576 = $0.2082. The day limit is $8. |

## C6 re-run (self-destroy paths, HEAD 622c7c1)

| Item | Value |
|---|---|
| vast credit before | $11.3561 (07:36:54Z) |
| vast credit after | $11.1756 (08:04:23Z; $11.1809 at 08:03:45Z, late booking). |
| Auto top-ups seen | 0 (the credit went down only; it stayed above $5) |
| vast spend | credit before − credit after + $10 × top-ups = 11.3561 − 11.1756 + 0 = $0.1805 (budget $1.00) |
| vast spend (estimate from dph_total in the poll) | about $0.173 |
| Rentals | 7: 55888548 P100 $0.107/h (07:43:39Z-07:44:51Z, stopped by vast), 55888563 A5000 $0.236/h (to 07:50:49Z, A), 55888582 RTX 3090 $0.328/h (to 07:58:46Z, B), 55888774 P100 $0.107/h (07:44:56Z-08:00:18Z, C), 55889552, 55890431, 55890655 P100 $0.107/h each (replacements, to `down` at 08:03Z) |
| Peak sum of dph_total | $0.672/h |
| EC2 | t3.large head i-05b75dc8e51be411d, about 07:38Z to 08:03Z (25 min) at $0.0832/h = about $0.035, plus 60 GiB gp3 (< $0.002). Budget about $0.15. |
| Day total so far (vast) | credit 11.5658 → 11.1756 = $0.3902 (no top-up today). The day limit is $8. |

## C8 re-run (full scale test, HEAD 7dd15c2, 0.1.0rc1)

| Item | Value |
|---|---|
| vast credit before | $11.1756 (08:13:45Z) |
| vast credit after | $10.9682 (08:38:33Z; $10.9709 at 08:37:11Z, late booking). vast can book the last minutes later. |
| Auto top-ups seen | 0 (the credit went down only; it stayed above $5) |
| vast spend | credit before − credit after + $10 × top-ups = 11.1756 − 10.9682 + 0 = $0.2074 (budget $3.00) |
| vast spend (estimate from dph_total in the poll) | about $0.213 |
| Rentals | 8. C8.1: 6 (55894123 P100 $0.112/h, 55894124 A5000 $0.236/h unused, 55894125 RTX 3090 $0.327/h, 55894128 CMP 170HX $0.548/h unused, 55894139 RTX 4090 $0.870/h, 55894153 CMP 170HX $0.615/h), 08:24:55Z to 08:31:08Z. PID 1 probe: 2 (55894989 P100 $0.112/h, 08:32:32Z-08:35:22Z; 55894991 A5000 $0.236/h unused, 33 s) |
| Peak sum of dph_total | $2.708/h (6 candidates, 08:25:42Z); $1.923/h with the 4 workers |
| EC2 | t3.large head i-02e0828fe2dc560e2, about 08:16Z to 08:37Z (21 min) at $0.0832/h = about $0.03, plus 60 GiB gp3 (< $0.002). Budget about $0.15. |
| Day total so far (vast) | credit 11.5658 → 10.9682 = $0.5976 (no top-up today). The day limit is $8. |

## C9 (Ray libraries, HEAD 483cc52 + C9 script changes)

| Item | Value |
|---|---|
| vast credit before | $10.9682 (09:05:22Z) |
| vast credit after | $8.2814 (10:35:36Z; $8.2972 at 10:35:23Z, late booking). vast can book the last minutes later. |
| Auto top-ups seen | 0 (the credit went down only; it stayed above $5) |
| vast spend | credit before − credit after + $10 × top-ups = 10.9682 − 8.2814 + 0 = $2.6868 (budget about $5; allowed up to $15) |
| vast spend (estimate from dph_total in the poll) | about $1.70. The difference (about $1.0) is probably vast bandwidth and storage charges: torch 2.7.1 with its CUDA wheels (about 2.5 GB) was downloaded on 5 workers, plus the 2 GB object transfer, the Data shuffles and the 60 GB disks of 15 rentals. |
| Rentals | 15. 7 enrolled workers: 55900080 P100 $0.121/h, 55900093 RTX 3060 $0.287/h, 55901492 P100, 55902431 P100 (these 4 died from the pids limit), 55902441 RTX 3060 and 55902669 P100 (to `down`), 2 `gpu2` 2x CMP 170HX $1.089/h (55901477 09:23Z-10:22Z, added by the autoscaler for the CPU demand of C9.1.1 and ended by the idle timeout; 55910384 10:30Z-10:34Z for C9.3.4). 8 candidates ended unused. |
| Peak sum of dph_total | $2.211/h (limit 2.5) |
| EC2 | t3.large head i-065e7431df568f951, about 09:06Z to 10:35Z (89 min) at $0.0832/h = about $0.12, plus 60 GiB gp3 (< $0.01). S3 test bucket rvtest-c9-20261014-f3bf8b5c, 43 small objects for about 35 min (< $0.01; deleted). Budget about $0.30. |
| Day total so far (vast) | credit 11.5658 → 8.2814 = $3.2844 (no top-up today). The day limit is $8. |
