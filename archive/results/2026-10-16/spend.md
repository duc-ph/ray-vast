# Spend, 2026-10-16 (UTC)

## C10 (process limits of the Ray libraries)

| Item | Value |
|---|---|
| vast credit before | $7.7294 (07:05:18Z) |
| vast credit after | $6.1676 (11:09:38Z) |
| Auto top-ups | none (the credit stayed above $5) |
| vast spend (credit) | $1.5618 (budget $5.00) |
| vast spend (estimate from dph_total and the poll) | about $1.19; vast also bills storage and the last minutes later |
| Rentals | 14 (see `c10/vast_timeline.jsonl`). Phase A first cluster: 6 (3 host-retry rounds, k = 2; m33638 RTX 5060 Ti $0.361/h three times, m45582 RTX 5070 Ti $0.70/h three times). Phase A second cluster: 2 (m3969 Tesla P100 $0.121/h, 07:28Z-09:21Z; m32816 ended unused). Phase B: 6 (m3969 P100 $0.121/h and m8966 RTX 3090 $0.334/h, 09:31Z-11:08Z; m55704 2x CMP 170HX $1.022/h for R2, 10:59Z-11:08Z; 3 ended unused). |
| Peak sum of dph_total | about $1.48/h (phase B with gpu2) |
| EC2 | 3 t3.large heads: rvtest-c10a #1 about 07:08Z-07:20Z, rvtest-c10a #2 about 07:21Z-09:21Z, rvtest-c10b about 09:23Z-11:09Z; about 4.0 h at $0.0832/h = about $0.33, plus 60 GiB gp3 (< $0.02) |
| Day total (vast) | $1.5618. The day limit is $8. |
