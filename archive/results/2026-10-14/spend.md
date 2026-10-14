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
