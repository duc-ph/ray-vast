# Changelog

## 0.3.0

The first public release.

- Two workers on one public IP reach each other through hairpin NAT. Each worker tests hairpin NAT before it enrolls. An IP without hairpin NAT takes one worker only. The provider logs the IPs that it skips for this reason.
