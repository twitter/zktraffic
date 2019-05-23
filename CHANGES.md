ChangeLog
=========

0.2.0 (2019-05-23)
--------------------

Features
********
- support python3


0.1.8 (2015-12-07)
--------------------

Bug Handling
************
- treat empty ('') paths as /

Features
********
- zk-stats-daemon now supports --exclude-bytes, which
  will skip accounting bytes per path and request type

0.1.7 (2015-12-03)
--------------------

Bug Handling
************
- add ansicolors to requirements
- --set-cpu-affinity wasn't effective on all threads,
  because set_cpu_affinity() was called after spawning
  daemon threads

Features
********
- zk-omni-dump: zk + fle + zab sniffer with automatic port detection. This
  is useful for JUnit testcases where ports are randomly assigned
- zk-stats-daemon now supports --sampling to capture only a % of packets,
  which is useful to reduce the amount of consumed CPU time

0.1.6 (2015-07-10)
--------------------

Bug Handling
************
- zab-dump: createSession's opcode was missing

Features
********
- zk-dump: add support for profiling latencies by path/type/client
- zab-dump: don't print learner pings unless --include-pings
- fle-dump/zab-dump: improved test coverage

0.1.5 (2015-06-22)
--------------------

Bug Handling
************
- per-path aggregation for watches wasn't working
- silence scapy logging
- add missing help strings and their defaults

Features
********
-

Breaking changes
****************
- zk-dump: rename --sort-by to --group-by

0.1.4 (2015-06-17)
--------------------

Bug Handling
************
- handle IOError for fix zk-dump, fle-dump, zab-dump

Features
********
-  add --count-requests and --sort-by
