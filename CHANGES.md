ChangeLog
=========

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
- zk-omni-dump: zk + fle + zab sniffer with automatic port detection. This
  is useful for JUnit testcases where ports are randomly assigned

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
