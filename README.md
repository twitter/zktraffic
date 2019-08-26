# ZKTraffic [![Build Status](https://travis-ci.org/twitter/zktraffic.svg?branch=master)](https://travis-ci.org/twitter/zktraffic) [![Coverage Status](https://coveralls.io/repos/twitter/zktraffic/badge.png)](https://coveralls.io/r/twitter/zktraffic) [![PyPI version](https://badge.fury.io/py/zktraffic.svg)](http://badge.fury.io/py/zktraffic)

**Table of Contents**

- [tl;dr](#tldr)
- [Installing](#installing)
- [What is ZKTraffic?](#what-is-zktraffic)
- [Contributing and Testing](#contributing-and-testing)
- [More tools!](#more-tools)
- [OS X](#os-x)
- [Dependencies](#dependencies)

### tl;dr ###

ZooKeeper protocol analyzer and stats gathering daemon

### Installing ###

You can install ZKTraffic via pip:

.. code-block:: bash

   $ pip install zktraffic

Or run it from source (if you have the dependencies installed, see below):

.. code-block:: bash

   $ git clone https://github.com/twitter/zktraffic.git
   $ cd zktraffic
   $ sudo ZKTRAFFIC_SOURCE=1 bin/zk-dump --iface=eth0

To get a quick count of requests by path:

.. code-block:: bash

   $ sudo ZKTRAFFIC_SOURCE=1 bin/zk-dump --iface=eth0 --count-requests 10000 --sort-by path
   / 1749
   /services/prod/search 846
   /configs/teleportation/features 843

Or by type:

.. code-block:: bash

   $ sudo ZKTRAFFIC_SOURCE=1 bin/zk-dump --iface=eth0 --count-requests 10000 --sort-by type
   GetChildrenRequest 9044
   ExistsRequest 958

You can also measure latencies by path (avg, p95 and p99):

.. code-block:: bash

   $ sudo ZKTRAFFIC_SOURCE=1 bin/zk-dump --measure-latency 1000 --group-by path --aggregation-depth 2 --sort-by p99
   path                     avg         p95         p99
   ---------------  -----------  ----------  ----------
   /party/services  0.000199077  0.00048846  0.00267805
   /party           0.000349498  0.00136839  0.00201204
   /party/configs   0.000157728  0.00036664  0.00122663

Or by type:

.. code-block:: bash

   $ sudo ZKTRAFFIC_SOURCE=1 bin/zk-dump --measure-latency 1000 --group-by type --sort-by p99
   type                            avg          p95          p99
   ----------------------  -----------  -----------  -----------
   CreateEphemeralRequest  0.000735009  0.000978041  0.0032404
   GetChildrenRequest      0.000182547  0.000453258  0.00220628
   ExistsRequest           0.000162728  0.000430155  0.000862937

Or by client:

.. code-block:: bash

   $ sudo ZKTRAFFIC_SOURCE=1 bin/zk-dump --measure-latency 1000 --group-by client --sort-by p99
   client                          avg          p95          p99
   ----------------------  -----------  -----------  -----------
   10.0.1.3:44308          0.000735009  0.000978041  0.0032404
   10.0.1.6:34305          0.000182547  0.000453258  0.00220628
   10.0.1.9:36110          0.000162728  0.000430155  0.000862937

Or use the stats gathering daemon:

.. code-block:: bash

   $ sudo ZKTRAFFIC_SOURCE=1 bin/zk-stats-daemon --iface=eth0 --http-port=9090

Or you can build PEX files — from the source — for any of the available tools:

.. code-block:: bash

   $ pip install pex

   # zk-dump
   $ pex -v -e zktraffic.cli.zk -o zk-dump.pex .

   # zk-stats-daemon
   $ pex -v -e zktraffic.cli.stats_daemon -o stats-daemon.pex .

   # zab-dump
   $ pex -v -e zktraffic.cli.zab -o zab-dump.pex .

   # fle-dump
   $ pex -v -e zktraffic.cli.fle -o fle-dump.pex .

More info about PEX [here](https://pex.readthedocs.org "PEX").

### What is ZKTraffic? ###

An {iptraf,top}-esque traffic monitor for ZooKeeper. Right now it exports
per-path (and global) stats. Eventually it'll be made to export per-user
stats too.

It has a front-end, zk-dump, that can be used in interactive mode to dump traffic:

```
# need root or CAP_NET_ADMIN & CAP_NET_RAW
$ sudo zk-dump --iface eth0
21:08:05:991542 ConnectRequest(ver=0, zxid=0, timeout=10000, session=0x0, readonly=False, client=127.0.0.1:50049)
————————►21:08:06:013513 ConnectReply(ver=0, timeout=10000, session=0x148cf0aedc60000, readonly=False, client=127.0.0.1:50049)
21:08:07:432361 ExistsRequest(xid=1, path=/, watch=False, size=14, client=127.0.0.1:50049)
————————►21:08:07:447353 ExistsReply(xid=1, zxid=31, error=0, client=127.0.0.1:50049)
21:08:07:448033 GetChildrenRequest(xid=2, path=/, watch=False, size=14, client=127.0.0.1:50049)
————————►21:08:07:456169 GetChildrenReply(xid=2, zxid=31, error=0, count=1, client=127.0.0.1:50049)
...
```

Or, it can work in daemon mode from which it exposes HTTP/JSON endpoints with
stats that can be fed into your favourite data collection system:

.. code-block:: bash

   $ sudo zk-stats-daemon.pex --app_daemonize --aggregation-depth=5

   # Wait for 1 min and:

   $ sleep 60 && curl http://localhost:7070/json/paths | python -mjson.tool
   {
    "ConnectRequest": 2,
    "ConnectRequestBytes": 90,
    "CreateRequest/configs": 2,
    "CreateRequest/configs/server": 2,
    "CreateRequest/discovery": 2,
    "CreateRequest/discovery/hosts": 2,
    "CreateRequest/discovery/services": 2,
    "CreateRequestBytes/configs": 110,
    "CreateRequestBytes/configs/server": 124,
    "CreateRequestBytes/discovery": 114,
    "CreateRequestBytes/discovery/hosts": 126,
    "CreateRequestBytes/discovery/services": 132,
    "ExistsRequest/": 1574,
    "ExistsRequest/configs": 3,
    "ExistsRequest/configs/server": 2,
    "ExistsRequest/discovery": 4,
    "ExistsRequest/discovery/hosts": 2,
    "ExistsRequest/discovery/services": 2,
    "ExistsRequestBytes/": 22036,
    "ExistsRequestBytes/configs": 63,
    "ExistsRequestBytes/configs/server": 56,
    "ExistsRequestBytes/discovery": 92,
    "ExistsRequestBytes/discovery/hosts": 58,
    "ExistsRequestBytes/discovery/services": 64,
    "GetChildrenRequest/configs": 1285,
    "GetChildrenRequest/configs/server": 1242,
    "GetChildrenRequest/discovery": 1223,
    "GetChildrenRequest/discovery/hosts": 1250,
    "GetChildrenRequest/discovery/services": 1222,
    "GetChildrenRequest/zookeeper/config": 1285,
    "GetChildrenRequest/zookeeper/quota/limits": 1228,
    "GetChildrenRequest/zookeeper/quota/limits/by-path": 1269,
    "GetChildrenRequest/zookeeper/quota/limits/global": 1230,
    "GetChildrenRequest/zookeeper/quota/stats/by-path": 1222,
    "GetChildrenRequestBytes/discovery/hosts": 36250,
    "GetChildrenRequestBytes/discovery/services": 39104,
    "GetChildrenRequestBytes/zookeeper/config": 38550,
    "GetChildrenRequestBytes/zookeeper/quota/limits": 44208,
    "GetChildrenRequestBytes/zookeeper/quota/limits/by-path": 55836,
    "GetChildrenRequestBytes/zookeeper/quota/limits/global": 52890,
    "GetChildrenRequestBytes/zookeeper/quota/limits/slices": 51815,
    "GetChildrenRequestBytes/zookeeper/quota/stats": 42630,
    "GetChildrenRequestBytes/zookeeper/quota/stats/by-path": 52546,
    "GetChildrenRequestBytes/zookeeper/quota/stats/global": 50568,
    "reads/": 2761,
    "reads/configs": 1288,
    "reads/configs/server": 1244,
    "reads/discovery": 1227,
    "reads/discovery/hosts": 1252,
    "reads/discovery/services": 1224,
    "reads/zookeeper/config": 1285,
    "reads/zookeeper/quota/limits": 1228,
    "reads/zookeeper/quota/limits/by-path": 1269,
    "reads/zookeeper/quota/limits/global": 1230,
    "readsBytes/": 38654,
    "readsBytes/discovery/services": 39168,
    "readsBytes/zookeeper/config": 38550,
    "readsBytes/zookeeper/quota/limits": 44208,
    "readsBytes/zookeeper/quota/limits/by-path": 55836,
    "readsBytes/zookeeper/quota/limits/global": 52890,
    "readsBytes/zookeeper/quota/limits/slices": 51815,
    "readsBytes/zookeeper/quota/stats": 42630,
    "readsBytes/zookeeper/quota/stats/by-path": 52546,
    "readsBytes/zookeeper/quota/stats/global": 50568,
    "total/readBytes": 655586,
    "total/reads": 21251,
    "total/writeBytes": 606,
    "total/writes": 10,
    "writes/": 0,
    "writes/configs": 2,
    "writes/configs/server": 2,
    "writes/discovery": 2,
    "writes/discovery/hosts": 2,
    "writes/discovery/services": 2,
    "writesBytes/": 0,
    "writesBytes/configs": 110,
    "writesBytes/configs/server": 124,
    "writesBytes/discovery": 114,
    "writesBytes/discovery/hosts": 126,
    "writesBytes/discovery/services": 132
   }

Other relevant endpoints for stats are:

* /json/ips: top-N per-ip stats
* /json/auths: per-auth stats
* /json/auths-dump: a full dump of known auths
* /json/info: process uptime and introspection info
* /threads: stacks for all threads

### Contributing and Testing ###

Please see [CONTRIBUTING.md](CONTRIBUTING.md).

### More tools! ###

Along with zk-dump and zk-stats-daemon, you can find fle-dump which allows you
to inspect FastLeaderElection traffic (i.e.: the protocol by which ZooKeeper decides
who will lead and the mechanism by which the leader is subsequently discovered):

.. code-block:: bash

   $ sudo fle-dump --iface eth0 -c
   Notification(
     timestamp=00:57:12:593254,
     src=10.0.0.1:32938,
     dst=10.0.0.2:3888,
     state=following,
     leader=3,
     zxid=0,
     election_epoch=0,
     peer_epoch=0,
     config=
          server.0=10.0.0.1:2889:3888:participant;0.0.0.0:2181
          server.1=10.0.0.2:2889:3888:participant;0.0.0.0:2181
          server.2=10.0.0.3:2889:3888:participant;0.0.0.0:2181
          server.3=10.0.0.4:2889:3888:participant;0.0.0.0:2181
          server.4=10.0.0.5:2889:3888:participant;0.0.0.0:2181
          version=10010d4d6
   )
   Notification(
     timestamp=00:57:12:595525,
     src=10.0.0.2:3888,
     dst=10.0.0.1:32938,
     state=looking,
     leader=1,
     zxid=4296326153,
     election_epoch=1,
     peer_epoch=1,
     config=
          server.0=10.0.0.1:2889:3888:participant;0.0.0.0:2181
          server.1=10.0.0.2:2889:3888:participant;0.0.0.0:2181
          server.2=10.0.0.3:2889:3888:participant;0.0.0.0:2181
          server.3=10.0.0.4:2889:3888:participant;0.0.0.0:2181
          server.4=10.0.0.5:2889:3888:participant;0.0.0.0:2181
          version=10010d4d6
   )
   ...

Note: for initial messages to be visible you'll need the patch available
at [ZOOKEEPER-2098](https://issues.apache.org/jira/browse/ZOOKEEPER-2098 "ZOOKEEPER-2098"),
if you are using ZooKeeper prior to ZooKeeper 3.5.1-rc2.

Note: if you are using Linux 3.14 or later, you'll need to disable [TCP Auto Corking](http://git.kernel.org/cgit/linux/kernel/git/torvalds/linux.git/commit/?id=f54b311142a92ea2e42598e347b84e1655caf8e3) by running `echo 0 > /proc/sys/net/ipv4/tcp_autocorking`.

If you are interested in debugging ZAB (ZooKeeper Atomic Broadcast protocol), you can use
zab-dump:

.. code-block:: bash

   $ sudo zab-dump --iface eth0

   Request(
    cxid=6,
    dst=10.0.0.1:2889,
    length=112,
    req_type=CreateRequest,
    session_id=0x34e4d23b0d70001,
    src=10.0.0.2:48604,
    timestr=22:54:31:995353,
    zxid=-1,
   )
   Proposal(
    cxid=6,
    dst=10.0.0.2:48603,
    length=110,
    session_id=0x34e4d23b0d70001,
    src=10.0.0.1:2889,
    timestr=22:54:31:995753,
    txn_time=1435816471995,
    txn_type=CreateRequest,
    txn_zxid=8589934619,
    zxid=8589934619,
   )
   Proposal(
    cxid=6,
    dst=10.0.0.1:48604,
    length=110,
    session_id=0x34e4d23b0d70001,
    src=10.0.0.1:2889,
    timestr=22:54:31:995755,
    txn_time=1435816471995,
    txn_type=CreateRequest,
    txn_zxid=8589934619,
    zxid=8589934619,
   )
   Proposal(
    cxid=6,
    dst=10.0.0.3:48605,
    length=110,
    session_id=0x34e4d23b0d70001,
    src=10.0.0.1:2889,
    timestr=22:54:31:995770,
    txn_time=1435816471995,
    txn_type=CreateRequest,
    txn_zxid=8589934619,
    zxid=8589934619,
   )
   Ack(
    dst=10.0.0.1:2889,
    length=20,
    src=10.0.0.1:48603,
    timestr=22:54:31:996068,
    zxid=8589934619,
   )
   Ack(
    dst=10.0.0.1:2889,
    length=20,
    src=10.0.0.1:48604,
    timestr=22:54:31:996316,
    zxid=8589934619,
   )
   Ack(
    dst=10.0.0.1:2889,
    length=20,
    src=10.0.0.1:48604,
    timestr=22:54:31:996318,
    zxid=8589934619,
   )
   Commit(
    dst=10.0.0.1:48603,
    length=20,
    src=10.0.0.1:2889,
    timestr=22:54:31:996193,
    zxid=8589934619,
   )
   Commit(
    dst=10.0.0.2:48604,
    length=20,
    src=10.0.0.1:2889,
    timestr=22:54:31:996195,
    zxid=8589934619,
   )
   Commit(
    dst=10.0.0.2:48605,
    length=20,
    src=10.0.0.1:2889,
    timestr=22:54:31:996442,
    zxid=8589934619,
   )

### OS X ###
Although no one has tried running this on OS X in production, it can be used for some parts of development and unit testing. If you are running on OS X, please run the following to install the correct dependencies:

.. code-block:: bash

   $ pip install -r ./osx_requirements.txt

### Dependencies ###
* Python 2.7 (Py3K soon)
* ansicolors
* dpkt-fix
* hexdump
* psutil>=2.1.0
* scapy==2.4.2
* six
* twitter.common.app
* twitter.common.collections
* twitter.common.exceptions
* twitter.common.http
* twitter.common.log
