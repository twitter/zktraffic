# Zktraffic #

[TOC]


## tl;dr ##

ZooKeeper protocol analyzer and stats gathering daemon

### What is Zktraffic? ###

An {iptraf,top}-esque traffic monitor for ZooKeeper. Right now it exports
per-path (and global) stats. Eventually it'll be made to export per-user
stats too.

It has a front-end, zk-dump, that can be used in interactive mode to dump traffic:

```
# need root or CAP_NET_ADMIN & CAP_NET_RAW
$ sudo zk-dump --iface eth0
Writing log files to disk in /var/tmp
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

```
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

```

Other relevant endpoints for stats are:

* /json/ips: top-N per-ip stats
* /json/auths: per-auth stats
* /json/auths-dump: a full dump of known auths


### Dependencies ###
* ansicolors
* dpkt-fix
* twitter.common.app
* twitter.common.collections
* twitter.common.exceptions
* twitter.common.http
* twitter.common.log
* scapy==2.2.0-dev
