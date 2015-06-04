# ==================================================================================================
# Copyright 2015 Twitter, Inc.
# --------------------------------------------------------------------------------------------------
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this work except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file, or at:
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==================================================================================================

from datetime import datetime

from zktraffic.base.network import BadPacket
from zktraffic.base.util import read_long, read_number
from zktraffic.base.zookeeper import OpCodes, ZK_REQUEST_TYPES


class PacketType(object):
  REQUEST             =  1
  PROPOSAL            =  2
  ACK                 =  3
  COMMIT              =  4
  PING                =  5
  REVALIDATE          =  6
  SYNC                =  7
  INFORM              =  8
  COMMITANDACTIVATE   =  9
  NEWLEADER           = 10
  FOLLOWERINFO        = 11
  UPTODATE            = 12
  DIFF                = 13
  TRUNC               = 14
  SNAP                = 15
  OBSERVERINFO        = 16
  LEADERINFO          = 17
  ACKEPOCH            = 18
  INFORMANDACTIVATE   = 19

  VALID = range(REQUEST, INFORMANDACTIVATE + 1)

  NAMES = [
    "zero",
    "request",
    "proposal",
    "ack",
    "commit",
    "ping",
    "revalidate",
    "sync",
    "inform",
    "commitandactivate",
    "newleader",
    "followerinfo",
    "uptodate",
    "diff",
    "trunc",
    "snap",
    "observerinfo",
    "leaderinfo",
    "ackepoch",
    "informandactivate",
  ]

  @classmethod
  def invalid(cls, ptype):
    return ptype not in cls.VALID

  @classmethod
  def to_str(cls, ptype):
    return "" if cls.invalid(ptype) else cls.NAMES[ptype]


class QuorumPacketBase(type):
  TYPES = {}
  PTYPE = None

  def __new__(cls, clsname, bases, dct):
    obj = super(QuorumPacketBase, cls).__new__(cls, clsname, bases, dct)
    if obj.PTYPE in cls.TYPES:
      raise ValueError("Duplicate ptype name: %s" % obj.PTYPE)
    else:
      if obj.PTYPE is not None:
        cls.TYPES[obj.PTYPE] = obj
      return obj

  @classmethod
  def get(cls, key, default=None):
    return cls.TYPES.get(key, default)


class QuorumPacket(QuorumPacketBase("QuorumPacketBase", (object,), {})):
  __slots__ = ("timestamp", "src", "dst", "type", "zxid", "length")

  MIN_SIZE = 12

  def __init__(self, timestamp, src, dst, ptype, zxid, length):
    self.timestamp = timestamp
    self.src = src
    self.dst = dst
    self.type = ptype
    self.zxid = zxid
    self.length = length

  @property
  def timestr(self):
    return datetime.fromtimestamp(self.timestamp).strftime("%H:%M:%S:%f")

  @property
  def type_literal(self):
    return PacketType.to_str(self.type)

  @classmethod
  def with_params(cls, timestamp, src, dst, ptype, zxid, data, offset):
    return cls(timestamp, src, dst, ptype, zxid, len(data))

  @classmethod
  def from_payload(cls, data, src, dst, timestamp):
    if len(data) < cls.MIN_SIZE:
      raise BadPacket("Too small")

    ptype, offset = read_number(data, 0)
    if PacketType.invalid(ptype):
      raise BadPacket("Invalid type")

    zxid, offset = read_long(data, offset)
    handler = QuorumPacketBase.get(ptype, cls)
    return handler.with_params(timestamp, src, dst, ptype, zxid, data, offset)

  def __str__(self):
    def gen():
      for k in dir(self):
        v = getattr(self, k)
        cond = (isinstance(v, int) or isinstance(v, basestring)) and \
               not k.isupper() and not k.startswith("_") and not "_literal" in k \
               and not k == "type"
        if cond:
          alt_k = "%s_literal" % k
          if hasattr(self, alt_k):
            v = getattr(self, alt_k)
          yield k, v
          
    s = "%s(\n" % self.__class__.__name__
    for k, v in gen():
        s += " %s=%s,\n" % (k, v)
    s += ")\n"
    return s


class Request(QuorumPacket):
  PTYPE = PacketType.REQUEST
  __slots__ = ("session_id", "cxid", "req_type")
  
  def __init__(self, timestamp, src, dst, ptype, zxid, length,
               session_id, cxid, req_type):
    super(Request, self).__init__(timestamp, src, dst, ptype, zxid, length)
    self.session_id = session_id
    self.cxid = cxid
    self.req_type = req_type

  @property
  def req_type_literal(self):
    return ZK_REQUEST_TYPES[self.req_type] if self.req_type in ZK_REQUEST_TYPES else str(self.req_type)

  @classmethod
  def with_params(cls, timestamp, src, dst, ptype, zxid, data, offset):
    data_len, offset = read_number(data, offset)
    session_id, offset = read_long(data, offset)
    cxid, offset = read_number(data, offset)
    req_type, offset = read_number(data, offset)
    # TODO: dissect the remaining data
    # see server_message.py and client_message.py
    return cls(timestamp, src, dst, ptype, zxid, len(data),
               session_id, cxid, req_type)


class Proposal(QuorumPacket):
  PTYPE = PacketType.PROPOSAL
  __slots__ = ("client_id", "cxid", "txn_zxid", "txn_time", "txn_type")

  def __init__(self, timestamp, src, dst, ptype, zxid, length,
               client_id, cxid, txn_zxid, txn_time, txn_type):
    super(Proposal, self).__init__(timestamp, src, dst, ptype, zxid, length)
    self.client_id = client_id
    self.cxid = cxid
    self.txn_zxid = txn_zxid
    self.txn_time = txn_time
    self.txn_type = txn_type

  @property
  def txn_type_literal(self):
    return ZK_REQUEST_TYPES[self.txn_type] if self.txn_type in ZK_REQUEST_TYPES else str(self.txn_type)

  @classmethod
  def with_params(cls, timestamp, src, dst, ptype, zxid, data, offset):
    data_len, offset = read_number(data, offset)
    client_id, offset = read_long(data, offset)
    cxid, offset = read_number(data, offset)
    txn_zxid, offset = read_long(data, offset)
    txn_time, offset = read_long(data, offset)
    txn_type, offset = read_number(data, offset)
    # TODO: dissect the remaining data
    # see org.apache.zookeeper.server.util.SerializeUtils.deserializeTxn()
    return cls(timestamp, src, dst, ptype, zxid, len(data),
               client_id, cxid, txn_zxid, txn_time, txn_type)


class Ack(QuorumPacket):
  PTYPE = PacketType.ACK


class Commit(QuorumPacket):
  PTYPE = PacketType.COMMIT


class Ping(QuorumPacket):
  PTYPE = PacketType.PING
  # TODO: dissect the data (in almost all cases, data is null)


class Revalidate(QuorumPacket):
  PTYPE = PacketType.REVALIDATE
  __slots__ = ("session_id", "timeout")
  def __init__(self, timestamp, src, dst, ptype, zxid, length,
               session_id, timeout):
    super(Revalidate, self).__init__(timestamp, src, dst, ptype, zxid, length)
    self.session_id = session_id
    self.timeout = timeout

  @classmethod
  def with_params(cls, timestamp, src, dst, ptype, zxid, data, offset):
    data_len, offset = read_number(data, offset)
    session_id, offset = read_long(data, offset)
    timeout, offset = read_number(data, offset)
    return cls(timestamp, src, dst, ptype, zxid, len(data),
               session_id, timeout)


class Sync(QuorumPacket):
  PTYPE = PacketType.SYNC


class Inform(Proposal):
  PTYPE = PacketType.INFORM


class CommitAndActivate(QuorumPacket):
  PTYPE = PacketType.COMMITANDACTIVATE
  __slots__ = ("suggested_leader_id")
  def __init__(self, timestamp, src, dst, ptype, zxid, length,
               suggested_leader_id):
    super(CommitAndActivate, self).__init__(timestamp, src, dst, ptype, zxid, length)
    self.suggested_leader_id = suggested_leader_id

  @classmethod
  def with_params(cls, timestamp, src, dst, ptype, zxid, data, offset):
    data_len, offset = read_number(data, offset)
    suggested_leader_id, offset = read_long(data, offset)
    return cls(timestamp, src, dst, ptype, zxid, len(data),
               suggested_leader_id)


class NewLeader(QuorumPacket):
  PTYPE = PacketType.NEWLEADER
  # TODO: dissect the data (in almost all cases, data is null)


class FollowerInfo(QuorumPacket):
  PTYPE = PacketType.FOLLOWERINFO
  __slots__ = ("sid", "protocol_version", "config_version")
  def __init__(self, timestamp, src, dst, ptype, zxid, length,
               sid, protocol_version, config_version):
    super(FollowerInfo, self).__init__(timestamp, src, dst, ptype, zxid, length)
    self.sid = sid
    self.protocol_version = protocol_version
    self.config_version = config_version

  @classmethod
  def with_params(cls, timestamp, src, dst, ptype, zxid, data, offset):
    data_len, offset = read_number(data, offset)
    sid, offset = read_long(data, offset)
    protocol_version, offset = read_number(data, offset)
    config_version, offset = read_long(data, offset)
    return cls(timestamp, src, dst, ptype, zxid, len(data),
               sid, protocol_version, config_version)


class UpToDate(QuorumPacket):
  PTYPE = PacketType.UPTODATE


class Diff(QuorumPacket):
  PTYPE = PacketType.DIFF


class Trunc(QuorumPacket):
  PTYPE = PacketType.TRUNC


class ObserverInfo(FollowerInfo):
  PTYPE = PacketType.OBSERVERINFO


class LeaderInfo(QuorumPacket):
  PTYPE = PacketType.LEADERINFO
  __slots__ = ("protocol_version")
  def __init__(self, timestamp, src, dst, ptype, zxid, length,
               protocol_version):
    super(LeaderInfo, self).__init__(timestamp, src, dst, ptype, zxid, length)
    self.protocol_version = protocol_version

  @classmethod
  def with_params(cls, timestamp, src, dst, ptype, zxid, data, offset):
    data_len, offset = read_number(data, offset)
    protocol_version, offset = read_number(data, offset)
    return cls(timestamp, src, dst, ptype, zxid, len(data),
               protocol_version)


class AckEpoch(QuorumPacket):
  PTYPE = PacketType.ACKEPOCH
  __slots__ = ("epoch")
  def __init__(self, timestamp, src, dst, ptype, zxid, length,
               epoch):
    super(AckEpoch, self).__init__(timestamp, src, dst, ptype, zxid, length)
    self.epoch = epoch

  @classmethod
  def with_params(cls, timestamp, src, dst, ptype, zxid, data, offset):
    data_len, offset = read_number(data, offset)
    epoch, offset = read_number(data, offset)
    return cls(timestamp, src, dst, ptype, zxid, len(data),
               epoch)


class InformAndActivate(Proposal):
  __slots__ = ("client_id", "cxid", "txn_zxid", "txn_time", "txn_type", "suggested_leader_id")
  PTYPE = PacketType.INFORMANDACTIVATE
  def __init__(self, timestamp, src, dst, ptype, zxid, length,
               suggested_leader_id,
               client_id, cxid, txn_zxid, txn_time, txn_type):
    super(Proposal, self).__init__(timestamp, src, dst, ptype, zxid, length)
    self.suggested_leader_id = suggested_leader_id
    self.client_id = client_id
    self.cxid = cxid
    self.txn_zxid = txn_zxid
    self.txn_time = txn_time
    self.txn_type = txn_type

  @classmethod
  def with_params(cls, timestamp, src, dst, ptype, zxid, data, offset):
    data_len, offset = read_number(data, offset)
    suggested_leader_id, offset = read_long(data, offset)
    client_id, offset = read_long(data, offset)
    cxid, offset = read_number(data, offset)
    txn_zxid, offset = read_long(data, offset)
    txn_time, offset = read_long(data, offset)
    txn_type, offset = read_number(data, offset)
    return cls(timestamp, src, dst, ptype, zxid, len(data),
               suggested_leader_id,
               client_id, cxid, txn_zxid, txn_time, txn_type)
