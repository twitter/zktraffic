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


class QuorumPacket(object):
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
  def from_payload(cls, data, src, dst, timestamp):
    if len(data) < cls.MIN_SIZE:
      raise BadPacket("Too small")

    ptype, offset = read_number(data, 0)
    if PacketType.invalid(ptype):
      raise BadPacket("Invalid type")

    zxid, _ = read_long(data, offset)

    return cls(timestamp, src, dst, ptype, zxid, len(data))

  def __str__(self):
    return "QuorumPacket(\n%s=%s,\n%s=%s,\n%s=%s,\n%s=%s,\n%s=%s,\n%s=%s\n)\n" % (
      " " * 5 + "timestamp", self.timestr,
      " " * 5 + "src", self.src,
      " " * 5 + "dst", self.dst,
      " " * 5 + "type", self.type_literal,
      " " * 5 + "zxid", self.zxid,
      " " * 5 + "length", self.length,
    )
