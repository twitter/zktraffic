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
from zktraffic.base.util import read_long, read_number, read_string


class PeerState(object):
  LOOKING = 0
  FOLLOWING = 1
  LEADING = 2
  OBSERVING = 3
  STATES = [LOOKING, FOLLOWING, LEADING, OBSERVING]
  NAMES = [
    "looking",
    "following",
    "leading",
    "observing",
  ]

  @classmethod
  def invalid(cls, state):
    return state not in cls.STATES

  @classmethod
  def to_str(cls, state):
    return "" if cls.invalid(state) else cls.NAMES[state]


class Message(object):
  PROTO_VER = -65536
  OLD_LEN = 28
  WITH_CONFIG_LEN = 36

  __slots__ = ()

  @classmethod
  def from_payload(cls, data, src, dst, timestamp):
    if len(data) < 16:
      raise BadPacket("Too small")

    proto, offset = read_long(data, 0)
    if proto == cls.PROTO_VER:
      server_id, offset = read_long(data, offset)
      election_addr, _ = read_string(data, offset)

      return Initial(timestamp, src, dst, server_id, election_addr)

    if len(data) >= cls.OLD_LEN:
      state, offset = read_number(data, 0)
      if PeerState.invalid(state):
        raise BadPacket("Invalid state: %d" % state)

      leader, offset = read_long(data, offset)
      zxid, offset = read_long(data, offset)
      election_epoch, offset = read_long(data, offset)
      peer_epoch, offset = read_long(data, offset) if len(data) > cls.OLD_LEN else (-1, offset)
      config = data[cls.WITH_CONFIG_LEN:] if len(data) > cls.WITH_CONFIG_LEN else ""

      return Notification(
        timestamp,
        src,
        dst,
        state,
        leader,
        zxid,
        election_epoch,
        peer_epoch,
        config
      )

    raise BadPacket("Unknown unknown")

  @property
  def timestr(self):
    return datetime.fromtimestamp(self.timestamp).strftime("%H:%M:%S:%f")


class Initial(Message):
  __slots__ = ("timestamp", "src", "dst", "server_id", "election_addr")

  def __init__(self, timestamp, src, dst, server_id, election_addr):
    self.timestamp = timestamp
    self.src = src
    self.dst = dst
    self.server_id = server_id
    self.election_addr = election_addr

  def __str__(self):
    return "%s(\n%s=%s,\n%s=%s,\n%s=%s,\n%s=%s,\n%s=%s\n)\n" % (
      "Initial",
      " " * 5 + "timestamp", self.timestr,
      " " * 5 + "src", self.src,
      " " * 5 + "dst", self.dst,
      " " * 5 + "server_id", self.server_id,
      " " * 5 + "election_addr", self.election_addr
    )


class Notification(Message):
  __slots__ = (
    "timestamp",
    "src",
    "dst",
    "state",
    "leader",
    "zxid",
    "election_epoch",
    "peer_epoch",
    "config"
  )

  def __init__(self, timestamp, src, dst, state, leader, zxid, election_epoch, peer_epoch, config):
    self.timestamp = timestamp
    self.src = src
    self.dst = dst
    self.state = state
    self.leader = leader
    self.zxid = zxid
    self.election_epoch = election_epoch
    self.peer_epoch = peer_epoch
    self.config = config

  @property
  def state_literal(self):
    return PeerState.to_str(self.state)

  def __str__(self):
    config = [" " * 10 + cline for cline in self.config.split("\n")]
    return "%s(\n%s=%s,\n%s=%s,\n%s=%s,\n%s=%s,\n%s=%s,\n%s=%s,\n%s=%s,\n%s=%s,\n%s=\n%s\n)\n" % (
      "Notification",
      " " * 5 + "timestamp", self.timestr,
      " " * 5 + "src", self.src,
      " " * 5 + "dst", self.dst,
      " " * 5 + "state", self.state_literal,
      " " * 5 + "leader", self.leader,
      " " * 5 + "zxid", self.zxid,
      " " * 5 + "election_epoch", self.election_epoch,
      " " * 5 + "peer_epoch", self.peer_epoch,
      " " * 5 + "config", "\n".join(config),
    )
