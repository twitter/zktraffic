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

import unittest

from zktraffic.base.network import BadPacket
from zktraffic.base.zookeeper import OpCodes
from zktraffic.network.sniffer import Sniffer
from zktraffic.zab.quorum_packet import (
  Ack,
  Commit,
  PacketType,
  Ping,
  Proposal,
  QuorumPacket,
  Request,
)

from .common import get_full_path


LEADER_PORT = 20022


class ZabTestCase(unittest.TestCase):
  def test_basic(self):
    payload = '%s%s%s' % (
      '\x00\x00\x00\x02',                  # type
      '\x00\x00\x00\x00\x00\x00\x07\xd0',  # zxid
      'cchenwashere',                      # data
    )
    packet = QuorumPacket.from_payload(payload, '127.0.0.1:2889', '127.0.0.1:10000', 0)
    self.assertEqual(PacketType.PROPOSAL, packet.type)
    self.assertEqual(packet.zxid, 2000)

  def test_from_pcap(self):
    requests = []
    proposals = []
    commits = []
    acks = []
    pings = []

    def handler(message):
      if isinstance(message, Request):
        requests.append(message)
      elif isinstance(message, Proposal):
        proposals.append(message)
      elif isinstance(message, Commit):
        commits.append(message)
      elif isinstance(message, Ack):
        acks.append(message)
      elif isinstance(message, Ping):
        pings.append(message)

    sniffer = Sniffer(
      iface="test",
      port=LEADER_PORT,
      msg_cls=QuorumPacket,
      handler=handler,
      dump_bad_packet=False,
      start=False
    )

    sniffer.run(offline=get_full_path('zab_request'))

    # requests
    assert len(requests) == 3

    assert requests[0].req_type == OpCodes.CREATESESSION
    assert requests[0].session_id_literal == "0x1001df405af0000"
    assert requests[0].zxid == -1

    assert requests[1].req_type == OpCodes.CREATE
    assert requests[1].session_id_literal == "0x1001df405af0000"
    assert requests[0].zxid == -1

    assert requests[1].req_type == OpCodes.CREATE
    assert requests[1].session_id_literal == "0x1001df405af0000"
    assert requests[0].zxid == -1

    # proposals
    assert len(proposals) == 6  # 2 createSession + 4 create

    assert proposals[0].txn_type == OpCodes.CREATESESSION
    assert proposals[0].session_id_literal == "0x1001df405af0000"
    assert proposals[0].zxid_literal == "0x100000001"

    assert proposals[2].txn_type == OpCodes.CREATE
    assert proposals[2].session_id_literal == "0x1001df405af0000"
    assert proposals[2].zxid_literal == "0x100000002"

    # commits
    assert len(commits) == 6  # 2 createSession + 4 create

    assert commits[0].zxid_literal == "0x100000001"
    assert commits[2].zxid_literal == "0x100000002"
    assert commits[4].zxid_literal == "0x100000003"

    # acks
    assert len(acks) == 6  # 2 createSession + 4 create

    assert acks[0].zxid_literal == "0x100000001"
    assert acks[2].zxid_literal == "0x100000002"
    assert acks[4].zxid_literal == "0x100000003"

    # pings
    assert len(pings) == 57  # for such a short run, this numbers looks too high

    assert pings[0].zxid_literal == "0x100000000"
