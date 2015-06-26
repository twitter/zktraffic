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

try:
  from StringIO import StringIO
except ImportError:
  from io import StringIO

import time

from zktraffic.base.zookeeper import OpCodes
from zktraffic.cli.printer import Printer
from zktraffic.zab.quorum_packet import (
  Request,
  PacketType,
  Proposal,
)


def test_zab():
  now = time.time()

  request = Request(
    timestamp=now,
    src="10.0.0.3:2889",
    dst="10.0.0.4:58566",
    ptype=PacketType.REQUEST,
    zxid=-1,
    length=100,
    session_id=0xdeadbeef,
    cxid=90000,
    req_type=OpCodes.CREATE,
  )

  proposal = Proposal(
    timestamp=now,
    src="10.0.0.3:2889",
    dst="10.0.0.4:58566",
    ptype=PacketType.PROPOSAL,
    zxid=0x100,
    length=100,
    session_id=0xdeadbeef,
    cxid=90000,
    txn_zxid=0x100,
    txn_time=now,
    txn_type=OpCodes.CREATE,
  )

  output = StringIO()
  printer = Printer(colors=False, output=output)

  printer.add(request)
  printer.add(proposal)

  # consume all messages
  while not printer.empty:
    time.sleep(0.0001)

  # stop it
  printer.stop()
  while not printer.stopped:
    time.sleep(0.0001)

  assert str(request) in output.getvalue()
  assert str(proposal) in output.getvalue()
