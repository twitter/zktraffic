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

from zktraffic.cli.printer import Printer
from zktraffic.fle.message import (
  Initial,
  Notification,
  PeerState,
)


def test_fle():
  now = time.time()
  init = Initial(
    timestamp=now,
    src="10.0.0.1:32000",
    dst="10.0.0.2:3888",
    server_id=1,
    election_addr="10.0.0.1:3888"
  )
  notif = Notification(
    timestamp=now,
    src="10.0.0.1:32000",
    dst="10.0.0.2:3888",
    state=PeerState.LOOKING,
    leader=4,
    zxid=0x100,
    peer_epoch=0x1,
    election_epoch=0x1,
    version=2,
    config="config"
  )

  output = StringIO()
  printer = Printer(colors=False, output=output)

  printer.add(init)
  printer.add(notif)

  # wait for the messages to be consumed
  while not printer.empty:
    time.sleep(0.0001)

  # stop it
  printer.stop()
  while not printer.stopped:
    time.sleep(0.0001)

  assert str(init) in output.getvalue()
  assert str(notif) in output.getvalue()
