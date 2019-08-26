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

from zktraffic.base.util import to_bytes
from zktraffic.base.network import BadPacket
from zktraffic.fle.message import Message


class MessagesTestCase(unittest.TestCase):
  def test_initial_message(self):
    payload = b''.join((
      b'\xff\xff\xff\xff\xff\xff\x00\x00',  # proto version: -65536L
      b'\x00\x00\x00\x00\x00\x00\x00\x06',  # server id
      b'\x00\x00\x00\x0e',                  # addr len
      b'127.0.0.1:3888',                    # addr
    ))
    init = Message.from_payload(payload, '127.0.0.1:3888', '127.0.0.1:9000', 0)
    self.assertEqual(6, init.server_id)
    self.assertEqual('127.0.0.1:3888', init.election_addr)

  def test_notification_28(self):
    payload = ''.join((
      '\x00\x00\x00\x01',                  # state
      '\x00\x00\x00\x00\x00\x00\x00\x03',  # leader
      '\x00\x00\x00\x00\x00\x00\x20\x00',     # zxid
      '\x00\x00\x00\x00\x00\x00\x00\x0a',    # election epoch
    ))
    notif = Message.from_payload(payload, '127.0.0.1:3888', '127.0.0.1:9000', 0)
    self.assertEqual(1, notif.state)
    self.assertEqual(3, notif.leader)
    self.assertEqual(0x2000, notif.zxid)
    self.assertEqual(10, notif.election_epoch)
    self.assertEqual(-1, notif.peer_epoch)
    self.assertEqual(0, notif.version)
    self.assertEqual('', notif.config)

  def test_notification_36(self):
    payload = ''.join((
      '\x00\x00\x00\x01',                  # state
      '\x00\x00\x00\x00\x00\x00\x00\x03',  # leader
      '\x00\x00\x00\x00\x00\x00\x20\x00',  # zxid
      '\x00\x00\x00\x00\x00\x00\x00\x0a',  # election epoch
      '\x00\x00\x00\x00\x00\x00\x00\x0a',  # peer epoch
    ))
    notif = Message.from_payload(payload, '127.0.0.1:3888', '127.0.0.1:9000', 0)
    self.assertEqual(1, notif.state)
    self.assertEqual(3, notif.leader)
    self.assertEqual(0x2000, notif.zxid)
    self.assertEqual(10, notif.election_epoch)
    self.assertEqual(10, notif.peer_epoch)
    self.assertEqual(0, notif.version)
    self.assertEqual('', notif.config)

  def test_notification_v1(self):
    payload = ''.join((
      '\x00\x00\x00\x01',                  # state
      '\x00\x00\x00\x00\x00\x00\x00\x03',  # leader
      '\x00\x00\x00\x00\x00\x00\x20\x00',  # zxid
      '\x00\x00\x00\x00\x00\x00\x00\x0a',  # election epoch
      '\x00\x00\x00\x00\x00\x00\x00\x0a',  # peer epoch
      '\x00\x00\x00\x01',                  # version
    ))
    notif = Message.from_payload(payload, '127.0.0.1:3888', '127.0.0.1:9000', 0)
    self.assertEqual(1, notif.state)
    self.assertEqual(3, notif.leader)
    self.assertEqual(0x2000, notif.zxid)
    self.assertEqual(10, notif.election_epoch)
    self.assertEqual(10, notif.peer_epoch)
    self.assertEqual(1, notif.version)
    self.assertEqual('', notif.config)

  def test_notification_v2_with_config(self):
    config = '%s\n%s\n%s\n%s' % (
      'server.0=10.0.0.1:2889:3888:participant;0.0.0.0:2181',
      'server.0=10.0.0.2:2889:3888:participant;0.0.0.0:2181',
      'server.0=10.0.0.3:2889:3888:participant;0.0.0.0:2181',
      'version=deadbeef'
    )
    payload = b''.join((
      b'\x00\x00\x00\x01',                  # state
      b'\x00\x00\x00\x00\x00\x00\x00\x03',  # leader
      b'\x00\x00\x00\x00\x00\x00\x20\x00',  # zxid
      b'\x00\x00\x00\x00\x00\x00\x00\x0a',  # election epoch
      b'\x00\x00\x00\x00\x00\x00\x00\x0a',  # peer epoch
      b'\x00\x00\x00\x02',                  # version
      b'\x00\x00\x00\xaf',                  # config length
      to_bytes(config),
    ))
    notif = Message.from_payload(payload, '127.0.0.1:3888', '127.0.0.1:9000', 0)
    self.assertEqual(1, notif.state)
    self.assertEqual(3, notif.leader)
    self.assertEqual(0x2000, notif.zxid)
    self.assertEqual(10, notif.election_epoch)
    self.assertEqual(10, notif.peer_epoch)
    self.assertEqual(2, notif.version)
    self.assertEqual(config, notif.config)

  def test_invalid_state(self):
    payload = ''.join((
      '\x00\x00\x00\x05',                  # bad state
      '\x00\x00\x00\x00\x00\x00\x00\x03',  # leader
      '\x00\x00\x00\x00\x00\x00\x20\x00',  # zxid
      '\x00\x00\x00\x00\x00\x00\x00\x0a',  # election epoch
    ))
    self.assertRaises(BadPacket, Message.from_payload, payload, '127.0.0.1:388', '127.0.0.1:900', 0)
