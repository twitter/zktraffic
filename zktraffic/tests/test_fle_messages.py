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
from zktraffic.fle.message import Message


class MessagesTestCase(unittest.TestCase):
  def test_initial_message(self):
    payload = '%s%s%s%s' % (
      '\xff\xff\xff\xff\xff\xff\x00\x00',  # proto version: -65536L
      '\x00\x00\x00\x00\x00\x00\x00\x06',  # server id
      '\x00\x00\x00\x0e',                  # addr len
      '127.0.0.1:3888',                    # addr
    )
    init = Message.from_payload(payload, '127.0.0.1:3888', '127.0.0.1:9000', 0)
    self.assertEqual(6, init.server_id)
    self.assertEqual('127.0.0.1:3888', init.election_addr)

  def test_notification_28(self):
    payload = '%s%s%s%s' % (
      '\x00\x00\x00\x01',                  # state
      '\x00\x00\x00\x00\x00\x00\x00\x03',  # leader
      '\x00\x00\x00\x00\x00\x00 \x00',     # zxid
      '\x00\x00\x00\x00\x00\x00\x00\n',    # election epoch
    )
    notif = Message.from_payload(payload, '127.0.0.1:3888', '127.0.0.1:9000', 0)
    self.assertEqual(1, notif.state)
    self.assertEqual(3, notif.leader)
    self.assertEqual(0x2000, notif.zxid)
    self.assertEqual(10, notif.election_epoch)
    self.assertEqual(-1, notif.peer_epoch)
    self.assertEqual('', notif.config)

  def test_notification_36(self):
    payload = '%s%s%s%s%s' % (
      '\x00\x00\x00\x01',                  # state
      '\x00\x00\x00\x00\x00\x00\x00\x03',  # leader
      '\x00\x00\x00\x00\x00\x00 \x00',     # zxid
      '\x00\x00\x00\x00\x00\x00\x00\n',    # election epoch
      '\x00\x00\x00\x00\x00\x00\x00\n',    # peer epoch
    )
    notif = Message.from_payload(payload, '127.0.0.1:3888', '127.0.0.1:9000', 0)
    self.assertEqual(1, notif.state)
    self.assertEqual(3, notif.leader)
    self.assertEqual(0x2000, notif.zxid)
    self.assertEqual(10, notif.election_epoch)
    self.assertEqual(10, notif.peer_epoch)
    self.assertEqual('', notif.config)

  def test_notification_with_config(self):
    config = '%s\n%s\n%s\n%s' % (
      'server.0=10.0.0.1:2889:3888:participant;0.0.0.0:2181',
      'server.0=10.0.0.2:2889:3888:participant;0.0.0.0:2181',
      'server.0=10.0.0.3:2889:3888:participant;0.0.0.0:2181',
      'version=deadbeef'
    )
    payload = '%s%s%s%s%s%s' % (
      '\x00\x00\x00\x01',                  # state
      '\x00\x00\x00\x00\x00\x00\x00\x03',  # leader
      '\x00\x00\x00\x00\x00\x00 \x00',     # zxid
      '\x00\x00\x00\x00\x00\x00\x00\n',    # election epoch
      '\x00\x00\x00\x00\x00\x00\x00\n',    # peer epoch
      config,
    )
    notif = Message.from_payload(payload, '127.0.0.1:3888', '127.0.0.1:9000', 0)
    self.assertEqual(1, notif.state)
    self.assertEqual(3, notif.leader)
    self.assertEqual(0x2000, notif.zxid)
    self.assertEqual(10, notif.election_epoch)
    self.assertEqual(10, notif.peer_epoch)
    self.assertEqual(config, notif.config)

  def test_invalid_state(self):
    payload = '%s%s%s%s' % (
      '\x00\x00\x00\x05',                  # bad state
      '\x00\x00\x00\x00\x00\x00\x00\x03',  # leader
      '\x00\x00\x00\x00\x00\x00 \x00',     # zxid
      '\x00\x00\x00\x00\x00\x00\x00\n',    # election epoch
    )
    self.assertRaises(BadPacket, Message.from_payload, payload, '127.0.0.1:388', '127.0.0.1:900', 0)
