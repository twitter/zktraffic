# ==================================================================================================
# Copyright 2014 Twitter, Inc.
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

from unittest import TestCase
import os
import signal
import socket
import sys

from zktraffic.base.sniffer import Sniffer, SnifferConfig

from scapy.sendrecv import sniff
import mock

class TestSniffer(TestCase):
  def setUp(self):
    self.zkt = Sniffer(SnifferConfig())

  @mock.patch('os.kill', spec=os.kill)
  @mock.patch('zktraffic.base.sniffer.sniff', spec=sniff)
  def test_run_socket_error(self, mock_sniff, mock_kill):
    mock_sniff.side_effect = socket.error

    self.zkt.run()

    mock_sniff.assert_called_once_with(
        filter=self.zkt.config.filter,
        store=0,
        prn=self.zkt.handle_packet,
        iface=self.zkt.config.iface)
    mock_kill.assert_called_once_with(os.getpid(), signal.SIGINT)

  @mock.patch('os.kill', spec=os.kill)
  @mock.patch('zktraffic.base.sniffer.sniff', spec=sniff)
  def test_run(self, mock_sniff, mock_kill):
    self.zkt.run()

    mock_sniff.assert_called_once_with(
        filter=self.zkt.config.filter,
        store=0,
        prn=self.zkt.handle_packet,
        iface=self.zkt.config.iface)
    mock_kill.assert_called_once_with(os.getpid(), signal.SIGINT)
