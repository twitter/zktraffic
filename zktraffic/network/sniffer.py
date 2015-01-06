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

from __future__ import print_function
from collections import defaultdict
from threading import Thread
import logging
import os
import signal
import socket
import struct
import sys

from zktraffic.base.network import BadPacket, get_ip, get_ip_packet

from scapy.sendrecv import sniff
from scapy.config import conf as scapy_conf
from twitter.common import log


scapy_conf.logLevel = logging.ERROR  # shush scappy


MAX_PACKET_SIZE = 65535


class Sniffer(Thread):
  """
  A generic & simple packet sniffer
  """
  class RegistrationError(Exception): pass

  def __init__(self, iface, port, msg_cls, handler=None):
    super(Sniffer, self).__init__()
    self.setDaemon(True)

    self._msg_cls = msg_cls
    self._iface = iface
    self._port = port
    self._packet_size = MAX_PACKET_SIZE
    self._handlers = []

    if handler is not None:
      self.add_handler(handler)

    self.start()

  def add_handler(self, handler):
    if handler is None:
      raise self.RegistrationError("handler is none")

    if handler in self._handlers:
      raise self.RegistrationError("handler %s has already been added" % handler)

    self._handlers.append(handler)

  def pause(self):
    """ TODO(rgs): scapy doesn't expose a way to call breakloop() """
    pass

  def unpause(self):
    """ TODO(rgs): scapy doesn't expose a way to call unpause the main loop() """
    pass

  def run(self):
    pfilter = "port %d" % self._port
    try:
      log.info("Setting filter: %s", pfilter)
      sniff(filter=pfilter, store=0, prn=self.handle_packet, iface=self._iface)
    except socket.error as ex:
      log.error("Error: %s, device: %s", ex, self._iface)
    finally:
      log.info("The sniff loop exited")
      os.kill(os.getpid(), signal.SIGINT)

  def handle_packet(self, packet):
    try:
      message = self._message_from_packet(packet)
      for h in self._handlers:
        h(message)
    except (BadPacket, struct.error):
      pass
    except Exception as ex:
      print("got: %s" % str(ex))
      sys.stdout.flush()

  def _message_from_packet(self, packet):
    """
    :returns: Returns an instance of Message
    :raises:
      :exc:`BadPacket` if the packet is of an unknown type
      :exc:`DeserializationError` if deserialization failed
      :exc:`struct.error` if deserialization failed
    """
    ip_p = get_ip_packet(packet.load, 0, self._port)
    if ip_p.data.sport != self._port and ip_p.data.dport != self._port:
      raise BadPacket("Wrong port")

    return self._msg_cls.from_payload(
      ip_p.data.data,
      intern("%s:%s" % (get_ip(ip_p, ip_p.src), ip_p.data.sport)),
      intern("%s:%s" % (get_ip(ip_p, ip_p.dst), ip_p.data.dport)),
      packet.time
    )
