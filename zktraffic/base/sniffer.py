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


from collections import defaultdict
from random import random
from threading import Thread

import logging
import os
import hexdump
import signal
import socket
import struct
import sys

from .client_message import ClientMessage, Request
from .network import BadPacket, get_ip, get_ip_packet, SnifferBase
from .server_message import Reply, ServerMessage, WatchEvent
from .zookeeper import DeserializationError, OpCodes
from .util import StringTooLong, to_bytes

from scapy.config import conf as scapy_conf
scapy_conf.logLevel = logging.ERROR  # shush scapy

from scapy.sendrecv import sniff
from six.moves import intern
from twitter.common import log


DEFAULT_PORT = 2181
FOUR_LETTER_WORDS = (
  to_bytes('conf'),
  to_bytes('cons'),
  to_bytes('crst'),
  to_bytes('dump'),
  to_bytes('envi'),
  to_bytes('ruok'),
  to_bytes('srst'),
  to_bytes('srvr'),
  to_bytes('stat'),
  to_bytes('wchs'),
  to_bytes('wchc'),
  to_bytes('wchp'),
  to_bytes('mntr'),
  to_bytes('kill'),  # deprecated
  to_bytes('reqs'),  # deprecated
)


class SnifferConfig(object):
  def __init__(self,
      iface="eth0",
      writes_only=False,
      debug=False):
    """
      if client_port is 0 we sniff all clients
      if zookeeper_port is changed later on you must call update_filter()
    """
    self.iface = iface
    self.writes_only = writes_only
    self.debug = debug
    self.client_port = 0
    self.track_replies = False
    self.max_queued_requests = 10000
    self.zookeeper_port = DEFAULT_PORT
    self.excluded_opcodes = set()
    self.is_loopback = iface in ["lo", "lo0"]
    self.read_timeout_ms = 0
    self.dump_bad_packet = False
    self.sampling = 1.0  # percentage of packets to inspect [0, 1]

    # These are set after initialization, and require `update_filter` to be called
    self.included_ips = []
    self.excluded_ips = []

    self.update_filter()
    self.exclude_pings()

  def update_filter(self):
    self.filter = "port %d" % (self.zookeeper_port)

    assert not (self.included_ips and self.excluded_ips)

    if self.excluded_ips:
      self.filter +=  " and host not " + " and host not ".join(self.excluded_ips)
    elif self.included_ips:
      self.filter += " and (host " + " or host ".join(self.included_ips) + ")"

  def include_pings(self):
    self.update_exclusion_list(OpCodes.PING, False)

  def exclude_pings(self):
    self.update_exclusion_list(OpCodes.PING, True)

  def excluded(self, opcode):
    return opcode in self.excluded_opcodes

  def update_exclusion_list(self, opcode, exclude):
    if exclude:
      self.excluded_opcodes.add(opcode)
    else:
      try:
        self.excluded_opcodes.remove(opcode)
      except KeyError:
        pass

  def __str__(self):
    return """
***sniffer config ***
iface = %s
writes_only = %s
filter = %s
zookeeper_port = %d
is_loopback = %s
read_timeout_ms = %d
debug = %s
""" % (self.iface,
          str((self.writes_only)).lower(),
          self.filter,
          self.zookeeper_port,
          str(self.is_loopback),
          self.read_timeout_ms,
          str(self.debug).lower())


class Sniffer(SnifferBase):
  class RegistrationError(Exception): pass

  def __init__(self,
               config,
               request_handler=None,
               reply_handler=None,
               event_handler=None,
               error_to_stderr=False):
    """
    This sniffer will intercept:
     - client requests
     - server replies
     - server events (i.e.: connection state change or, most of the times, watches)
    Hence handlers for each.
    """
    super(Sniffer, self).__init__()

    self._error_to_stderr = error_to_stderr
    self._packet_size = 65535
    self._request_handlers = []
    self._reply_handlers = []
    self._event_handlers = []
    self._requests_xids = defaultdict(dict)  # if tracking replies, keep a tab for seen reqs
    self._four_letter_mode = {}              # key: client addr, val: four letter
    self._wants_stop = False

    self.config = config

    self.add_request_handler(request_handler)
    self.add_reply_handler(reply_handler)
    self.add_event_handler(event_handler)

    self.setDaemon(True)

  def stop(self):
    self._wants_stop = True

  def add_request_handler(self, handler):
    self._add_handler(self._request_handlers, handler)

  def add_reply_handler(self, handler):
    self._add_handler(self._reply_handlers, handler)

  def add_event_handler(self, handler):
    self._add_handler(self._event_handlers, handler)

  def _add_handler(self, handlers, handler):
    if handler is None:
      return

    if handler in handlers:
      raise self.RegistrationError("handler %s has already been added" % (handler))

    handlers.append(handler)

  def wants_stop(self, *args, **kwargs):  # pragma: no cover
    return self._wants_stop

  def run(self):
    try:
      log.info("Setting filter: %s", self.config.filter)
      if self.config.iface == "any":  # pragma: no cover
        sniff(
          filter=self.config.filter,
          store=0,
          prn=self.handle_packet,
          stop_filter=self.wants_stop
        )
      else:
        sniff(
          filter=self.config.filter,
          store=0,
          prn=self.handle_packet,
          iface=self.config.iface,
          stop_filter=self.wants_stop
        )
    except socket.error as ex:
      if self._error_to_stderr:
        sys.stderr.write("Error: %s, device: %s\n" % (ex, self.config.iface))
      else:
        log.error("Error: %s, device: %s", ex, self.config.iface)
    finally:
      log.info("The sniff loop exited")
      os.kill(os.getpid(), signal.SIGINT)

  def handle_packet(self, packet):
    sampling = self.config.sampling
    if sampling < 1.0 and random() > sampling:
      return

    try:
      message = self.message_from_packet(packet)
      self.handle_message(message)
    except (BadPacket, StringTooLong, DeserializationError, struct.error) as ex:
      if self.config.dump_bad_packet:
        print("got: %s" % str(ex))
        hexdump.hexdump(packet.load)
        sys.stdout.flush()

  def handle_message(self, message):
    if message and not self.config.excluded(message.opcode):
      for h in self._handlers_for(message):
        h(message)

  def _handlers_for(self, message):
    if isinstance(message, Request):
      if self.config.writes_only and not message.is_write:
        raise BadPacket("Not a write packet")
      return self._request_handlers
    elif isinstance(message, Reply):
      return self._reply_handlers
    elif isinstance(message, WatchEvent):
      return self._event_handlers

    raise BadPacket("No handlers for: %s" % (message))

  def message_from_packet(self, packet):
    """
    :returns: Returns an instance of ClientMessage or ServerMessage (or a subclass)
    :raises:
      :exc:`BadPacket` if the packet is for a client we are not tracking
      :exc:`DeserializationError` if deserialization failed
      :exc:`struct.error` if deserialization failed
    """
    client_port = self.config.client_port
    zk_port = self.config.zookeeper_port
    ip_p = get_ip_packet(packet.load, client_port, zk_port, self.config.is_loopback)

    if 0 == len(ip_p.data.data):
      return None

    if ip_p.data.dport == zk_port:
      data = ip_p.data.data
      src = intern("%s:%s" % (get_ip(ip_p, ip_p.src), ip_p.data.sport))
      dst = intern("%s:%s" % (get_ip(ip_p, ip_p.dst), ip_p.data.dport))
      client, server = src, dst
      if data.startswith(FOUR_LETTER_WORDS):
        self._set_four_letter_mode(client, data[0:4])
        raise BadPacket("Four letter request %s" % data[0:4])
      client_message = ClientMessage.from_payload(data, client, server)
      client_message.timestamp = packet.time
      self._track_client_message(client_message)
      return client_message

    if ip_p.data.sport == zk_port:
      data = ip_p.data.data
      src = intern("%s:%s" % (get_ip(ip_p, ip_p.src), ip_p.data.sport))
      dst = intern("%s:%s" % (get_ip(ip_p, ip_p.dst), ip_p.data.dport))
      client, server = dst, src
      four_letter = self._get_four_letter_mode(client)
      if four_letter:
        self._set_four_letter_mode(client, None)
        raise BadPacket("Four letter response %s" % four_letter)
      requests_xids = self._requests_xids.get(client, {})
      server_message = ServerMessage.from_payload(data, client, server, requests_xids)
      server_message.timestamp = packet.time
      return server_message

    raise BadPacket("Packet to the wrong port?")

  def _track_client_message(self, request):
    """
    Any request that is not a ping or a close should be tracked
    """
    if self.config.track_replies and not request.is_ping and not request.is_close:
      requests_xids = self._requests_xids[request.client]
      if len(requests_xids) > self.config.max_queued_requests:  # pragma: no cover
        # TODO: logging the counts of each type of pkts in the queue when this happens
        #       could be useful.
        if self._error_to_stderr:
          sys.stderr.write("Too many queued requests, replies for %s will be lost\n" %
                           request.client)
        else:
          log.error("Too many queued requests, replies for %s will be lost", request.client)
        return

      requests_xids[request.xid] = request.opcode

  def _get_four_letter_mode(self, client):
    return self._four_letter_mode.get(client)

  def _set_four_letter_mode(self, client, four_letter):
    if four_letter:
        self._four_letter_mode[client] = four_letter
    else:
        del self._four_letter_mode[client]
