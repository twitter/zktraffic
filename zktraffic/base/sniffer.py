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
import logging
import os
import signal
import socket
import struct
from threading import Thread

from .client_message import ClientMessage, Request
from .network import BadPacket, get_ip, get_ip_packet
from .server_message import Reply, ServerMessage, WatchEvent
from .zookeeper import DeserializationError, OpCodes

from scapy.sendrecv import sniff
from scapy.config import conf as scapy_conf
from twitter.common import log


scapy_conf.logLevel = logging.ERROR  # shush scapy

DEFAULT_PORT = 2181


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
    self.included_ips = []
    self.excluded_ips = []
    self.excluded_opcodes = set()
    self.is_loopback = False
    self.read_timeout_ms = 0

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
port = %d
is_loopback = %s
read_timeout_ms = %d
debug = %s
""" % (self.iface,
          str((self.writes_only)).lower(),
          self.filter,
          self.port,
          str(self.is_loopback),
          self.read_timeout_ms,
          str(self.debug).lower())


class Sniffer(Thread):
  class RegistrationError(Exception): pass

  def __init__(self,
      config,
      request_handler=None,
      reply_handler=None,
      event_handler=None):
    """
    This sniffer will intercept:
     - client requests
     - server replies
     - server events (i.e.: connection state change or, most of the times, watches)
    Hence handlers for each.
    """
    super(Sniffer, self).__init__()

    self._packet_size = 65535
    self._request_handlers = []
    self._reply_handlers = []
    self._event_handlers = []
    self._requests_xids = defaultdict(dict)  # if tracking replies, keep a tab for seen reqs

    self.config = config

    self.add_request_handler(request_handler)
    self.add_reply_handler(reply_handler)
    self.add_event_handler(event_handler)

    self.setDaemon(True)

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

  def pause(self):
    """ TODO(rgs): scapy doesn't expose a way to call breakloop() """
    pass

  def unpause(self):
    """ TODO(rgs): scapy doesn't expose a way to call unpause the main loop() """
    pass

  def run(self):
    try:
      log.info("Setting filter: %s", self.config.filter)
      sniff(filter=self.config.filter, store=0, prn=self.handle_packet, iface=self.config.iface)
    except socket.error as ex:
      log.error("Error: %s, device: %s", ex, self.config.iface)
    finally:
      log.info("The sniff loop exited")
      os.kill(os.getpid(), signal.SIGINT)

  def handle_packet(self, packet):
    try:
      message = self._message_from_packet(packet)
      if not self.config.excluded(message.opcode):
        for h in self._handlers_for(message):
          h(message)
    except (BadPacket, DeserializationError, struct.error):
      pass

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

  def _message_from_packet(self, packet):
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
    timestamp = packet.time

    if ip_p.data.dport == zk_port:
      client = intern("%s:%s" % (get_ip(ip_p, ip_p.src), ip_p.data.sport))
      client_message = ClientMessage.from_payload(ip_p.data.data, client)
      client_message.timestamp = timestamp
      self._track_client_message(client_message)
      return client_message

    if ip_p.data.sport == zk_port:
      client = intern("%s:%s" % (get_ip(ip_p, ip_p.dst), ip_p.data.dport))
      requests_xids = self._requests_xids.get(client, {})
      server_message = ServerMessage.from_payload(ip_p.data.data, client, requests_xids)
      server_message.timestamp = timestamp
      return server_message

    raise BadPacket("Packet to the wrong port?")

  def _track_client_message(self, request):
    """
    Any request that is not a ping or a close should be tracked
    """
    if self.config.track_replies and not request.is_ping and not request.is_close:
      requests_xids = self._requests_xids[request.client]
      if len(requests_xids) > self.config.max_queued_requests:
        log.error("Too many queued requests, replies for %s will be lost", request.client)
        return
      requests_xids[request.xid] = request.opcode
