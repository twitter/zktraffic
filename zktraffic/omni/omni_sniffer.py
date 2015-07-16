#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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

import dpkt
import hexdump
import os
import signal
import struct
import socket
import sys
import time
import traceback

from scapy.packet import Raw, Packet
from scapy.sendrecv import sniff

from zktraffic.base.network import BadPacket, get_ip, get_ip_packet, SnifferBase
from zktraffic.base.sniffer import Sniffer as ZKSniffer
from zktraffic.base.util import read_long, read_string, QuorumConfig
from zktraffic.network.sniffer import Sniffer
import zktraffic.fle.message as FLE
import zktraffic.zab.quorum_packet as ZAB


class OmniSniffer(SnifferBase):
  def __init__(self,
               fle_sniffer_factory,
               zab_sniffer_factory,
               zk_sniffer_factory,
               pfilter="tcp",
               dump_bad_packet=False,
               start=True):
    super(OmniSniffer, self).__init__()
    self.setDaemon(True)

    self.fle_sniffer_factory = fle_sniffer_factory
    self.zab_sniffer_factory = zab_sniffer_factory
    self.zk_sniffer_factory = zk_sniffer_factory
    self._sniffers = {}  # dict[(str,int), SnifferBase]

    self._pfilter = pfilter
    self._dump_bad_packet = dump_bad_packet
    self._last_tcp_seq = {}  # dict[((str,int),(str,int)), int]

    if start:  # pragma: no cover
      self.start()

  def run(self, *args, **kwargs):
    try:
      sniff_kwargs = {"filter": self._pfilter, "store": 0, "prn": self.handle_packet}

      if "offline" in kwargs:
        sniff_kwargs["offline"] = kwargs["offline"]

      sniff(**sniff_kwargs)
    except socket.error as ex:
      sys.stderr.write("Error: %s, filter: %s\n" % (ex, self._pfilter))
    finally:
      if "offline" not in kwargs:
        os.kill(os.getpid(), signal.SIGINT)

  def handle_packet(self, packet):
    try:
      message = self.message_from_packet(packet)
      sniffer = self._find_sniffer_for_packet(packet)
      sniffer.handle_message(message)
      self.handle_message(message)
    except (BadPacket, struct.error) as ex:
      if self._dump_bad_packet:
        print("got: %s" % str(ex))
        hexdump.hexdump(packet.load)
        traceback.print_exc()
        sys.stdout.flush()
    except Exception as ex:
      print("got: %s" % str(ex))
      hexdump.hexdump(packet.load)
      traceback.print_exc()
      sys.stdout.flush()

  def handle_message(self, message):
    # print('OMNI DUMP MESSAGE %s' % message)
    pass

  def message_from_packet(self, packet):
    """

    :param packet: scapy.packet.Packet
    :return: message
    """
    self._check_packet(packet)

    message = self._dispatch_message_from_packet(packet)
    if message:
      if isinstance(message, FLE.Notification):
        self._setup_on_fle_notification(packet, message)
      return message

    if self._is_packet_fle_initial(packet):
      message = self._fle_message_from_packet(packet)
      assert isinstance(message, FLE.Initial)
      self._setup_on_fle_initial(packet, message)
      return message

    raise BadPacket("Unknown packet")

  def _parse_packet_src_dst(self, packet):
    assert isinstance(packet, Packet)
    ip_p = get_ip_packet(packet.load)
    src = (get_ip(ip_p, ip_p.src), ip_p.data.sport)
    dst = (get_ip(ip_p, ip_p.dst), ip_p.data.dport)
    ret = (src, dst)
    return ret

  def _find_sniffer_for_packet(self, packet):
    sniffer = None
    src, dst = self._parse_packet_src_dst(packet)
    if src in self._sniffers: sniffer = self._sniffers[src]
    if dst in self._sniffers: sniffer = self._sniffers[dst]
    assert sniffer is None or isinstance(sniffer, SnifferBase)
    return sniffer

  def _dispatch_message_from_packet(self, packet):
    message = None
    sniffer = self._find_sniffer_for_packet(packet)
    if sniffer:
      sniffer_type = self._get_sniffer_type(sniffer)
      assert sniffer_type in ('fle', 'zab', 'zk')
      message = sniffer.message_from_packet(packet)
    return message

  def _fle_message_from_packet(self, packet):
    src, dst = self._parse_packet_src_dst(packet)
    data = get_ip_packet(packet.load).data.data
    message = FLE.Message.from_payload(data,
                                       '%s:%d' % (src[0], src[1]),
                                       '%s:%d' % (dst[0], dst[1]),
                                       time.time())
    return message

  def _check_packet(self, packet):
    """
    check tcp seq duplicates
    NOTE: TX/RX duplicate happens on loopback interfaces
    :param packet:
    :return: None
    """
    src, dst = self._parse_packet_src_dst(packet)
    tcp = get_ip_packet(packet.load).data
    if tcp.flags & dpkt.tcp.TH_RST:
      if (src, dst) in self._last_tcp_seq:
        del self._last_tcp_seq[(src, dst)]
    else:
      if not tcp.data: raise BadPacket("no payload")
      if (src, dst) in self._last_tcp_seq:
        last_seq = self._last_tcp_seq[(src, dst)]
        if tcp.seq <= last_seq:
          # this exception eliminates dups
          raise BadPacket("This sequence(%d<=%d) seen before" % (tcp.seq, last_seq))
      self._last_tcp_seq[(src, dst)] = tcp.seq

  def _is_packet_fle_initial(self, packet):
    data = get_ip_packet(packet.load).data.data

    proto, offset = read_long(data, 0)
    if proto != FLE.Initial.PROTO_VER: return False

    server_id, offset = read_long(data, offset)
    if server_id < 0: return False

    election_addr, offset = read_string(data, offset)
    if election_addr.count(":") != 1: return False

    expected_len = 8 + 8 + 4 + len(election_addr)
    if len(data) != expected_len: return False

    return True

  def _setup_on_fle_initial(self, packet, message):
    """
    setup self._sniffers on FLE.Initial
    :param packet:
    :param message:
    """
    assert isinstance(message, FLE.Initial)
    src, dst = self._parse_packet_src_dst(packet)
    self._regist_sniffer(dst[0], dst[1], 'fle')

  def _setup_on_fle_notification(self, packet, message):
    """
    parse config and setup self._sniffers on FLE.Notification
    :param packet:
    :param message:
    """
    assert isinstance(message, FLE.Notification)
    src, dst = self._parse_packet_src_dst(packet)
    config = QuorumConfig(message.config)
    servers = [x for x in config.entries if isinstance(x, QuorumConfig.Server)]
    for server in servers:
      zab_fle_ip = server.zab_fle_hostname
      if zab_fle_ip == 'localhost':
        assert src[0] == '127.0.0.1', 'foreign localhost(src %s != 127.0.0.1)' % src[0]
        assert dst[0] == '127.0.0.1', 'foreign localhost(dst %s != 127.0.0.1)' % dst[0]
        zab_fle_ip = '127.0.0.1'

      zk_ip = server.zk_hostname
      if zk_ip in ('localhost', '0.0.0.0'):
        zk_ip = zab_fle_ip

      try:
        # check whether correct ip string
        socket.inet_aton(zab_fle_ip)
        socket.inet_aton(zk_ip)
      except Exception as e:
        raise BadPacket(e)

      self._regist_sniffer(zab_fle_ip, server.fle_port, 'fle')
      self._regist_sniffer(zab_fle_ip, server.zab_port, 'zab')
      self._regist_sniffer(zk_ip, server.zk_port, 'zk')

  def _regist_sniffer(self, ip, port, type):
    if (ip, port) in self._sniffers:
      current_sniffer = self._sniffers[(ip, port)]
      current_sniffer_type = self._get_sniffer_type(current_sniffer)
      assert type == current_sniffer_type, 'Conflict %s vs %s' % (type, current_sniffer_type)
      return

    if type == 'fle':
      sniffer = self.fle_sniffer_factory(port)
    elif type == 'zab':
      sniffer = self.zab_sniffer_factory(port)
    elif type == 'zk':
      sniffer = self.zk_sniffer_factory(port)
    else:
      raise ValueError('Unknown sniffer type %s' % type)
    print('OMNI DUMP REGISTERED SNIFFER %s(%s) (%s,%d)' % (sniffer, type, ip, port))
    self._sniffers[(ip, port)] = sniffer

  def _get_sniffer_type(self, sniffer):
    if isinstance(sniffer, Sniffer):
      if sniffer._msg_cls == FLE.Message:
        return 'fle'
      elif sniffer._msg_cls == ZAB.QuorumPacket:
        return 'zab'
      else:
        raise ValueError('Unknown sniffer msg cls %s' % sniffer._msg_cls)
    else:
      if isinstance(sniffer, ZKSniffer):
        return 'zk'
    raise ValueError('Unknown sniffer %s' % sniffer)
