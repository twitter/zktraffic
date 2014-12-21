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


""" network packets & header processing stuff """

import socket

import dpkt


class Error(Exception): pass
class BadPacket(Error): pass


_loopback = dpkt.loopback.Loopback()
_ethernet = dpkt.ethernet.Ethernet()

def get_ip_packet(data, client_port, server_port, is_loopback=False):
  """ if client_port is 0 any client_port is good """

  header = _loopback if is_loopback else _ethernet

  header.unpack(data)

  tcp_p = getattr(header.data, "data", None)
  if type(tcp_p) != dpkt.tcp.TCP:
    raise BadPacket("Not a TCP packet")

  if tcp_p.dport == server_port:
    if client_port != 0 and tcp_p.sport != client_port:
      raise BadPacket("Request from different client")
  elif tcp_p.sport == server_port:
    if client_port != 0 and tcp_p.dport != client_port:
      raise BadPacket("Reply for different client")
  else:
    raise BadPacket("Packet not for/from client/server")

  return header.data


def get_ip(ip_packet, packed_addr):
  af_type = socket.AF_INET if type(ip_packet) == dpkt.ip.IP else socket.AF_INET6
  return socket.inet_ntop(af_type, packed_addr)
