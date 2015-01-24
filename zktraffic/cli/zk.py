# -*- coding: utf-8 -*-

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

from collections import defaultdict, deque
from datetime import datetime
import socket
import sys
import threading
import time
import zlib

from zktraffic.base.sniffer import Sniffer, SnifferConfig

import colors
from twitter.common import app


def setup():
  app.add_option('--iface', default='eth0', type=str)
  app.add_option('--client-port', default=0, type=int)
  app.add_option('--zookeeper-port', default=2181, type=int)
  app.add_option('--max-queued-requests', default=10000, type=int)
  app.add_option('--unpaired', default=False, action='store_true', help='Don\'t pair reqs/reps')
  app.add_option('--exclude-host',
                 dest='excluded_hosts',
                 metavar='HOST',
                 default=[],
                 action='append',
                 help='Host that should be excluded (you can use this multiple times)')
  app.add_option('--include-host',
                 dest='included_hosts',
                 metavar='HOST',
                 default=[],
                 action='append',
                 help='Host that should be included (you can use this multiple times)')
  app.add_option('-p', '--include-pings', default=False, action='store_true')
  app.add_option('-c', '--colors', default=False, action='store_true')


class Requests(object):
  def __init__(self):
    self.requests_by_xid = defaultdict(list)

  def add(self, req):
    self.requests_by_xid[req.xid].append(req)

  def pop(self, xid):
    return self.requests_by_xid.pop(xid) if xid in self.requests_by_xid else []


right_arrow = lambda i: "%s%s" % ("—" * i * 4, "►" if i > 0 else "")


def format_timestamp(timestamp):
  dt = datetime.fromtimestamp(timestamp)
  return dt.strftime("%H:%M:%S:%f")


class BasePrinter(threading.Thread):
  NUM_COLORS = len(colors.COLORS)

  def __init__(self, colors, loopback):
    super(BasePrinter, self).__init__()
    self.write = self.colored_write if colors else self.simple_write
    self.loopback = loopback

    self.setDaemon(True)

  def run(self):
    pass

  def request_handler(self, req):
    pass

  def reply_handler(self, rep):
    pass

  def event_handler(self, rep):
    pass

  def colored_write(self, *msgs):
    c = colors.COLORS[zlib.adler32(msgs[0].client) % self.NUM_COLORS]
    cfunc = getattr(colors, c)
    for i, m in enumerate(msgs):
      sys.stdout.write(cfunc("%s%s %s" % (right_arrow(i), format_timestamp(m.timestamp), m)))
    sys.stdout.flush()

  def simple_write(self, *msgs):
    for i, m in enumerate(msgs):
      sys.stdout.write("%s%s %s" % (right_arrow(i), format_timestamp(m.timestamp), m))
    sys.stdout.flush()


class DefaultPrinter(BasePrinter):
  def __init__(self, colors, loopback):
    super(DefaultPrinter, self).__init__(colors, loopback)
    self._requests_by_client = defaultdict(Requests)
    self._replies = deque()

  def run(self):
    while True:
      try:
        rep = self._replies.popleft()
      except IndexError:
        time.sleep(0.01)
        continue

      reqs = self._requests_by_client[rep.client].pop(rep.xid)
      if not reqs:
        continue

      # HACK: if we are on the loopback, drop dupes
      msgs = reqs[0:1] + [rep] if self.loopback else reqs + [rep]
      self.write(*msgs)

  def request_handler(self, req):
    self._requests_by_client[req.client].add(req)

  def reply_handler(self, rep):
    self._replies.append(rep)

  def event_handler(self, evt):
    """ TODO: a queue for this would be good to avoid blocking pcap """
    self.write(evt)


class UnpairedPrinter(BasePrinter):
  def __init__(self, colors, loopback):
    super(UnpairedPrinter, self).__init__(colors, loopback)
    self._messages = deque()

  def run(self):
    while True:
      try:
        msg = self._messages.popleft()
      except IndexError:
        time.sleep(0.01)
        continue

      self.write(msg)

  def request_handler(self, req):
    self._messages.append(req)

  def reply_handler(self, rep):
    self._messages.append(rep)

  def event_handler(self, evt):
    self._messages.append(evt)


def expand_hosts(hosts):
  """ given a list of hosts, expand to its IPs """
  ips = set()

  for host in hosts:
    ips.update(get_ips(host))

  return list(ips)


def get_ips(host, port=0):
  """ lookup all IPs (v4 and v6) """
  ips = set()

  for af_type in (socket.AF_INET, socket.AF_INET6):
    try:
      records = socket.getaddrinfo(host, port, af_type, socket.SOCK_STREAM)
      ips.update(rec[4][0] for rec in records)
    except socket.gaierror as ex:
      if af_type == socket.AF_INET:
        sys.stderr.write("Skipping host: no IPv4s for %s\n" % host)
      else:
        sys.stderr.write("Skipping host: no IPv6s for %s\n" % host)

  return ips


def main(_, options):
  config = SnifferConfig(options.iface)
  config.track_replies = True
  config.zookeeper_port = options.zookeeper_port
  config.max_queued_requests = options.max_queued_requests
  config.client_port = options.client_port if options.client_port != 0 else config.client_port

  if options.excluded_hosts and options.included_hosts:
    sys.stderr.write("The flags --include-host and --exclude-host can't be mixed")
    sys.exit(1)

  if options.excluded_hosts:
    config.excluded_ips += expand_hosts(options.excluded_hosts)
  elif options.included_hosts:
    config.included_ips += expand_hosts(options.included_hosts)

  config.update_filter()

  if options.include_pings:
    config.include_pings()

  loopback = options.iface in ["lo", "lo0"]

  if options.unpaired:
    p = UnpairedPrinter(options.colors, loopback)
  else:
    p = DefaultPrinter(options.colors, loopback)
  p.start()

  sniffer = Sniffer(config, p.request_handler, p.reply_handler, p.event_handler)
  sniffer.start()

  try:
    while True:
      time.sleep(60)
  except (KeyboardInterrupt, SystemExit):
    pass

  sys.stdout.write("\033[0m")
  sys.stdout.flush()


if __name__ == '__main__':
  setup()
  app.main()
