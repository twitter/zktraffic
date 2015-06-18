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

from zktraffic import __version__
from zktraffic.base.sniffer import Sniffer, SnifferConfig
from zktraffic.base.zookeeper import OpCodes

import colors
from twitter.common import app
from twitter.common.log.options import LogOptions


def setup():
  LogOptions.set_stderr_log_level('NONE')

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
  app.add_option('--dump-bad-packet', default=False, action='store_true')
  app.add_option('--count-requests', default=0, type=int,
                 help='Count N requests and report a sorted summary (default: sort by path)')
  app.add_option('--sort-by', default='path', type=str,
                 help='Only makes sense with --count-requests. Possible values: path or type')
  app.add_option('--version', default=False, action='store_true')


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
      try:
        self.write(*msgs)
      except IOError:  # PIPE broken, most likely
        break

  def request_handler(self, req):
    # close requests don't have a reply, dispatch it immediately
    if req.opcode == OpCodes.CLOSE:
      self.write(req)
    else:
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

      try:
        self.write(msg)
      except IOError:  # PIPE broken, most likely
        break

  def request_handler(self, req):
    self._messages.append(req)

  def reply_handler(self, rep):
    self._messages.append(rep)

  def event_handler(self, evt):
    self._messages.append(evt)


class CountPrinter(BasePrinter):
  """ use to accumulate up to N requests and then print a summary """
  def __init__(self, count, sort_by, loopback):
    super(CountPrinter, self).__init__(False, loopback)
    self.count, self.sort_by = count, sort_by
    self.seen = 0
    self.requests = defaultdict(int)

  def run(self):
    while self.seen < self.count:
      time.sleep(0.001)

    results = sorted(self.requests.items(), key=lambda item: item[1], reverse=True)
    for res in results:
       sys.stdout.write("%s %d\n" % res)
    sys.stdout.flush()

  def request_handler(self, req):
    self._add(req)

  def reply_handler(self, rep):
    """ we only care about requests & watches """
    pass

  def event_handler(self, evt):
    self._add(evt)

  def _add(self, msg):
    if self.seen >= self.count:
      return

    # eventually we should grab a lock here, but as of now
    # this is only called from a single thread.
    key = msg.path if self.sort_by == "path" else msg.name
    self.requests[key] += 1
    self.seen += 1


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

  if options.version:
    sys.stdout.write("%s\n" % __version__)
    sys.exit(0)

  config = SnifferConfig(options.iface)
  config.track_replies = True
  config.zookeeper_port = options.zookeeper_port
  config.max_queued_requests = options.max_queued_requests
  config.client_port = options.client_port if options.client_port != 0 else config.client_port

  if options.excluded_hosts and options.included_hosts:
    sys.stderr.write("The flags --include-host and --exclude-host can't be mixed.\n")
    sys.exit(1)

  if options.excluded_hosts:
    config.excluded_ips += expand_hosts(options.excluded_hosts)
  elif options.included_hosts:
    config.included_ips += expand_hosts(options.included_hosts)

  config.update_filter()

  if options.include_pings:
    config.include_pings()

  config.dump_bad_packet = options.dump_bad_packet

  loopback = options.iface in ["lo", "lo0"]

  if options.count_requests > 0:
    if options.sort_by not in ["path", "type"]:
      sys.stderr.write("Unknown value for --sort-by, use 'path' or 'type'.\n")
      sys.exit(1)

    p = CountPrinter(options.count_requests, options.sort_by, loopback)
  elif options.unpaired:
    p = UnpairedPrinter(options.colors, loopback)
  else:
    p = DefaultPrinter(options.colors, loopback)
  p.start()

  sniffer = Sniffer(
    config,
    p.request_handler,
    p.reply_handler,
    p.event_handler,
    error_to_stderr=True
  )
  sniffer.start()

  try:
    while p.isAlive():
      time.sleep(0.5)
  except (KeyboardInterrupt, SystemExit):
    pass

  # shutdown sniffer
  sniffer.stop()
  while sniffer.isAlive():
    time.sleep(0.001)

  try:
    sys.stdout.write("\033[0m")
    sys.stdout.flush()
  except IOError: pass


if __name__ == '__main__':
  setup()
  app.main()
