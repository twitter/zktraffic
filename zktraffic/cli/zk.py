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

import socket
import sys
import time

from .printer import (
  CountPrinter,
  LatencyPrinter,
  UnpairedPrinter,
  DefaultPrinter
)

from zktraffic import __version__
from zktraffic.base.sniffer import Sniffer, SnifferConfig

from twitter.common.log.options import LogOptions


def setup():
  from twitter.common import app

  LogOptions.set_stderr_log_level('NONE')

  app.add_option('--iface', default='eth0', type=str, metavar='<iface>',
                 help='The interface to sniff on')
  app.add_option('--client-port', default=0, type=int, metavar='<client_port>',
                 help='The client port to filter by')
  app.add_option('--zookeeper-port', default=2181, type=int, metavar='<server_port>',
                 help='The ZooKeeper server port to filter by')
  app.add_option('--max-queued-requests', default=10000, type=int, metavar='<max>',
                 help='The maximum number of requests queued to be deserialized')
  app.add_option('--exclude-host',
                 dest='excluded_hosts',
                 metavar='<host>',
                 default=[],
                 action='append',
                 help='Host that should be excluded (you can use this multiple times)')
  app.add_option('--include-host',
                 dest='included_hosts',
                 metavar='<host>',
                 default=[],
                 action='append',
                 help='Host that should be included (you can use this multiple times)')
  app.add_option('--count-requests', default=0, type=int, metavar='<nreqs>',
                 help='Count N requests and report a summary (default: group by path)')
  app.add_option('--measure-latency', default=0, type=int, metavar='<nreqs>',
                 help='Measure latency of N pairs of requests and replies (default: group by path')
  app.add_option('--group-by', default='path', type=str, metavar='<group>',
                 help='Used with --count-requests or --measure-latency. Possible values: path, type or client')
  app.add_option('--sort-by', default='avg', type=str, metavar='<sort>',
                 help='Used with --measure-latency. Possible values: avg, p95 and p99')
  app.add_option("--aggregation-depth", default=0, type=int, metavar='<depth>',
                 help="Aggregate paths up to a certain depth. Used with --count-requests or --measure-latency")
  app.add_option('--unpaired', default=False, action='store_true',
                 help='Don\'t pair reqs/reps')
  app.add_option('-p', '--include-pings', default=False, action='store_true',
                 help='Whether to include ping requests and replies')
  app.add_option('-c', '--colors', default=False, action='store_true',
                 help='Color each client/server stream differently')
  app.add_option('--dump-bad-packet', default=False, action='store_true',
                 help='If unable to to deserialize a packet, print it out')
  app.add_option('--version', default=False, action='store_true')


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


def validate_group_by(group_by):
  if group_by not in ["path", "type", "client"]:
    sys.stderr.write("Unknown value for --group-by, use 'path', 'type' or 'client'.\n")
    sys.exit(1)


def validate_aggregation_depth(depth):
  if depth < 0:
    sys.stderr.write("Aggregation depth must be >= 0.\n")
    sys.exit(1)


def validate_sort_by(sort_by):
  if sort_by not in ["avg", "p95", "p99"]:
    sys.stderr.write("Unknown value for --sort-by, possible values are 'avg', 'p95' and 'p99'.\n")
    sys.exit(1)


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

  if options.count_requests > 0 and options.measure_latency > 0:
    sys.stderr.write("The flags --count-requests and --measure-latency can't be mixed.\n")
    sys.exit(1)

  if options.count_requests > 0:
    validate_group_by(options.group_by)
    validate_aggregation_depth(options.aggregation_depth)
    p = CountPrinter(options.count_requests, options.group_by, loopback, options.aggregation_depth)
  elif options.measure_latency > 0:
    validate_group_by(options.group_by)
    validate_aggregation_depth(options.aggregation_depth)
    validate_sort_by(options.sort_by)
    p = LatencyPrinter(
      options.measure_latency, options.group_by, loopback, options.aggregation_depth,
      options.sort_by)
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
    p.cancel()

  # shutdown sniffer
  sniffer.stop()
  while sniffer.isAlive():
    time.sleep(0.001)

  try:
    sys.stdout.write("\033[0m")
    sys.stdout.flush()
  except IOError: pass


if __name__ == '__main__':
  from twitter.common import app
  setup()
  app.main()
