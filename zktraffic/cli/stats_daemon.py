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

import signal
import socket
import sys
import time

from zktraffic import __version__
from zktraffic.endpoints.stats_server import StatsServer
from zktraffic.base.process import ProcessOptions

from twitter.common import app, log
from twitter.common.http import HttpServer
from twitter.common.http.diagnostics import DiagnosticsEndpoints


def setup():
  app.add_option("--iface",
                 dest="iface",
                 metavar="IFACE",
                 default="eth0",
                 help="interface to capture packets from")
  app.add_option("--http-port",
                 dest="http_port",
                 metavar="HTTPPORT",
                 type=int,
                 default=7070,
                 help="listen port for http endpoints")
  app.add_option("--http-address",
                 dest="http_addr",
                 metavar="HTTPADDR",
                 type=str,
                 default=socket.gethostname(),
                 help="listen address for http endpoints")
  app.add_option("--zookeeper-port",
                 type=int,
                 default=2181,
                 help="ZK's client port (from which to sniff)")
  app.add_option("--aggregation-depth",
                 dest="aggregation_depth",
                 type=int,
                 default=0,
                 help="aggregate paths up to a certain depth")
  app.add_option("--max-results",
                 dest="max_results",
                 type=int,
                 default=10,
                 help="top number of results to be exported")
  app.add_option("--refresh-time",
                 dest="refresh_time",
                 type=int,
                 default=0,
                 help="refresh time in the generated html")
  app.add_option("--niceness",
                 dest="niceness",
                 type=int,
                 default=0,
                 help="set the niceness")
  app.add_option("--set-cpu-affinity",
                 dest="cpu_affinity",
                 metavar="CPU#[,CPU#]",
                 type=str,
                 default=None,
                 help="A comma-separated list of CPU cores to pin this process to")
  app.add_option("--sampling",
                 type=float,
                 default=1.0,
                 help="Percentage of packets to inspect [0, 1]")
  app.add_option("--max-queued-requests",
                 type=int,
                 default=400000,
                 help="max queued requests")
  app.add_option("--max-queued-replies",
                 type=int,
                 default=400000,
                 help="max queued replies")
  app.add_option("--max-queued-events",
                 type=int,
                 default=400000,
                 help="max queued events")
  app.add_option("--exclude-bytes", default=False, action='store_true',
                 help="Exclude stats for bytes per path and request type")
  app.add_option('--version', default=False, action='store_true')


class Server(HttpServer):
  pass


def main(_, opts):

  if opts.version:
    sys.stdout.write("%s\n" % __version__)
    sys.exit(0)

  # set proc options before we spawn threads
  process = ProcessOptions()

  if opts.niceness >= 0:
    process.set_niceness(opts.niceness)

  if opts.cpu_affinity:
    process.set_cpu_affinity(opts.cpu_affinity)

  if opts.sampling < 0 or opts.sampling > 1:
    sys.stdout.write("--sampling takes values within [0, 1]\n")
    sys.exit(1)

  stats = StatsServer(opts.iface,
                      opts.zookeeper_port,
                      opts.aggregation_depth,
                      opts.max_results,
                      opts.max_queued_requests,
                      opts.max_queued_replies,
                      opts.max_queued_events,
                      sampling=opts.sampling,
                      include_bytes=not opts.exclude_bytes)

  log.info("Starting with opts: %s" % (opts))

  signal.signal(signal.SIGINT, signal.SIG_DFL)

  server = Server()
  server.mount_routes(DiagnosticsEndpoints())
  server.mount_routes(stats)
  server.run(opts.http_addr, opts.http_port)

  stats.sniffer.join()


if __name__ == '__main__':
  setup()
  app.main()
