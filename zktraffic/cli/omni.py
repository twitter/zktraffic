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

import sys
import time

from twitter.common import app
from twitter.common.log.options import LogOptions

from zktraffic import __version__
from zktraffic.cli.printer import Printer as Printer, DefaultPrinter as ZKDefaultPrinter
from zktraffic.base.sniffer import Sniffer as ZKSniffer, SnifferConfig as ZKSnifferConfig
from zktraffic.network.sniffer import Sniffer
import zktraffic.fle.message as FLE
import zktraffic.zab.quorum_packet as ZAB
from zktraffic.omni.omni_sniffer import OmniSniffer


def setup():
  LogOptions.set_stderr_log_level('NONE')

  app.add_option('--packet-filter', default='tcp', type=str,
                 help='pcap filter string. e.g. "tcp portrange 11221-32767" for JUnit tests')
  app.add_option('-c', '--colors', default=False, action='store_true')
  app.add_option('--dump-bad-packet', default=False, action='store_true')
  app.add_option('--include-pings', default=False, action='store_true',
                 help='Whether to include ZAB/ZK pings')
  app.add_option('--offline', default=None, type=str,
                 help='offline mode with a pcap file')
  app.add_option('--version', default=False, action='store_true')


def main(_, options):
  if options.version:
    sys.stdout.write("%s\n" % __version__)
    sys.exit(0)

  printer = Printer(options.colors,
                    output=sys.stdout,
                    skip_print=None if options.include_pings else lambda msg: isinstance(msg, ZAB.Ping))
  zk_printer = ZKDefaultPrinter(options.colors, loopback=False, output=sys.stdout)
  zk_printer.start()

  def fle_sniffer_factory(port):
    return Sniffer(None, port, FLE.Message, printer.add, options.dump_bad_packet, start=False)

  def zab_sniffer_factory(port):
    return Sniffer(None, port, ZAB.QuorumPacket, printer.add, options.dump_bad_packet, start=False)

  def zk_sniffer_factory(port):
    config = ZKSnifferConfig(None)
    config.track_replies = True
    config.zookeeper_port = port
    config.client_port = 0
    if options.include_pings:
      config.include_pings()
    return ZKSniffer(
      config,
      zk_printer.request_handler,
      zk_printer.reply_handler,
      zk_printer.event_handler,
      error_to_stderr=True
    )

  if not options.offline:
    sniffer = OmniSniffer(
      fle_sniffer_factory,
      zab_sniffer_factory,
      zk_sniffer_factory,
      pfilter=options.packet_filter,
      dump_bad_packet=options.dump_bad_packet)
  else:
    sniffer = OmniSniffer(
      fle_sniffer_factory,
      zab_sniffer_factory,
      zk_sniffer_factory,
      pfilter=options.packet_filter,
      dump_bad_packet=options.dump_bad_packet,
      start=False)
    sniffer.run(offline=options.offline)

  try:
    while (printer.isAlive() or zk_printer.isAlive()) and not options.offline:
      sniffer.join(1)
  except (KeyboardInterrupt, SystemExit):
    pass

  # consume all messages
  while not printer.empty or not zk_printer.empty:
    time.sleep(0.0001)

  # stop it
  printer.stop()
  zk_printer.stop()
  while not printer.stopped or not zk_printer.stopped:
    time.sleep(0.0001)

if __name__ == '__main__':
  setup()
  app.main()
