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

try:
  from StringIO import StringIO
except ImportError:
  from io import StringIO

import time

from zktraffic.base.sniffer import Sniffer, SnifferConfig
from zktraffic.cli.printer import (
  CountPrinter,
  DefaultPrinter,
  LatencyPrinter,
  UnpairedPrinter
)

from .common import consume_packets


def get_sniffer(printer, with_pings=True):
  config = SnifferConfig()
  config.track_replies = True

  if with_pings:
    config.include_pings()

  sniffer = Sniffer(config)
  sniffer.add_request_handler(printer.request_handler)
  sniffer.add_reply_handler(printer.reply_handler)
  sniffer.add_event_handler(printer.event_handler)

  return sniffer


def _test(printer_cls):
  output = StringIO()

  printer = printer_cls(colors=False, loopback=True, output=output)
  printer.start()

  sniffer = get_sniffer(printer)
  consume_packets('dump', sniffer)

  # wait for expected requests, replies & events
  expected = [13, 13, 1]
  while [printer.seen_requests, printer.seen_replies, printer.seen_events] != expected:  # pragma: no cover
    time.sleep(0.001)

  # wait for queues to be empty
  while not printer.empty:  # pragma: no cover
    time.sleep(0.001)

  # stop the printer
  printer.stop()
  while not printer.stopped:  # pragma: no cover
    time.sleep(0.001)

  assert "PingRequest(client=127.0.0.1:60446)" in output.getvalue()
  assert "ExistsRequest(xid=6, path=/dknightly" in output.getvalue()
  assert "CreateRequest(size=62, xid=7, path=/dknightly," in output.getvalue()
  assert "SetDataRequest(xid=15, path=/dknightly," in output.getvalue()
  assert "EventNodeDataChanged(state=3, path=/dknightly," in output.getvalue()


def test_default_printer():
  _test(DefaultPrinter)


def test_default_printer_close_reqs():
  output = StringIO()
  printer = UnpairedPrinter(colors=False, loopback=True, output=output)
  printer.start()

  sniffer = get_sniffer(printer, with_pings=False)
  consume_packets('connects', sniffer)  # no connect replies in this pcap file

  # 3 connect requests + 3 close requests
  expected = [6, 0, 0]
  while [printer.seen_requests, printer.seen_replies, printer.seen_events] != expected:  # pragma: no cover
    time.sleep(0.001)

  # wait for queues to be empty
  while not printer.empty:  # pragma: no cover
    time.sleep(0.001)

  # stop the printer
  printer.stop()
  while not printer.stopped:  # pragma: no cover
    time.sleep(0.001)

  assert "ConnectRequest" in output.getvalue()
  assert "CloseRequest" in output.getvalue()


def test_unpaired_printer():
  _test(UnpairedPrinter)


def test_count_printer():
  output = StringIO()

  printer = CountPrinter(
    count=14,             # requests + events, replies are not counted
    group_by='type',
    loopback=True,
    aggregation_depth=0,
    output=output
  )
  printer.start()

  sniffer = get_sniffer(printer)
  consume_packets('dump', sniffer)

  while not printer.stopped:  # pragma: no cover
    time.sleep(0.001)

  assert "ExistsRequest 5" in output.getvalue()
  assert "PingRequest 3" in output.getvalue()
  assert "GetChildrenRequest 2" in output.getvalue()
  assert "GetDataRequest 1" in output.getvalue()
  assert "CreateRequest 1" in output.getvalue()
  assert "NodeDataChanged 1" in output.getvalue()
  assert "SetDataRequest 1" in output.getvalue()


def test_count_printer_group_by_client():
  output = StringIO()

  printer = CountPrinter(
    count=14,             # requests + events, replies are not counted
    group_by='client',
    loopback=True,
    aggregation_depth=0,
    output=output
  )
  printer.start()

  sniffer = get_sniffer(printer)
  consume_packets('dump', sniffer)

  while not printer.stopped:  # pragma: no cover
    time.sleep(0.001)

  assert "127.0.0.1:60446 14" in output.getvalue()


def test_latency_printer():
  output = StringIO()

  printer = LatencyPrinter(
    count=10,             # 5*exists + 1*get_data + 1*create + 2*get_children + 1*set_data
    group_by='type',
    loopback=True,
    aggregation_depth=0,
    sort_by='avg',
    output=output,
    include_pings=False  # accounting for pings has ordering issues, so ignore
  )
  printer.start()

  sniffer = get_sniffer(printer)
  consume_packets('dump', sniffer)

  while not printer.stopped:  # pragma: no cover
    time.sleep(0.001)

  # this is pretty primitive matching, since we are not mocking the timestamps
  assert "GetDataRequest" in output.getvalue()
  assert "CreateRequest" in output.getvalue()
  assert "ExistsRequest" in output.getvalue()
  assert "GetChildrenRequest" in output.getvalue()
  assert "SetDataRequest" in output.getvalue()
