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


import time

from zktraffic.base.sniffer import Sniffer, SnifferConfig
from zktraffic.stats.loaders import QueueStatsLoader
from zktraffic.stats.accumulators import PerPathStatsAccumulator

from .common import consume_packets


class TestablePerPathAccumulator(PerPathStatsAccumulator):
  def __init__(self, *args, **kwargs):
    super(TestablePerPathAccumulator, self).__init__(*args, **kwargs)
    self.processed_requests = 0
    self.processed_events = 0
    self.processed_replies = 0

  def update_request_stats(self, request):
    super(TestablePerPathAccumulator, self).update_request_stats(request)
    self.processed_requests += 1

  def update_reply_stats(self, reply):
    super(TestablePerPathAccumulator, self).update_reply_stats(reply)
    self.processed_replies += 1

  def update_event_stats(self, event):
    super(TestablePerPathAccumulator, self).update_event_stats(event)
    self.processed_events += 1


class TestableStatsLoader(object):
  def __init__(self, aggregation_depth, include_bytes=True):
    self._loader = QueueStatsLoader()
    self._accumulator = TestablePerPathAccumulator(aggregation_depth, include_bytes)
    self._loader.register_accumulator('0', self._accumulator)
    self._loader.start()

  @property
  def processed_requests(self):
    return self._accumulator.processed_requests

  @property
  def processed_events(self):
    return self._accumulator.processed_events

  def stop(self):
    self._loader.stop()

  @property
  def handle_request(self):
    return self._loader.handle_request

  @property
  def handle_reply(self):
    return self._loader.handle_reply

  @property
  def handle_event(self):
    return self._loader.handle_event

  @property
  def cur_stats(self):
    return self._accumulator._cur_stats


NUMBER_OF_REQUESTS_SET_DATA = 35
NUMBER_OF_REQUESTS_WATCHES = 5
SLEEP_MAX = 5.0


def wait_for_stats(zkt, pcap_file, loop_cond):
  consume_packets(pcap_file, zkt)
  slept = 0
  sleep_size = 0.001

  while loop_cond():
    time.sleep(sleep_size)
    slept += sleep_size
    if slept > SLEEP_MAX:
      break


def get_sniffer(request_handler, reply_handler=None, event_handler=None):
  config = SnifferConfig()
  config.track_replies = True

  sniffer = Sniffer(config)
  sniffer.add_request_handler(request_handler)
  if reply_handler:
    sniffer.add_reply_handler(reply_handler)
  if event_handler:
    sniffer.add_event_handler(event_handler)

  return sniffer


def test_init_path_stats():
  stats = TestableStatsLoader(aggregation_depth=1)
  cur_stats = stats.cur_stats
  assert "writes" in cur_stats
  assert "/" in cur_stats["writes"]
  assert "writesBytes" in cur_stats
  assert "/" in cur_stats["writesBytes"]
  assert "reads" in cur_stats
  assert "/" in cur_stats["reads"]
  assert "readsBytes" in cur_stats
  assert "/" in cur_stats["readsBytes"]
  assert "total" in cur_stats
  assert "/writes" in cur_stats["total"]
  assert "/writeBytes" in cur_stats["total"]
  assert "/reads" in cur_stats["total"]
  assert "/readBytes" in cur_stats["total"]

  #add some traffic
  sniffer = get_sniffer(stats.handle_request)
  wait_for_stats(sniffer, "set_data", lambda: stats.processed_requests < NUMBER_OF_REQUESTS_SET_DATA)
  cur_stats = stats.cur_stats

  #writes for / should stay 0
  assert cur_stats["writes"]["/"] == 0
  assert cur_stats["total"]["/writes"] == 20

  stats.stop()


def test_per_path_stats():
  stats = TestableStatsLoader(aggregation_depth=1)
  sniffer = get_sniffer(stats.handle_request)

  wait_for_stats(
    sniffer, "set_data", lambda: stats.processed_requests < NUMBER_OF_REQUESTS_SET_DATA)
  cur_stats = stats.cur_stats

  assert cur_stats["writes"]["/load-testing"] == 20
  assert cur_stats["SetDataRequest"]["/load-testing"] == 20

  stats.stop()


def test_per_path_stats_aggregated():
  stats = TestableStatsLoader(aggregation_depth=2)
  sniffer = get_sniffer(stats.handle_request)

  wait_for_stats(
    sniffer, "set_data", lambda: stats.processed_requests < NUMBER_OF_REQUESTS_SET_DATA)
  cur_stats = stats.cur_stats

  for i in range(0, 5):
    assert cur_stats["writes"]["/load-testing/%d" % (i)] == 4
    assert cur_stats["SetDataRequest"]["/load-testing/%d" % (i)] == 4

  assert cur_stats["total"]["/writes"] == 20
  assert cur_stats["total"]["/reads"] == 16

  stats.stop()


def test_setting_watches():
  stats = TestableStatsLoader(aggregation_depth=1)
  sniffer = get_sniffer(stats.handle_request)

  wait_for_stats(
    sniffer, "getdata_watches", lambda: stats.processed_requests < NUMBER_OF_REQUESTS_WATCHES)
  cur_stats = stats.cur_stats

  assert cur_stats["watches"]["/test"] == 2
  assert cur_stats["GetDataRequest"]["/test"] == 2
  assert cur_stats["GetChildrenRequest"]["/test"] == 2

  stats.stop()


def test_firing_watches():
  stats = TestableStatsLoader(aggregation_depth=2)

  sniffer = get_sniffer(stats.handle_request, stats.handle_reply, stats.handle_event)

  wait_for_stats(sniffer, "fire_watches", lambda: stats.processed_events < 1)
  cur_stats = stats.cur_stats

  assert cur_stats["NodeChildrenChanged"]["/in/portland"] == 1

  stats.stop()


def test_auth():
  stats = TestableStatsLoader(aggregation_depth=1)

  sniffer = get_sniffer(stats.handle_request, stats.handle_reply, stats.handle_event)

  wait_for_stats(sniffer, "auth", lambda: stats.processed_requests < 1)
  cur_stats = stats.cur_stats

  assert cur_stats["SetAuthRequest"]["/tacos:tacos"] == 1

  stats.stop()


def test_exclude_bytes():
  stats = TestableStatsLoader(aggregation_depth=1, include_bytes=False)
  sniffer = get_sniffer(stats.handle_request)

  wait_for_stats(
    sniffer, "set_data", lambda: stats.processed_requests < NUMBER_OF_REQUESTS_SET_DATA)
  cur_stats = stats.cur_stats

  # there shouldn't be any *Bytes key
  keys = cur_stats.keys()
  assert len(keys) > 0
  for key in keys:
    assert "Bytes" not in key

  stats.stop()
