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


class TestablePerPathAccumulators(QueueStatsLoader):
  def __init__(self, aggregation_depth=0):
    self.processed_requests = 0
    super(TestablePerPathAccumulators, self).__init__()
    self.register_accumulator('0', PerPathStatsAccumulator(aggregation_depth))

  def cur_stats(self):
    return self._accumulators['0']._cur_stats

  def update_request_stats(self, request):
    self.processed_requests += 1
    self._accumulators['0'].update_request_stats(request)


NUMBER_OF_REQUESTS_SET_DATA = 30
NUMBER_OF_REQUESTS_WATCHES = 4
SLEEP_MAX = 2


def wait_for_stats(zkt, stats, pcap_file, expected_requests):
  consume_packets(pcap_file, zkt)
  slept = 0

  while stats.processed_requests < expected_requests:
    time.sleep(0.5)
    slept += 0.5
    if slept > SLEEP_MAX:
      break

  return stats.cur_stats()


def test_init_path_stats():
  stats = TestablePerPathAccumulators(aggregation_depth=1)
  stats.start()
  cur_stats = stats.cur_stats()
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
  zkt = Sniffer(SnifferConfig())
  zkt.add_request_handler(stats.handle_request)
  cur_stats = wait_for_stats(zkt, stats, "set_data", 1)

  #writes for / should stay 0
  assert cur_stats["writes"]["/"] == 0
  assert cur_stats["total"]["/writes"] == 20

  stats.stop()

def test_per_path_stats():
  stats = TestablePerPathAccumulators(aggregation_depth=1)
  stats.start()
  zkt = Sniffer(SnifferConfig())
  zkt.add_request_handler(stats.handle_request)

  cur_stats = wait_for_stats(zkt, stats, "set_data", NUMBER_OF_REQUESTS_SET_DATA)

  assert cur_stats["writes"]["/load-testing"] == 20
  assert cur_stats["SetDataRequest"]["/load-testing"] == 20

  stats.stop()


def test_per_path_stats_aggregated():
  stats = TestablePerPathAccumulators(aggregation_depth=2)
  stats.start()
  zkt = Sniffer(SnifferConfig())
  zkt.add_request_handler(stats.handle_request)

  cur_stats = wait_for_stats(zkt, stats, "set_data", NUMBER_OF_REQUESTS_SET_DATA)

  for i in range(0, 5):
    assert cur_stats["writes"]["/load-testing/%d" % (i)] == 4
    assert cur_stats["SetDataRequest"]["/load-testing/%d" % (i)] == 4

  assert cur_stats["total"]["/writes"] == 20
  assert cur_stats["total"]["/reads"] == 16

  stats.stop()


def test_watches():
  stats = TestablePerPathAccumulators(aggregation_depth=1)
  stats.start()
  zkt = Sniffer(SnifferConfig())
  zkt.add_request_handler(stats.handle_request)

  cur_stats = wait_for_stats(zkt, stats, "getdata_watches", NUMBER_OF_REQUESTS_WATCHES)

  assert cur_stats["watches"]["/test"] == 2
  assert cur_stats["GetDataRequest"]["/test"] == 2
  assert cur_stats["GetChildrenRequest"]["/test"] == 2

  stats.stop()
