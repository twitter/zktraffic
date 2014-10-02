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


from zktraffic.base.zookeeper import OpCodes
from zktraffic.stats.accumulated_stats import AccumulatedStats, StatsConfig
from zktraffic.base.sniffer import Sniffer, SnifferConfig

from .common import consume_packets


def test_aggregation_depth():
  stats = AccumulatedStats(StatsConfig(aggregation_depth=1))
  zkt = Sniffer(SnifferConfig())
  zkt.add_request_handler(stats.handle_request)

  consume_packets('set_data', zkt)

  assert stats.global_stats.ops_written == 20
  assert stats.global_stats.by_op_counters[OpCodes.SETDATA] == 20

  # Did aggregation work?
  assert stats.by_path["/load-testing"].ops_written == 20
  assert stats.by_path["/load-testing"].by_op_counters[OpCodes.SETDATA] == 20
