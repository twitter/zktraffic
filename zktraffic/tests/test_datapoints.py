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


from zktraffic.stats.per_path_datapoints import PerPathDatapoints
from zktraffic.base.sniffer import Sniffer, SnifferConfig

from .common import consume_packets


def test_aggregation_depth():
  datapoints = PerPathDatapoints()

  zkt = Sniffer(SnifferConfig())
  zkt.add_request_handler(datapoints.handle_request)

  consume_packets('set_data', zkt)

  d = datapoints.sum_minute()

  assert d['global']['ops_written'] == 20
  assert d['global']['by_op_counters']['SetDataRequest'] == 20

  for i in range(0, 4):
    assert d['paths']['/load-testing/%d' % (i)]['ops_written'] == 4
    assert d['paths']['/load-testing/%d' % (i)]['by_op_counters']['SetDataRequest'] == 4
