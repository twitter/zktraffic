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
from zktraffic.stats.accumulators import PerPathStatsAccumulator
from zktraffic.base.sniffer import Sniffer, SnifferConfig

from .common import consume_packets


def default_sniffer(aggregation_depth=1):
  stats = PerPathStatsAccumulator(aggregation_depth=aggregation_depth)
  sniffer = Sniffer(SnifferConfig())
  sniffer.add_request_handler(stats.update_request_stats)
  return (sniffer, stats)


# TODO(rgs): assert amount of bytes in writes/reads/ops
def test_packets_set_data():
  sniffer, stats = default_sniffer(aggregation_depth=2)

  consume_packets('set_data', sniffer)

  assert stats._cur_stats["total"]["/writes"] == 20
  assert stats._cur_stats["total"]["/reads"] == 16

  # Now check that each path has the right stats...
  for i in range(0, 5):
    assert stats._cur_stats["SetDataRequest"]["/load-testing/%d" % i] == 4


def test_packets_create_delete():
  sniffer, stats = default_sniffer(aggregation_depth=2)

  consume_packets('create', sniffer)

  assert stats._cur_stats["total"]["/writes"] == 45

  # Now check that each path has the right stats...
  for i in range(0, 5):
    assert stats._cur_stats["DeleteRequest"]["/load-testing/%d" % i] == 4
    assert stats._cur_stats["CreateRequest"]["/load-testing/%d" % i] == 5


# py-zookeeper (so the C library) doesn't add the request length when issuing Creates so lets
# exercise that special parsing case
def test_create_znode_pyzookeeper():
  sniffer, stats = default_sniffer(aggregation_depth=2)
  consume_packets('create-pyzookeeper', sniffer)
  assert stats._cur_stats["CreateRequest"]["/git/twitter-config_sha"] == 1


def test_watches():
  sniffer, stats = default_sniffer()
  consume_packets('getdata_watches', sniffer)

  assert stats._cur_stats["GetDataRequest"]["/test"] == 2
  assert stats._cur_stats["GetChildrenRequest"]["/test"] == 2
  assert stats._cur_stats["watches"]["/test"] == 2


def test_connects():
  sniffer, stats = default_sniffer()
  consume_packets('connects', sniffer)

  assert stats._cur_stats["ConnectRequest"][""] == 3
  assert stats._cur_stats["CloseRequest"][""] == 3


def test_multi():
  sniffer, stats = default_sniffer(aggregation_depth=1)
  consume_packets('multi', sniffer)

  assert stats._cur_stats["MultiRequest"]["/foo"] == 1


def test_auth():
  sniffer, stats = default_sniffer()
  consume_packets('auth', sniffer)

  assert stats._cur_stats["SetAuthRequest"]["/tacos:tacos"] == 1


def test_reconfig():
  sniffer, stats = default_sniffer()
  consume_packets('reconfig', sniffer)

  assert stats._cur_stats["ReconfigRequest"][""] == 1
