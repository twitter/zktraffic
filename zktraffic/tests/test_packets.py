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


def default_sniffer():
  stats = AccumulatedStats(StatsConfig())
  sniffer = Sniffer(SnifferConfig())
  sniffer.add_request_handler(stats.handle_request)
  return (sniffer, stats)


# TODO(rgs): assert amount of bytes in writes/reads/ops
def test_packets_set_data():
  sniffer, stats = default_sniffer()

  consume_packets('set_data', sniffer)

  assert stats.global_stats.ops_written == 20
  assert stats.global_stats.by_op_counters[OpCodes.SETDATA] == 20

  # Now check that each path has the right stats...
  for i in range(0, 5):
    assert stats.by_path["/load-testing/%d" % (i)].ops_written == 4
    assert stats.by_path["/load-testing/%d" % (i)].by_op_counters[OpCodes.SETDATA] == 4


def test_packets_create_delete():
  sniffer, stats = default_sniffer()

  consume_packets('create', sniffer)

  assert stats.global_stats.ops_written == 45
  assert stats.global_stats.by_op_counters[OpCodes.DELETE] == 20
  assert stats.global_stats.by_op_counters[OpCodes.CREATE] == 25

  # Now check that each path has the right stats...
  for i in range(0, 5):
    assert stats.by_path["/load-testing/%d" % (i)].ops_written == 9
    assert stats.by_path["/load-testing/%d" % (i)].by_op_counters[OpCodes.DELETE] == 4
    assert stats.by_path["/load-testing/%d" % (i)].by_op_counters[OpCodes.CREATE] == 5


# py-zookeeper (so the C library) doesn't add the request length when issuing Creates so lets
# exercise that special parsing case
def test_create_znode_pyzookeeper():
  sniffer, stats = default_sniffer()
  consume_packets('create-pyzookeeper', sniffer)

  assert stats.by_path["/git/twitter-config_sha"].ops_written == 1
  assert stats.by_path["/git/twitter-config_sha"].by_op_counters[OpCodes.CREATE] == 1
  assert stats.by_path["/git/twitter-config_sha"].bytes_written == 60


def test_watches():
  sniffer, stats = default_sniffer()
  consume_packets('getdata_watches', sniffer)

  assert stats.global_stats.by_op_counters[OpCodes.GETDATA] == 2
  assert stats.global_stats.by_op_counters[OpCodes.GETCHILDREN] == 2
  assert stats.global_stats.watches == 2


def test_connects():
  sniffer, stats = default_sniffer()
  consume_packets('connects', sniffer)

  assert stats.global_stats.by_op_counters[OpCodes.CONNECT] == 3
  assert stats.global_stats.by_op_counters[OpCodes.CLOSE] == 3


def test_multi():
  sniffer, stats = default_sniffer()
  consume_packets('multi', sniffer)

  assert stats.global_stats.by_op_counters[OpCodes.MULTI] == 1
  assert stats.by_path["/foo"].ops_written == 1


def test_auth():
  sniffer, stats = default_sniffer()
  consume_packets('auth', sniffer)

  assert stats.global_stats.by_op_counters[OpCodes.SETAUTH] == 1


def test_reconfig():
  sniffer, stats = default_sniffer()
  consume_packets('reconfig', sniffer)

  print str(stats.global_stats.by_op_counters)
  assert stats.global_stats.by_op_counters[OpCodes.RECONFIG] == 1
