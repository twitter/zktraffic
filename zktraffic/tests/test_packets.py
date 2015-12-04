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


from zktraffic.base.client_message import (
  ConnectRequest,
  MultiRequest,
  ReconfigRequest,
  SetWatchesRequest
)
from zktraffic.base.server_message import (
  ConnectReply,
  MultiReply,
  ReconfigReply,
)
from zktraffic.base.sniffer import Sniffer, SnifferConfig
from zktraffic.stats.accumulators import PerPathStatsAccumulator

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

  assert stats._cur_stats["ConnectRequest"]["/"] == 3
  assert stats._cur_stats["CloseRequest"]["/"] == 3


def test_connect_replies():
  _test_requests_replies('connect_replies', ConnectRequest, ConnectReply, nreqs=3, nreps=3)


def test_multi():
  _test_requests_replies('multi', MultiRequest, MultiReply, nreqs=1, nreps=1)


def test_auth():
  sniffer, stats = default_sniffer()
  consume_packets('auth', sniffer)

  assert stats._cur_stats["SetAuthRequest"]["/tacos:tacos"] == 1


def test_reconfig():
  _test_requests_replies('reconfig', ReconfigRequest, ReconfigReply, nreqs=1, nreps=1)


def test_setwatches():
  requests = []

  def handler(request):
    if isinstance(request, SetWatchesRequest):
      requests.append(request)

  sniffer = Sniffer(SnifferConfig())
  sniffer.add_request_handler(handler)
  consume_packets('setwatches', sniffer)

  assert len(requests) == 1

  req = requests[0]

  assert len(req.child) == 5
  assert "/foo" in req.child
  assert "/in" in req.child
  assert "/zookeeper" in req.child
  assert "/in/portland" in req.child
  assert "/" in req.child


def _test_requests_replies(pcap_name, request_cls, reply_cls, nreqs, nreps):
  requests = []
  replies = []

  def handler(msg):
    if isinstance(msg, request_cls):
      requests.append(msg)
    elif isinstance(msg, reply_cls):
      replies.append(msg)

  config = SnifferConfig()
  config.track_replies = True
  sniffer = Sniffer(config)
  sniffer.add_request_handler(handler)
  sniffer.add_reply_handler(handler)

  consume_packets(pcap_name, sniffer)

  assert len(requests) == nreqs
  assert len(replies) == nreps

  sniffer.stop()

def test_four_letter():
  sniffer, stats = default_sniffer()

  # stat(4l), ConnectRequest, GetChildrenRequest, and conf(4l)
  consume_packets('get_children_with_four_letter', sniffer)

  assert stats._cur_stats["ConnectRequest"]["/"] == 1
  assert stats._cur_stats["GetChildrenRequest"]["/"] == 1
