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


import multiprocessing

from zktraffic.stats.loaders import QueueStatsLoader
from zktraffic.stats.accumulators import (
  PerAuthStatsAccumulator,
  PerIPStatsAccumulator,
  PerPathStatsAccumulator,
)

from .endpoints_server import EndpointsServer

from twitter.common.http import HttpServer


class StatsServer(EndpointsServer):
  def __init__(self,
               iface,
               zkport,
               aggregation_depth,
               max_results=EndpointsServer.MAX_RESULTS,
               max_reqs=400000,
               max_reps=400000,
               max_events=400000):

    # Forcing a load of the multiprocessing module here
    # seem to be hitting http://bugs.python.org/issue8200
    multiprocessing.current_process().name

    self._max_results = max_results

    self._stats = QueueStatsLoader(max_reqs, max_reps, max_events)

    self._stats.register_accumulator('per_path', PerPathStatsAccumulator(aggregation_depth))
    self._stats.register_accumulator('per_ip', PerIPStatsAccumulator(aggregation_depth))
    self._stats.register_accumulator('per_auth', PerAuthStatsAccumulator(aggregation_depth))

    self._stats.start()

    super(StatsServer, self).__init__(
      iface, zkport, self._stats.handle_request, self._stats.handle_reply, self._stats.handle_event)

  def _get_stats(self, name, prefix=''):
    stats_by_opname = self._stats.stats(name, self._max_results)

    stats = {}
    for opname, opstats in stats_by_opname.items():
      for path, value in opstats.items():
        stats["%s%s%s" % (prefix, opname, path)] = value

    return stats

  @HttpServer.route("/json/paths")
  def json_paths(self):
    return self._get_stats('per_path')

  @HttpServer.route("/json/ips")
  def json_ips(self):
    return self._get_stats('per_ip', 'per_ip/')

  @HttpServer.route("/json/auths")
  def json_auths(self):
    return self._get_stats('per_auth', 'per_auth/')

  @HttpServer.route("/json/auths-dump")
  def json_auths_dump(self):
    return self._stats.auth_by_client
