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


'''
Contains various accumulators that build stats into dictionary objects driven
by input from the queued stats loader
'''
from collections import defaultdict

from twitter.common import log


class TopStatsAccumulator(object):
  def __init__(self, aggregation_depth):
    """
    if aggregation_depth > 0 then we aggregate for paths up to that depth
    as a safety measure set a cap on the num of requests, replies & events
    """

    self._prev_stats = {}
    self.init_cur_stats()
    self._aggregation_depth = aggregation_depth

  def update_request_stats(self, request):
    raise NotImplementedError

  def update_reply_stats(self, reply):
    raise NotImplementedError

  def update_event_stats(self, event):
    raise NotImplementedError

  def accumulate_stats(self):
    log.debug("Accumulating stats for accumulator %s ...", self.__class__.__name__)
    self._prev_stats = self._cur_stats
    self.init_cur_stats()

  def init_cur_stats(self):
    """Initialize the _cur_stats dictionary with defaults to avoid no data issues"""
    self._cur_stats = defaultdict(lambda: defaultdict(int))
    self._cur_stats["writes"]["/"] = 0
    self._cur_stats["writesBytes"]["/"] = 0
    self._cur_stats["reads"]["/"] = 0
    self._cur_stats["readsBytes"]["/"] = 0
    self._cur_stats["total"]["/writes"] = 0
    self._cur_stats["total"]["/writeBytes"] = 0
    self._cur_stats["total"]["/reads"] = 0
    self._cur_stats["total"]["/readBytes"] = 0

  def get_path(self, message, suffix=None):
    if self._aggregation_depth > 0:
      path = message.parent_path(self._aggregation_depth)
    else:
      path = message.path

    return intern(path if suffix is None else ':'.join((path, suffix)))

  def stats(self, top):
    top_stats = {}

    if top == 0:
      return self._prev_stats

    for op, per_path_s in self._prev_stats.items():
      paths = sorted(per_path_s.keys(), lambda a, b: per_path_s[b] - per_path_s[a])
      top_stats[op] = dict((p, per_path_s[p]) for p in paths[0:top])

    return top_stats


  def _update_request_stats(self, path, request):
    """ here we actually update the stats for a given request """
    log.debug("Request stats update : %s, %s", request.name, path)
    self._cur_stats[request.name][path] += 1
    self._cur_stats["%sBytes" % (request.name)][path] += request.size
    if request.is_write:
      self._cur_stats["writes"][path] += 1
      self._cur_stats["writesBytes"][path] += request.size
      self._cur_stats["total"]["/writes"] += 1
      self._cur_stats["total"]["/writeBytes"] += request.size
    else:
      self._cur_stats["reads"][path] += 1
      self._cur_stats["readsBytes"][path] += request.size
      self._cur_stats["total"]["/reads"] += 1
      self._cur_stats["total"]["/readBytes"] += request.size
      if request.watch:
        self._cur_stats["watches"][path] += 1


class PerPathStatsAccumulator(TopStatsAccumulator):
  def __init__(self, aggregation_depth):
    TopStatsAccumulator.__init__(self, aggregation_depth)

  def update_request_stats(self, request):
    self._update_request_stats(self.get_path(request), request)

  def update_reply_stats(self, reply):
    self._cur_stats[reply.name][reply.path] += 1

  def update_event_stats(self, event):
    self._cur_stats[event.name][event.path] += 1


class PerIPStatsAccumulator(TopStatsAccumulator):
  def update_request_stats(self, request):
    self._update_request_stats(self.get_path(request, request.ip), request)

  def update_reply_stats(self, reply):
    pass

  def update_event_stats(self, event):
    pass


class PerAuthStatsAccumulator(TopStatsAccumulator):
  def update_request_stats(self, request):
    self._update_request_stats(self.get_path(request, request.auth), request)

  def update_reply_stats(self, reply):
    pass

  def update_event_stats(self, event):
    pass
