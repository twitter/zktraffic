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


import sys
from threading import Lock, Thread
import time

from .accumulated_stats import AccumulatedStats, StatsConfig
from .stats import Counters, sizeof_fmt

from twitter.common.collections import OrderedDict


NUMBER_OF_DATAPOINTS = 60


class PathDatapoints(dict):
  def __missing__(self, path):
    value = self[path] = [(i, 0) for i in range(0, NUMBER_OF_DATAPOINTS)]
    return value


class PerPathDatapoints(Thread):
  PURGE_SLEEP_TIME = 2  # sleep time between purging old datapoints
  DEFAULT_TOP_RESULTS = 10  # number of (top) results to show by default

  def __init__(self, older_than=120, aggregation_depth=0):
    """
    datapoints that are `older_than` will be dropped
    if aggregation_depth > 0 then we aggregate for paths up to that depth
    """
    self._older_than = older_than
    self._aggregation_depth = aggregation_depth
    self._requests_by_timestamp = OrderedDict()
    self._lock = Lock()

    super(PerPathDatapoints, self).__init__()

  def size(self):
    size = {"samples": 0, "requests_mem_usage": 0}
    with self._lock:
      samples, mem_usage = 0, 0
      for reqs in self._requests_by_timestamp.values():
        samples += len(reqs)
        mem_usage += sum(sys.getsizeof(r) for r in reqs)

    size["samples"] = samples
    size["requests_mem_usage"] = mem_usage
    size["requests_mem_usage"] = sizeof_fmt(size["requests_mem_usage"])
    size["ordered_dict_mem_usage"] = sizeof_fmt(sys.getsizeof(self._requests_by_timestamp))

    return size

  def run(self):
    """ drop samples that are too old """
    while True:
      time.sleep(self.PURGE_SLEEP_TIME)
      old_tstamp = time.time() - self._older_than
      with self._lock:
        for tstamp in self._requests_by_timestamp.keys():
          if tstamp < old_tstamp:
            del self._requests_by_timestamp[tstamp]

  def handle_request(self, request):
    if self._aggregation_depth > 0:
      request.path = intern(request.parent_path(self._aggregation_depth))

    with self._lock:
      tstamp = int(time.time())
      if tstamp not in self._requests_by_timestamp:
        self._requests_by_timestamp[tstamp] = []
      self._requests_by_timestamp[tstamp].append(request)

  def sum_minute(self, top=DEFAULT_TOP_RESULTS, order_by=Counters.WRITES,
                 display=[Counters.ALL], view=AccumulatedStats.VIEW_BY_PATH):
    now = int(time.time())
    old = now - NUMBER_OF_DATAPOINTS
    stats = AccumulatedStats(StatsConfig())

    with self._lock:
      # note that this is an OrderedDict so samples are in chronological order
      for tstamp in self._requests_by_timestamp.keys():
        if tstamp < old:
          continue

        if tstamp > now:
          break

        for r in self._requests_by_timestamp[tstamp]:
          stats.handle_request(r)

    return stats.dict(top=top,
                      order_by=order_by,
                      display_filters=display,
                      view=view)

  def datapoints_writes(self):
    return self._filter_datapoints(condition=lambda req: req.is_write)

  def datapoints_reads(self):
    return self._filter_datapoints(condition=lambda req: not req.is_write)

  def datapoints_for_op(self, op):
    return self._filter_datapoints(condition=lambda req: req.opcode == op)

  def datapoints_by_path_for_op(self, op, top):
    """ op is "writes" or "reads" or one of OpCodes.CREATE, OpCodes.SETDATA, etc.
        because why use Python if you can't abuse types?
        top is the number of results
    """
    if op == "writes":
      return self._datapoints_by_path_for_op_impl(lambda r: r.is_write, top)
    elif op == "reads":
      return self._datapoints_by_path_for_op_impl(lambda r: not r.is_write, top)
    else:
      return self._datapoints_by_path_for_op_impl(lambda r: r.opcode == op, top)

  def _datapoints_by_path_for_op_impl(self, request_filter, top):
    """ to make this moderately efficient we use a dict that
    provides a pre-populated list of datapoints.
    """
    tstamp = int(time.time()) - NUMBER_OF_DATAPOINTS
    datapoints = PathDatapoints()
    with self._lock:
      for i in range(0, NUMBER_OF_DATAPOINTS):
        if tstamp in self._requests_by_timestamp:
          for req in self._requests_by_timestamp[tstamp]:
            if request_filter(req):
              dp = datapoints[req.path][i][1] + 1
              datapoints[req.path][i] = (i, dp)
        tstamp += 1

    # sort
    def comparator(path_a, path_b):
      sum_a = sum(d[1] for d in datapoints[path_a])
      sum_b = sum(d[1] for d in datapoints[path_b])
      return sum_b - sum_a
    paths = sorted(datapoints.keys(), comparator)

    if len(paths) == 0:
      return [("/", datapoints["/"])]

    return [(p, datapoints[p]) for p in paths[0:top]]

  def _filter_datapoints(self, condition):
    tstamp = int(time.time()) - NUMBER_OF_DATAPOINTS
    datapoints = []
    for i in range(0, NUMBER_OF_DATAPOINTS):
      aggregate = sum(bool(condition(req)) for req in self._requests_by_timestamp.get(tstamp, []))
      datapoints.append((i, aggregate))
      tstamp += 1

    return datapoints
