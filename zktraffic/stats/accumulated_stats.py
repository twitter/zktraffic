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


from __future__ import print_function

from collections import defaultdict
import socket
import sys
from threading import Lock

from zktraffic.base.zookeeper import OpCodes, req_type_to_str

from .stats import Counters, sizeof_fmt


class OpsCounters(object):
  def __init__(self, label):
    self.label = label
    self.by_op_counters = defaultdict(int)
    self.by_op_bytes_counters = defaultdict(int)
    self.ops_written = 0
    self.ops_read = 0
    self.bytes_written = 0
    self.bytes_read = 0
    self.watches = 0

  def inc(self, request):
    if request.is_write:
      self.bytes_written += request.size
      self.ops_written += 1
    else:
      self.bytes_read += request.size
      self.ops_read += 1
      if request.watch:
        self.watches += 1

    self.by_op_counters[request.opcode] += 1
    self.by_op_bytes_counters[request.opcode] += request.size

  def dict(self):
    d = {
      "ops_written": self.ops_written,
      "ops_read": self.ops_read,
      "bytes_written": self.bytes_written,
      "bytes_read": self.bytes_read,
      "by_op_counters": {},
      "by_op_bytes_counters": {},
    }

    for op, value in self.by_op_counters.items():
      d["by_op_counters"][req_type_to_str(op)] = value

    for op, value in self.by_op_bytes_counters.items():
      d["by_op_bytes_counters"][req_type_to_str(op)] = value

    return d

  def as_str(self, filters=[Counters.ALL]):
    if Counters.ALL in filters:
      s = """
%s
bytes written: %s, read: %s, %s, %s, %s, %s, %s, %s, %s
operations written: %d, read %d, %s, %s, %s, %s, %s, %s, %s
""" % (self.label, sizeof_fmt(self.bytes_written), sizeof_fmt(self.bytes_read),
       self._op_bytes_str(OpCodes.CREATE),
       self._op_bytes_str(OpCodes.SETDATA),
       self._op_bytes_str(OpCodes.GETDATA),
       self._op_bytes_str(OpCodes.DELETE),
       self._op_bytes_str(OpCodes.GETCHILDREN),
       self._op_bytes_str(OpCodes.GETCHILDREN2),
       self._op_bytes_str(OpCodes.EXISTS),
       self.ops_written, self.ops_read, self._op_str(OpCodes.CREATE),
       self._op_str(OpCodes.SETDATA),
       self._op_str(OpCodes.GETDATA),
       self._op_str(OpCodes.DELETE),
       self._op_str(OpCodes.GETCHILDREN),
       self._op_str(OpCodes.GETCHILDREN2),
       self._op_str(OpCodes.EXISTS))

    else:
      first = True
      s = "%s\n " % (self.label)
      for f in filters:
        if not first:
          s += ", "

        if f == Counters.WRITES:
          s += "bytes written: %s" % (sizeof_fmt(self.bytes_written))
        elif f == Counters.READS:
          s += "bytes read: %s" % (sizeof_fmt(self.bytes_read))
        elif f == Counters.CREATE:
          s += self._op_str(OpCodes.CREATE)
        elif f == Counters.SET_DATA:
          s += self._op_str(OpCodes.SETDATA)
        elif f == Counters.GET_DATA:
          s += self._op_str(OpCodes.GETDATA)
        elif f == Counters.DELETE:
          s += self._op_str(OpCodes.DELETE)
        elif f == Counters.GET_CHILDREN:
          s += self._op_str(OpCodes.GETCHILDREN)
          s += ", "
          s += self._op_str(OpCodes.GETCHILDREN2)
        elif f == Counters.EXISTS:
          s += self._op_str(OpCodes.EXISTS)
        elif f == Counters.CREATE_BYTES:
          s += self._op_bytes_str(OpCodes.CREATE)
        elif f == Counters.SET_DATA_BYTES:
          s += self._op_bytes_str(OpCodes.SETDATA)
        elif f == Counters.GET_DATA_BYTES:
          s += self._op_bytes_str(OpCodes.GETDATA)
        elif f == Counters.DELETE_BYTES:
          s += self._op_bytes_str(OpCodes.DELETE)
        elif f == Counters.GET_CHILDREN_BYTES:
          s += self._op_bytes_str(OpCodes.GETCHILDREN)
          s += ", "
          s += self._op_bytes_str(OpCodes.GETCHILDREN2)
        elif f == Counters.EXISTS_BYTES:
          s += self._op_bytes_str(OpCodes.EXISTS)

        first = False

    return s + "\n"

  def _op_str(self, op_num):
    return "%s: %d" % (req_type_to_str(op_num), self.by_op_counters.get(op_num, 0))

  def _op_bytes_str(self, op_num):
    return "%s bytes: %s" % (req_type_to_str(op_num),
                             sizeof_fmt(self.by_op_bytes_counters.get(op_num, 0)))


class CountersDict(dict):
  def __missing__(self, key):
    value = self[key] = OpsCounters(key)
    return value


class StatsConfig(object):
  def __init__(self, aggregation_depth=0):
    """ aggregate paths upto this (/a/b for 2) """
    self.aggregation_depth = aggregation_depth

  def __str__(self):
    return """
***zks config ***
aggregation_depth = %d
""" % (self.aggregation_depth)


class AccumulatedStats(object):
  VIEW_BY_PATH = 1
  VIEW_BY_IP = 2

  @classmethod
  def view_to_str(cls, view):
    return "by-path" if view == cls.VIEW_BY_PATH else "by-ip"

  SORT_BY = {
    Counters.WRITES: lambda c: c.bytes_written,
    Counters.READS: lambda c: c.bytes_read,
    Counters.CREATE: lambda c: c.by_op_counters[OpCodes.CREATE],
    Counters.SET_DATA: lambda c: c.by_op_counters[OpCodes.SETDATA],
    Counters.GET_DATA: lambda c: c.by_op_counters[OpCodes.GETDATA],
    Counters.DELETE: lambda c: c.by_op_counters[OpCodes.DELETE],
    Counters.GET_CHILDREN: lambda c: c.by_op_counters[OpCodes.GETCHILDREN],
    Counters.CREATE_BYTES: lambda c: c.by_op_bytes_counters[OpCodes.CREATE],
    Counters.SET_DATA_BYTES: lambda c: c.by_op_bytes_counters[OpCodes.SETDATA],
    Counters.GET_DATA_BYTES: lambda c: c.by_op_bytes_counters[OpCodes.GETDATA],
    Counters.DELETE_BYTES: lambda c: c.by_op_bytes_counters[OpCodes.DELETE],
    Counters.GET_CHILDREN_BYTES: lambda c: c.by_op_bytes_counters[OpCodes.GETCHILDREN],
  }

  def __init__(self, stats_config):
    self.stats_config = stats_config
    self._hostname = socket.gethostname()
    self._lock = Lock()
    self.reset()

  def handle_request(self, request):
    with self._lock:
      self.global_stats.inc(request)
      d = self.stats_config.aggregation_depth
      path = request.path if d == 0 else request.parent_path(d)
      self.by_path[path].inc(request)
      self.by_ip[request.ip].inc(request)

  def reset(self):
    with self._lock:
      self.global_stats = OpsCounters("global stats for %s" % self._hostname)
      self.by_path = CountersDict()
      self.by_ip = CountersDict()

  def dump(self, top=10, order_by=Counters.WRITES, display_filters=[Counters.ALL],
           view=VIEW_BY_PATH):
    data_view = self.by_path if view == self.VIEW_BY_PATH else self.by_ip
    with self._lock:
      print(self.global_stats.as_str(display_filters), "\n", file=sys.stdout)
      results = sorted(data_view.values(),
                       key=self.SORT_BY[order_by],
                       reverse=True)
      for ops_counter in results[0:top]:
        print(ops_counter.as_str(display_filters), file=sys.stdout)

  def dict(self, top=10, order_by=Counters.WRITES, display_filters=[Counters.ALL],
           view=VIEW_BY_PATH):
    d = {}
    data_view = self.by_path if view == self.VIEW_BY_PATH else self.by_ip

    with self._lock:
      d["global"] = self.global_stats.dict()
      results = sorted(data_view.values(), key=self.SORT_BY[order_by], reverse=True)
      d["paths"] = dict((ops_counter.label, ops_counter.dict()) for ops_counter in results[0:top])

    return d
