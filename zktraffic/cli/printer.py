# -*- coding: utf-8 -*-

# ==================================================================================================
# Copyright 2015 Twitter, Inc.
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

from collections import defaultdict, deque
from datetime import datetime
from threading import Thread

import sys
import time

from zktraffic.stats.util import percentile

from tabulate import tabulate

import colors


NUM_COLORS = len(colors.COLORS)


class Printer(Thread):
  """ simple printer thread to use with FLE & ZAB messages """
  def __init__(self, colors, output=sys.stdout):
    super(Printer, self).__init__()
    self.setDaemon(True)
    self._queue = deque()
    self._print = self._print_color if colors else self._print_default
    self._output = output
    self.start()

  def run(self):
    while True:
      try:
        self._print(self._queue.popleft())
      except IndexError:
        time.sleep(0.1)
      except IOError:  # PIPE broken, most likely
        break

  def _print_default(self, msg):
    self._output.write(str(msg))
    self._output.flush()

  def _print_color(self, msg):
    attr = colors.COLORS[msg.src.__hash__() % NUM_COLORS]
    cfunc = getattr(colors, attr)
    self._output.write(cfunc(str(msg)))
    self._output.flush()

  def add(self, msg):
    self._queue.append(msg)


def right_arrow(level):
  return "%s%s" % ("—" * level * 4, "►" if level > 0 else "")


def format_timestamp(timestamp):
  dt = datetime.fromtimestamp(timestamp)
  return dt.strftime("%H:%M:%S:%f")


class Requests(object):
  def __init__(self):
    self.requests_by_xid = defaultdict(list)

  def add(self, req):
    self.requests_by_xid[req.xid].append(req)

  def pop(self, xid):
    return self.requests_by_xid.pop(xid) if xid in self.requests_by_xid else []


class BasePrinter(Thread):
  """ base printer for client-side messages """

  def __init__(self, colors, loopback, output=sys.stdout):
    super(BasePrinter, self).__init__()
    self.write = self.colored_write if colors else self.simple_write
    self.loopback = loopback
    self._output = output
    self.setDaemon(True)

  def run(self, *args, **kwargs):
    pass

  def request_handler(self, *args, **kwargs):
    pass

  def reply_handler(self, *args, **kwargs):
    pass

  def event_handler(self, *args, **kwargs):
    pass

  def colored_write(self, *msgs):
    c = colors.COLORS[msgs[0].client.__hash__() % NUM_COLORS]
    cfunc = getattr(colors, c)
    for i, m in enumerate(msgs):
      self._output.write(cfunc("%s%s %s" % (right_arrow(i), format_timestamp(m.timestamp), m)))
    self._output.flush()

  def simple_write(self, *msgs):
    for i, m in enumerate(msgs):
      self._output.write("%s%s %s" % (right_arrow(i), format_timestamp(m.timestamp), m))
    self._output.flush()

  def cancel(self, *args, **kwargs):
    """ will be called on KeyboardInterrupt """
    pass


class DefaultPrinter(BasePrinter):
  def __init__(self, colors, loopback, output=sys.stdout):
    super(DefaultPrinter, self).__init__(colors, loopback, output)
    self._requests_by_client = defaultdict(Requests)
    self._replies = deque()

  def run(self):
    while True:
      try:
        rep = self._replies.popleft()
      except IndexError:
        time.sleep(0.0001)
        continue

      reqs = self._requests_by_client[rep.client].pop(rep.xid)
      if not reqs:
        continue

      # HACK: if we are on the loopback, drop dupes
      msgs = reqs[0:1] + [rep] if self.loopback else reqs + [rep]
      try:
        self.write(*msgs)
      except IOError:  # PIPE broken, most likely
        break

  def request_handler(self, req):
    # close requests don't have a reply, dispatch it immediately
    if req.is_close:
      self.write(req)
    else:
      self._requests_by_client[req.client].add(req)

  def reply_handler(self, rep):
    self._replies.append(rep)

  def event_handler(self, evt):
    """ TODO: a queue for this would be good to avoid blocking pcap """
    self.write(evt)


class UnpairedPrinter(BasePrinter):
  def __init__(self, colors, loopback, output=sys.stdout):
    super(UnpairedPrinter, self).__init__(colors, loopback, output)
    self._messages = deque()

  def run(self):
    while True:
      try:
        msg = self._messages.popleft()
      except IndexError:
        time.sleep(0.01)
        continue

      try:
        self.write(msg)
      except IOError:  # PIPE broken, most likely
        break

  def request_handler(self, req):
    self._messages.append(req)

  def reply_handler(self, rep):
    self._messages.append(rep)

  def event_handler(self, evt):
    self._messages.append(evt)


def key_of(msg, group_by, depth):
  """ get the msg's attribute to be used as key for grouping """
  if group_by == "path":
    key = msg.path if depth == 0 else msg.parent_path(depth)
  elif group_by == "type":
    key = msg.name
  elif group_by == "client":
    key = msg.client
  else:
    raise ValueError("Unknown group: %s" % group_by)

  return key


class CountPrinter(BasePrinter):
  """ use to accumulate up to N requests and then print a summary """
  def __init__(self, count, group_by, loopback, aggregation_depth, output=sys.stdout):
    super(CountPrinter, self).__init__(False, loopback, output)
    self.count, self.group_by, self.aggregation_depth = count, group_by, aggregation_depth
    self.seen = 0
    self.requests = defaultdict(int)

  def run(self):
    while self.seen < self.count:
      time.sleep(0.001)

    results = sorted(self.requests.items(), key=lambda item: item[1], reverse=True)
    for res in results:
      self._output.write("%s %d\n" % res)
    self._output.flush()

  def request_handler(self, req):
    self._add(req)

  def reply_handler(self, rep):
    """ we only care about requests & watches """
    pass

  def event_handler(self, evt):
    self._add(evt)

  def _add(self, msg):
    if self.seen >= self.count:
      return

    key = key_of(msg, self.group_by, self.aggregation_depth)

    # eventually we should grab a lock here, but as of now
    # this is only called from a single thread.
    self.requests[key] += 1
    self.seen += 1


class LatencyPrinter(BasePrinter):
  """ measures latencies between requests and replies """
  def __init__(self, count, group_by, loopback, aggregation_depth, sort_by, output=sys.stdout):
    super(LatencyPrinter, self).__init__(False, loopback, output)
    self._count, self._group_by, self._aggregation_depth = count, group_by, aggregation_depth
    self._sort_by = sort_by
    self._seen = 0
    self._latencies_by_group = defaultdict(list)
    self._requests_by_client = defaultdict(Requests)
    self._replies = deque()
    self._report_done = False

  def run(self):
    self.wait_for_requests()
    self.report()

  def wait_for_requests(self):
    """ spin until we've collected all requests """
    while self._seen < self._count:
      try:
        rep = self._replies.popleft()
      except IndexError:
        time.sleep(0.001)
        continue

      reqs = self._requests_by_client[rep.client].pop(rep.xid)
      if not reqs:
        continue

      req = reqs[0]
      key = key_of(req, self._group_by, self._aggregation_depth)
      latency = rep.timestamp - req.timestamp

      self._latencies_by_group[key].append(latency)
      self._seen += 1

      # update status
      self._output.write("\rCollecting (%d/%d)" % (self._seen, self._count))
      self._output.flush()

  def report(self):
    """ calculate & display latencies """

    # TODO: this should be protected by a lock
    if self._report_done:
      return
    if self._seen < self._count:  # force wait_for_requests to finish
      self._seen = self._count
    self._report_done = True

    # clear the line
    self._output.write("\r")

    results = {}
    for key, latencies in self._latencies_by_group.items():
      result = {}
      result["avg"] = sum(latencies) / len(latencies)
      latencies = sorted(latencies)
      result["p95"] = percentile(latencies, 0.95)
      result["p99"] = percentile(latencies, 0.99)
      results[key] = result

    headers = [self._group_by, "avg", "p95", "p99"]
    data = []

    results = sorted(results.items(), key=lambda it: it[1][self._sort_by], reverse=True)
    data = [tuple([key, result["avg"], result["p95"], result["p99"]]) for key, result in results]

    self._output.write("%s\n" % tabulate(data, headers=headers))
    self._output.flush()

  def cancel(self):
    """ if we were interrupted, but haven't reported; do it now """
    self.report()

  def request_handler(self, req):
    # close requests don't have a reply, so ignore
    if not req.is_close:
      self._requests_by_client[req.client].add(req)

  def reply_handler(self, rep):
    self._replies.append(rep)

  def event_handler(self, evt):
    """ events are asynchronously generated by the server, so we can't measure latency """
    pass
