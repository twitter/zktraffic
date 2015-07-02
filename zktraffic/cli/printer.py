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
  def __init__(self, colors, output=sys.stdout, skip_print=None):
    super(Printer, self).__init__()
    self.setDaemon(True)
    self._queue = deque()
    self._print = self._print_color if colors else self._print_default
    self._output = output
    self._stopped = True
    self._wants_stopped = False
    self._skip_print = skip_print  # a callable that takes msg and returns a bool
    self.start()

  @property
  def stopped(self):
    return self._stopped

  def stop(self):
    self._wants_stopped = True

  @property
  def empty(self):
    return len(self._queue) == 0

  def run(self):
    self._stopped = False

    while not self._wants_stopped:
      try:
        msg = self._queue.popleft()
        if not self._skip_print or not self._skip_print(msg):
          self._print(msg)
      except IndexError:
        time.sleep(0.1)
      except IOError:  # PIPE broken, most likely
        break

    self._stopped = True

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

  def __len__(self):
    return len(self.requests_by_xid)

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
    self._seen_requests = 0
    self._seen_replies = 0
    self._seen_events = 0
    self._wants_stopped = False
    self._stopped = True

  @property
  def stopped(self):
    return self._stopped

  @property
  def seen_requests(self):
    return self._seen_requests

  @property
  def seen_replies(self):
    return self._seen_replies

  @property
  def seen_events(self):
    return self._seen_events

  def run(self, *args, **kwargs):  # pragma: no cover
    pass

  def request_handler(self, *args, **kwargs):  # pragma: no cover
    pass

  def reply_handler(self, *args, **kwargs):  # pragma: no cover
    pass

  def event_handler(self, *args, **kwargs):  # pragma: no cover
    pass

  def colored_write(self, *msgs):  # pragma: no cover
    c = colors.COLORS[msgs[0].client.__hash__() % NUM_COLORS]
    cfunc = getattr(colors, c)
    for i, m in enumerate(msgs):
      self._output.write(cfunc("%s%s %s" % (right_arrow(i), format_timestamp(m.timestamp), m)))
    self._output.flush()

  def simple_write(self, *msgs):
    for i, m in enumerate(msgs):
      self._output.write("%s%s %s" % (right_arrow(i), format_timestamp(m.timestamp), m))
    self._output.flush()

  def cancel(self, *args, **kwargs):  # pragma: no cover
    """ will be called on KeyboardInterrupt """
    pass

  def stop(self):
    """" request the printer to stop """
    self._wants_stopped = True

  @property
  def empty(self):  # pragma: no cover
    """ returns true if nothing is queued """
    return True


class DefaultPrinter(BasePrinter):
  def __init__(self, colors, loopback, output=sys.stdout):
    super(DefaultPrinter, self).__init__(colors, loopback, output)
    self._requests_by_client = defaultdict(Requests)
    self._replies = deque()

  @property
  def empty(self):
    """ returns true if nothing is queued """
    return not any(self._requests_by_client.values()) and len(self._replies) == 0

  def run(self):
    self._stopped = False

    while not self._wants_stopped:
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

    self._stopped = True

  def request_handler(self, req):
    # close requests don't have a reply, dispatch it immediately
    if req.is_close:
      self.write(req)
    else:
      self._requests_by_client[req.client].add(req)

    self._seen_requests += 1

  def reply_handler(self, rep):
    self._replies.append(rep)
    self._seen_replies += 1

  def event_handler(self, evt):
    """ TODO: a queue for this would be good to avoid blocking pcap """
    self.write(evt)
    self._seen_events += 1


class UnpairedPrinter(BasePrinter):
  def __init__(self, colors, loopback, output=sys.stdout):
    super(UnpairedPrinter, self).__init__(colors, loopback, output)
    self._messages = deque()

  @property
  def empty(self):
    """ returns true if nothing is queued """
    return len(self._messages) == 0

  def run(self):
    self._stopped = False

    while not self._wants_stopped:
      try:
        msg = self._messages.popleft()
      except IndexError:
        time.sleep(0.0001)
        continue

      try:
        self.write(msg)
      except IOError:  # PIPE broken, most likely
        break

    self._stopped = True

  def request_handler(self, req):
    self._messages.append(req)
    self._seen_requests += 1

  def reply_handler(self, rep):
    self._messages.append(rep)
    self._seen_replies += 1

  def event_handler(self, evt):
    self._messages.append(evt)
    self._seen_events += 1


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
    self._stopped = False

    while self.seen < self.count:
      time.sleep(0.001)

    results = sorted(self.requests.items(), key=lambda item: item[1], reverse=True)
    for res in results:
      self._output.write("%s %d\n" % res)
    self._output.flush()

    self._stopped = True

  def request_handler(self, req):
    self._add(req)
    self._seen_requests += 1

  def reply_handler(self, rep):
    """ we only care about requests & watches """
    self._seen_replies += 1

  def event_handler(self, evt):
    self._add(evt)
    self._seen_events += 1

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
  def __init__(self, count, group_by, loopback, aggregation_depth, sort_by, output=sys.stdout, include_pings=True):
    super(LatencyPrinter, self).__init__(False, loopback, output)
    self._count, self._group_by, self._aggregation_depth = count, group_by, aggregation_depth
    self._sort_by = sort_by
    self._seen = 0
    # FIXME: accounting pings is broken because their uniqueness is based on timestamps,
    #        so we disable them for tests.
    self._include_pings = include_pings
    self._latencies_by_group = defaultdict(list)
    self._requests_by_client = defaultdict(Requests)
    self._replies = deque()
    self._report_done = False

  def run(self):
    self._stopped = False
    self.wait_for_requests()
    self.report()
    self._stopped = True

  def wait_for_requests(self):
    """ spin until we've collected all requests """
    while self._seen < self._count:
      try:
        rep = self._replies.popleft()
      except IndexError:
        time.sleep(0.001)
        continue

      # FIXME: this drops extra pings
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

  def cancel(self):  # pragma: no cover
    """ if we were interrupted, but haven't reported; do it now """
    self.report()

  def request_handler(self, req):
    if req.is_close:  # close requests don't have a reply, so ignore
      return

    if not self._include_pings and req.is_ping:
      return

    self._requests_by_client[req.client].add(req)
    self._seen_requests += 1

  def reply_handler(self, rep):
    if not self._include_pings and rep.is_ping:
      return

    self._replies.append(rep)
    self._seen_replies += 1

  def event_handler(self, evt):
    """ events are asynchronously generated by the server, so we can't measure latency """
    self._seen_events += 1
