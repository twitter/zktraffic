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

from collections import deque
from threading import Thread
import sys
import time

import colors


class Printer(Thread):
  def __init__(self, colors):
    super(Printer, self).__init__()
    self.setDaemon(True)
    self._queue = deque()
    self._print = self._print_color if colors else self._print_default

    self.start()

  def run(self):
    while True:
      try:
        self._print(self._queue.popleft())
      except IndexError:
        time.sleep(0.1)

  def _print_default(self, msg):
    sys.stdout.write(str(msg))
    sys.stdout.flush()

  def _print_color(self, msg):
    attr = colors.COLORS[msg.src.__hash__() % len(colors.COLORS)]
    cfunc = getattr(colors, attr)
    sys.stdout.write(cfunc(str(msg)))
    sys.stdout.flush()

  def add(self, msg):
    self._queue.append(msg)
