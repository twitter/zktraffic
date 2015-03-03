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

import time


class Timer(object):
    """ basic second-based timer """

    def __init__(self, start=None):
        self.reset()

    def after(self, seconds):
        return self.now - self.start >= float(seconds)

    def reset(self, start=None):
        self.start = float(start) if start is not None else time.time()

    @property
    def now(self):
        return time.time()
