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


import mock
import os
import sys

from zktraffic.base.process import ProcessOptions

import psutil


proc = ProcessOptions()


def test_niceness():
  """
  Test CPU niceness calls
  """
  proc.set_niceness(15)
  assert proc.get_niceness() == 15


def test_cpu_affinity_parsing():
    """
    Test CPU affinity list parsing
    """
    assert ProcessOptions.parse_cpu_affinity('0,1,2,3,4,5') == [0, 1, 2, 3, 4, 5]
    assert ProcessOptions.parse_cpu_affinity('2') == [2]


def test_cpu_affinity():
  """
  Test CPU affinity setting
  """
  def mock_cpu_affinity_handler(self, *args, **kwargs):
      return [0, 1]

  # if running in TravisCI, or on OS X without CPI affinty,
  # mock the cpu_affinity() method call
  if os.environ.get('TRAVIS') or sys.platform.startswith('darwin'):
      with mock.patch.object(psutil.Process, 'cpu_affinity', create=True, new=mock_cpu_affinity_handler):
          proc.set_cpu_affinity('0,1')
          assert proc.get_cpu_affinity() == [0, 1]
  else:
    proc.set_cpu_affinity('0,1')
    assert proc.get_cpu_affinity() == [0, 1]
    proc.set_cpu_affinity('1')
    assert proc.get_cpu_affinity() == [1]
