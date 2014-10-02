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


def sizeof_fmt(num):
  for x in ('', 'KB', 'MB', 'GB'):
    if num < 1024.0:
      if x == '':
        return "%d%s" % (num, x)
      else:
        return "%3.1f%s" % (num, x)
    num /= 1024.0
  return "%3.1f%s" % (num, 'TB')


class Counters(object):
  ALL = -1
  WRITES = 0
  READS = 1
  CREATE = 2
  SET_DATA = 3
  GET_DATA = 4
  DELETE = 5
  GET_CHILDREN = 6
  EXISTS = 7
  CREATE_BYTES = 8
  SET_DATA_BYTES = 9
  GET_DATA_BYTES = 10
  DELETE_BYTES = 11
  GET_CHILDREN_BYTES = 12
  EXISTS_BYTES = 13


CountersByName = {
  "all": Counters.ALL,
  "writes": Counters.WRITES,
  "reads": Counters.READS,
  "create": Counters.CREATE,
  "getdata": Counters.GET_DATA,
  "setdata": Counters.SET_DATA,
  "delete": Counters.DELETE,
  "getchildren": Counters.GET_CHILDREN,
  "getchildren_bytes": Counters.GET_CHILDREN_BYTES,
  "create_bytes": Counters.CREATE_BYTES,
  "getdata_bytes": Counters.GET_DATA_BYTES,
  "setdata_bytes": Counters.SET_DATA_BYTES,
  "delete_bytes": Counters.DELETE_BYTES,
}


def counter_to_str(counter):
  for name, c in CountersByName.items():
    if counter == c:
      return name
  return ""
