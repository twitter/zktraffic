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


""" ZooKeeper protocol definitions """

from collections import namedtuple

from .util import read_number, read_string, StringTooLong

from twitter.common import log


class OpCodes(object):
  CONNECT = 0
  CREATE = 1
  DELETE = 2
  EXISTS = 3
  GETDATA = 4
  SETDATA = 5
  GETACL = 6
  SETACL = 7
  GETCHILDREN = 8
  SYNC = 9
  PING = 11
  GETCHILDREN2 = 12
  CHECK = 13
  MULTI = 14
  CREATE2 = 15
  CLOSE = -11
  SETAUTH = 100
  SETWATCHES = 101


ZK_REQUEST_TYPES = {
  OpCodes.CONNECT: 'ConnectRequest',
  OpCodes.CREATE: 'CreateRequest',
  OpCodes.DELETE: 'DeleteRequest',
  OpCodes.EXISTS: 'ExistsRequest',
  OpCodes.GETDATA: 'GetDataRequest',
  OpCodes.SETDATA: 'SetDataRequest',
  OpCodes.GETACL: 'GetAclRequest',
  OpCodes.SETACL: 'SetAclRequest',
  OpCodes.GETCHILDREN: 'GetChildrenRequest',
  OpCodes.SYNC: 'SyncRequest',
  OpCodes.PING: 'PingRequest',
  OpCodes.GETCHILDREN2: 'GetChildren2Request',
  OpCodes.CHECK: 'CheckRequest',
  OpCodes.MULTI: 'MultiRequest',
  OpCodes.CREATE2: 'CreateRequest',
  OpCodes.CLOSE: 'CloseRequest',
  OpCodes.SETAUTH: 'SetAuthRequest',
  OpCodes.SETWATCHES: 'SetWatchesRequest',
}


ZK_VALID_PROTOCOL_VERSIONS = (0, 1)


ZK_WRITE_OPS = (
  OpCodes.CREATE,
  OpCodes.CREATE2,
  OpCodes.DELETE,
  OpCodes.SETDATA,
  OpCodes.MULTI,
  OpCodes.SETACL)


ZK_CAN_SET_WATCH = (
  OpCodes.EXISTS,
  OpCodes.GETDATA,
  OpCodes.GETCHILDREN,
  OpCodes.GETCHILDREN2)


WATCH_XID = -1
PING_XID = -2
AUTH_XID = -4
SET_WATCHES_XID = -8


def req_type_to_str(request_type):
  if request_type in ZK_REQUEST_TYPES:
    return ZK_REQUEST_TYPES[request_type]

  return "Unknown (%s)" % (request_type)


def can_set_watch(opcode):
  return opcode in ZK_CAN_SET_WATCH


def has_path(opcode):
  return opcode not in (
    OpCodes.CONNECT,
    OpCodes.SETWATCHES,
    OpCodes.PING,
    OpCodes.SETAUTH,
    OpCodes.MULTI,
    OpCodes.CLOSE)


def read_path(data, offset):
  try:
    path, offset = read_string(data, offset)
  except StringTooLong as ex:
    raise DeserializationError(str(ex))
  if not path.startswith('/'):
    log.debug("Bad path in request: %s", path)
    raise DeserializationError("Invalid path: %s" % (path))
  return (path, offset)


def read_opcode(data, offset):
  opcode, offset = read_number(data, offset)
  if opcode not in ZK_REQUEST_TYPES:
    log.debug("Bad request type: %s", opcode)
    raise DeserializationError("Invalid request type: %d" % (opcode))
  return (opcode, offset)


class DeserializationError(Exception):
  pass


class MultiHeader(namedtuple("MultiHeader", "opcode done error")):
  pass
