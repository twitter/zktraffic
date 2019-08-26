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


""" helpers """

import re
import struct


INT_STRUCT = struct.Struct('!i')
LONG_STRUCT = struct.Struct('!q')
BOOL_STRUCT = struct.Struct('B')
INT_BOOL_INT = struct.Struct('!iBi')
INT_INT_STRUCT = struct.Struct('!ii')
INT_INT_LONG_STRUCT = struct.Struct('!iiq')
INT_LONG_INT_LONG_STRUCT = struct.Struct('!iqiq')
REPLY_HEADER_STRUCT = struct.Struct('!iqi')
STAT_STRUCT = struct.Struct('!qqqqiiiqiiq')


def to_bytes(value):
    """ str to bytes (py3k) """
    vtype = type(value)

    if vtype == bytes or vtype == type(None):
        return value

    try:
        return vtype.encode(value)
    except UnicodeEncodeError:
        pass
    return value


def read_number(data, offset):
  data = to_bytes(data)
  try:
    return (INT_STRUCT.unpack_from(data, offset)[0], offset + INT_STRUCT.size)
  except struct.error:
    return (0, offset)


def read_long(data, offset):
  data = to_bytes(data)
  try:
    return (LONG_STRUCT.unpack_from(data, offset)[0], offset + LONG_STRUCT.size)
  except struct.error:
    return (0, offset)


def read_bool(data, offset):
  data = to_bytes(data)
  try:
    return (BOOL_STRUCT.unpack_from(data, offset)[0] is 1, offset + BOOL_STRUCT.size)
  except struct.error:
    return (False, offset + BOOL_STRUCT.size)


class ParsingError(Exception):
  """
  no more parsing should be done after this is raised because offsets can't be
  trusted no more.
  """


class StringTooLong(ParsingError): pass


def read_string(data, offset, maxlen=1024, default="unreadable"):
  """
  Note: even though strings are utf-8 decoded, we need to str()
        them since they can't be used by intern() otherwise.
  """
  data = to_bytes(data)
  old = offset
  length, offset = read_number(data, offset)

  if length > maxlen:
    raise StringTooLong("Length %d is greater than the maximum length (%d)" % (length, maxlen))

  if length < 0:
    return ("", old)

  try:
    s = data[offset:offset + length].decode("utf-8")
  except UnicodeDecodeError:
    s = default

  # back to str, so it can be consumed by intern()
  try:
    s = str(s)
  except UnicodeEncodeError:
    s = default

  return (s, offset + length)


def read_buffer(data, offset, maxlen=1024):
  old = offset
  length, offset = read_number(data, offset)
  if length <= 0:  # pragma: no cover
    return (b'', old)

  if length > maxlen:
    return (None, old)

  return (data[offset:offset + length], offset + length)


def read_int_bool_int(data, offset):
  data = to_bytes(data)
  return (INT_BOOL_INT.unpack_from(data, offset),
          offset + INT_BOOL_INT.size)


def read_int_long_int_long(data, offset):
  data = to_bytes(data)
  return (INT_LONG_INT_LONG_STRUCT.unpack_from(data, offset),
          offset + INT_LONG_INT_LONG_STRUCT.size)


def read_int_int_long(data, offset):
  data = to_bytes(data)
  return (INT_INT_LONG_STRUCT.unpack_from(data, offset), offset + INT_INT_LONG_STRUCT.size)


def read_reply_header(data, offset):
  return (REPLY_HEADER_STRUCT.unpack_from(data, offset), offset + REPLY_HEADER_STRUCT.size)


def read_int_int(data, offset):
  data = to_bytes(data)
  return (INT_INT_STRUCT.unpack_from(data, offset), offset + INT_INT_STRUCT.size)


def parent_path(path, level):
  """ for level 3 and path /a/b/c/d/e/f this returns /a/b/c """
  return '/'.join(path.split('/')[0:level + 1])


class QuorumConfig(object):
  class BadConfig(ParsingError):
    pass

  class ConfigEntry(object):
    pass

  class Server(ConfigEntry):
    sid = -1
    zab_fle_hostname = None
    zab_port = -1
    fle_port = -1
    learner_type = None
    zk_hostname = None
    zk_port = -1

    def __init__(self, sid, address_str):
      """
      See org.apache.zookeeper.server.Quorum.QuorumPeer.QuorumServer
      :param sid: int
      :param address_str: str
      """
      self.sid = sid
      server_client_parts = address_str.split(';')
      server_parts = server_client_parts[0].split(':')
      if len(server_client_parts) > 2 or len(server_parts) < 3 \
              or len(server_parts) > 4:
        raise QuorumConfig.BadConfig(address_str)

      if len(server_client_parts) == 2:
        client_parts = server_client_parts[1].split(':')
        if len(client_parts) > 2:
          raise QuorumConfig.BadConfig(address_str)

        self.zk_hostname = client_parts[0] if len(client_parts) == 2 else '0.0.0.0'
        try:
          self.zk_port = int(client_parts[-1])
        except ValueError as e:
          raise QuorumConfig.BadConfig(e)

      self.zab_fle_hostname = server_parts[0]
      try:
        self.zab_port = int(server_parts[1])
        self.fle_port = int(server_parts[2])
      except ValueError as e:
        raise QuorumConfig.BadConfig(e)

      if len(server_parts) == 4:
        self.learner_type = server_parts[3]
        if not self.learner_type in ('participant', 'observer'):
          raise QuorumConfig.BadConfig(address_str)

  class Version(ConfigEntry):
    def __init__(self, version):
      """
      :param version: int
      """
      self.version = version

  class Unsupported(ConfigEntry):
    def __init__(self, line):
      self.s = line

  def __init__(self, config_string):
    """
    :param config_string: str
    """
    self.entries = []
    empty_matcher = re.compile('^\s*$')
    server_matcher = re.compile('server\.(\d+)=(.*)')
    version_matcher = re.compile('version=(\d+)')
    for line in config_string.splitlines():
      if empty_matcher.match(line):
        continue
      server_m = server_matcher.match(line)
      if server_m:
        sid = int(server_m.group(1))
        address_str = server_m.group(2)
        server_ent = QuorumConfig.Server(sid, address_str)
        self.entries.append(server_ent)
        continue

      version_m = version_matcher.match(line)
      if version_m:
        version = int(version_m.group(1))
        version_ent = QuorumConfig.Version(version)
        self.entries.append(version_ent)
        continue

      self.entries.append(QuorumConfig.Unsupported(line))
