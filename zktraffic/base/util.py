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


def read_number(data, offset):
  try:
    return (INT_STRUCT.unpack_from(data, offset)[0], offset + INT_STRUCT.size)
  except struct.error:
    return (0, offset)


def read_long(data, offset):
  try:
    return (LONG_STRUCT.unpack_from(data, offset)[0], offset + LONG_STRUCT.size)
  except struct.error:
    return (0, offset)


def read_bool(data, offset):
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
  if length <= 0:
    return (b'', old)

  if length > maxlen:
    return (None, old)

  return (data[offset:offset + length], offset + length)


def read_int_bool_int(data, offset):
  return (INT_BOOL_INT.unpack_from(data, offset),
          offset + INT_BOOL_INT.size)


def read_int_long_int_long(data, offset):
  return (INT_LONG_INT_LONG_STRUCT.unpack_from(data, offset),
          offset + INT_LONG_INT_LONG_STRUCT.size)


def read_int_int_long(data, offset):
  return (INT_INT_LONG_STRUCT.unpack_from(data, offset), offset + INT_INT_LONG_STRUCT.size)


def read_reply_header(data, offset):
  return (REPLY_HEADER_STRUCT.unpack_from(data, offset), offset + REPLY_HEADER_STRUCT.size)


def read_int_int(data, offset):
  return (INT_INT_STRUCT.unpack_from(data, offset), offset + INT_INT_STRUCT.size)


def parent_path(path, level):
  """ for level 3 and path /a/b/c/d/e/f this returns /a/b/c """
  return '/'.join(path.split('/')[0:level + 1])
