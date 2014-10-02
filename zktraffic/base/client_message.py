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


from collections import namedtuple
import struct

from .util import (
  parent_path,
  ParsingError,
  read_bool,
  read_buffer,
  read_int_bool_int,
  read_int_long_int_long,
  read_long,
  read_number,
  read_string,
  StringTooLong,
)
from .zookeeper import (
  AUTH_XID,
  can_set_watch,
  DeserializationError,
  has_path,
  MultiHeader,
  OpCodes,
  PING_XID,
  read_opcode,
  read_path,
  req_type_to_str,
  SET_WATCHES_XID,
  ZK_VALID_PROTOCOL_VERSIONS,
  ZK_WRITE_OPS,
)


class ClientMessageType(type):
  TYPES = {}
  OPCODE = None

  def __new__(cls, clsname, bases, dct):
    obj = super(ClientMessageType, cls).__new__(cls, clsname, bases, dct)
    if obj.OPCODE in cls.TYPES:
      raise ValueError("Duplicate class/opcode name: %s" % obj.OPCODE)
    else:
      if obj.OPCODE is not None:
        cls.TYPES[obj.OPCODE] = obj
      return obj

  @classmethod
  def get(cls, key, default=None):
    return cls.TYPES.get(key, default)


class ClientMessage(ClientMessageType('ClientMessageType', (object,), {})):
  """
  client is ipaddr:port - it could be IPv6 so deal with that
  """
  __slots__ = ("size", "xid", "path", "client", "timestamp", "watch", "auth")

  def __init__(self, size, xid, path, client, watch):
    self.size = size
    self.xid = xid
    self.path = intern(path)
    self.client = client
    self.watch = watch
    self.timestamp = 0  # this will be set by caller later on
    self.auth = ""      # ditto

  MAX_REQUEST_SIZE = 100 * 1024 * 1024

  @classmethod
  def with_params(cls, xid, path, watch, data, offset, size, client):
    """
    Build a ClientMessage with the given params, possibly parsing some more.

    This must be overridden by subclasses so the specific parameters to each subclass
    can be extracted.

    :param xid: the transaction id associated with this ClientMessage
    :param path: the path associated with this ClientMessage, if any
    :param watch: a boolean indicating if watch on the path should be set
    :param data: the remaining data of the associated data packet
    :param offset: the offset from which the data should be read
    :param size: the total size of this ClientMessage
    :param client: the ip:port generating this ClientMessage

    :returns: Returns an instance of the specific ClientMessage subclass.
    :raises DeserializationError: if parsing the ClientMessage fails.
    """
    return cls(size, xid, path, client, watch)

  @classmethod
  def from_payload(cls, data, client):
    length, offset = read_number(data, 0)

    # Note: the C library doesn't include the length at the start
    if length >= cls.MAX_REQUEST_SIZE or length in (PING_XID, AUTH_XID, SET_WATCHES_XID):
      xid = length
      length = 0
    elif length == 0:
      length = 0
      return ConnectRequest.with_params(None, None, None, data, 0, length, client)
    elif length < 0:
      raise DeserializationError("Bad request length: %d" % (length))
    else:
      offs_start = offset  # if the xid is 0, it's a Connect so save the offset
      xid, offset = read_number(data, offset)
      if xid not in (PING_XID, AUTH_XID, SET_WATCHES_XID) and xid < 0:
        raise DeserializationError("Wrong XID: %d" % (xid))
      elif xid in ZK_VALID_PROTOCOL_VERSIONS:
        try:
          return ConnectRequest.with_params(None, None, None, data, offs_start, length, client)
        except DeserializationError:
          pass

    opcode, offset = read_opcode(data, offset)
    path, offset = read_path(data, offset) if has_path(opcode) else ("", offset)
    length, offset = read_number(data, offset) if length == 0 else (length, offset)
    watch, offset = read_bool(data, offset) if can_set_watch(opcode) else (False, offset)
    handler = ClientMessageType.get(opcode, cls)
    return handler.with_params(xid, path, watch, data, offset, length, client)

  @property
  def name(self):
    return req_type_to_str(self.opcode)

  @property
  def ip(self):
    """ client is ipaddr:port (maybe IPv6) """
    return self.client.rsplit(":", 1)[0]

  @property
  def port(self):
    """ client is ipaddr:port (maybe IPv6) """
    p = self.client.rfind(":")
    return self.client[p + 1:]

  def parent_path(self, level):
    return parent_path(self.path, level)

  @property
  def is_write(self):
    return self.opcode in ZK_WRITE_OPS

  @property
  def is_ping(self):
    return self.xid == PING_XID

  @property
  def is_auth(self):
    return self.xid == AUTH_XID

  @property
  def is_close(self):
    return self.opcode == OpCodes.CLOSE

  @property
  def opcode(self):
    return self.OPCODE

  def __str__(self):
    return "%s(xid=%d, path=%s, watch=%s, size=%d, client=%s)\n" % (
      self.name, self.xid, self.path, self.watch, self.size, self.client)


# TODO: Connect, Ping and Auth should probably inherit from ClientMessage since
#       they are not properly requests "on the ZK Datatree".
class Request(ClientMessage):
  pass


class PingRequest(Request):
  OPCODE = OpCodes.PING

  def __init__(self, client):
    super(PingRequest, self).__init__(0, PING_XID, "", client, False)

  @classmethod
  def with_params(cls, xid, path, watch, data, offset, size, client):
    return cls(client)

  def __str__(self):
    return "%s(client=%s)\n" % (self.name, self.client)


class ConnectRequest(Request):
  OPCODE = OpCodes.CONNECT

  def __init__(self, size, client, protocol, readonly, session, password, zxid, timeout):
    self.protocol = protocol
    self.readonly = readonly
    self.session = session
    self.password = password
    self.zxid = zxid
    self.timeout = timeout
    super(ConnectRequest, self).__init__(size, 0, "", client, False)

  @classmethod
  def with_params(cls, xid, path, watch, data, offset, size, client):
    try:
      (protocol, zxid, timeout, session), offset = read_int_long_int_long(data, offset)
    except struct.error:
      raise DeserializationError("Couldn't read connect request")

    if protocol not in ZK_VALID_PROTOCOL_VERSIONS:
      raise DeserializationError("Bad protocol version: %d" % (protocol))

    password, offset = read_buffer(data, offset)
    readonly, offset = read_bool(data, offset)

    return cls(size, client, protocol, readonly, session, password, zxid, timeout)

  @property
  def name(self):
    return "%s%s" % ("Re" if self.is_reconnect else "", super(ConnectRequest, self).name)

  @property
  def is_reconnect(self):
    return int(self.session) != 0

  def __str__(self):
    return "%s(ver=%s, zxid=%s, timeout=%s, session=0x%x, readonly=%s, client=%s)\n" % (
      self.name, self.protocol, self.zxid, self.timeout, self.session, self.readonly, self.client)


class SetAuthRequest(Request):
  OPCODE = OpCodes.SETAUTH

  def __init__(self, auth_type, scheme, credential, size, client):
    self.auth_type = auth_type
    self.scheme = scheme
    self.credential = credential

    # HACK: use part of the cred as the path so we can track auth requests
    path = "/%s" % (self.credential if self.credential else "")

    super(SetAuthRequest, self).__init__(size, AUTH_XID, path, client, False)

  @classmethod
  def with_params(cls, xid, path, watch, data, offset, size, client):
    auth_type, offset = read_number(data, offset)
    scheme = "very-long-scheme"
    credential = "very-long-credential"

    try:
      scheme, offset = read_string(data, offset)
      credential, offset = read_string(data, offset)
    except StringTooLong:
      pass

    return cls(auth_type, intern(scheme), intern(credential), size, client)

  def __str__(self):
    return "%s(type=%s, scheme=%s, credential=%s)\n" % (
      self.name, self.auth_type, self.scheme, self.credential)


class CloseRequest(Request):
  OPCODE = OpCodes.CLOSE

  def __init__(self, size, xid, client):
    super(CloseRequest, self).__init__(size, xid, "", client, False)

  @classmethod
  def with_params(cls, xid, path, watch, data, offset, size, client):
    return cls(size, xid, client)

  def __str__(self):
    return "%s(client=%s)\n" % (self.name, self.client)


class Acl(namedtuple("Acl", "perm scheme cred")): pass


class CreateRequest(Request):
  OPCODE = OpCodes.CREATE
  MAX_ACLS = 10
  MAX_PKT_SIZE = 8192

  def __init__(self, size, xid, path, client, watch, ephemeral, sequence, acls):
    super(CreateRequest, self).__init__(size, xid, path, client, watch)
    self.ephemeral = ephemeral
    self.sequence = sequence
    self.acls = acls

  @classmethod
  def with_params(cls, xid, path, watch, data, offset, size, client):
    acls = []
    ephemeral = False
    sequence = False
    data_length, offset = read_number(data, offset)

    if data_length >= 0 and data_length < cls.MAX_PKT_SIZE:
      offset += data_length

      acls_count, offset = read_number(data, offset)
      if acls_count < cls.MAX_ACLS:
        bad_acls = False
        for i in range(0, acls_count):
          perms, offset = read_number(data, offset)

          try:
            scheme, offset = read_string(data, offset)
            cred, offset = read_string(data, offset)
          except StringTooLong:
            bad_acls = True
            break

          acls.append(Acl(perms, scheme, cred))

        if not bad_acls:
          flags, offset = read_number(data, offset)
          ephemeral = flags & 0x1 == 1
          sequence = flags & 0x2 == 2

    return cls(size, xid, path, client, watch, ephemeral, sequence, acls)

  @property
  def name(self):
    return "CreateEphemeralRequest" if self.ephemeral else self.__class__.__name__

  def __str__(self):
    return "%s(size=%d, xid=%d, path=%s, ephemeral=%s, sequence=%s, client=%s)\n" % (
      self.name, self.size, self.xid, self.path, self.ephemeral, self.sequence, self.client)


class Create2Request(CreateRequest):
  OPCODE = OpCodes.CREATE2


class DeleteRequest(Request):
  OPCODE = OpCodes.DELETE


class SetWatchesRequest(Request):
  OPCODE = OpCodes.SETWATCHES
  MAX_WATCHES = 100

  class TooManyWatches(ParsingError): pass

  def __init__(self, size, xid, path, client, relzxid, data, exist, child):
    super(SetWatchesRequest, self).__init__(size, xid, path, client, True)
    self.relzxid = relzxid
    self.data = data
    self.exist = exist
    self.child = child

  @classmethod
  def with_params(cls, xid, path, watch, data, offset, size, client):
    relzxid, offset = read_long(data, offset)

    dataw = existw = childw = []
    try:
      dataw, offset = cls.read_strings(data, offset)
      existw, offset = cls.read_strings(data, offset)
      childw, offset = cls.read_strings(data, offset)
    except ParsingError:
      pass

    return cls(size, xid, path, client, relzxid, dataw, existw, childw)

  @classmethod
  def read_strings(cls, data, offset):
    """
    reads a list<str>

    Note: this might return early if a very long string is found. So the returned
    offset might not be the actual offset.
    """
    strs = []

    num_strs, offset = read_number(data, offset)
    if num_strs > cls.MAX_WATCHES:
      raise cls.TooManyWatches()

    for i in range(0, num_strs):
      s, offset = read_string(data, offset)
      strs.append(s)

    return (strs, offset)

  def __str__(self):
    return "%s(relzxid=%d, data=%s, exist=%s, child=%s, client=%s)\n" % (
      self.name, self.relzxid, self.data, self.exist, self.child, self.client)


class MultiRequest(Request):
  OPCODE = OpCodes.MULTI

  def __init__(self, size, xid, client, first_header):
    super(MultiRequest, self).__init__(size, xid, "", client, True)
    self.headers = [first_header]

  @classmethod
  def with_params(cls, xid, path, watch, data, offset, size, client):
    (first_opcode, done, err), _ = read_int_bool_int(data, offset)
    return cls(size, xid, client, MultiHeader(first_opcode, done, err))

  def __str__(self):
    return "%s(%s, client=%s)\n" % (
      self.name, self.headers[0], self.client)


class GetChildrenRequest(Request):
  OPCODE = OpCodes.GETCHILDREN


class GetChildren2Request(Request):
  OPCODE = OpCodes.GETCHILDREN2


class Check(Request):
  OPCODE = OpCodes.CHECK


class GetDataRequest(Request):
  OPCODE = OpCodes.GETDATA


class ExistsRequest(Request):
  OPCODE = OpCodes.EXISTS


class SyncRequest(Request):
  OPCODE = OpCodes.SYNC


class SetDataRequest(Request):
  OPCODE = OpCodes.SETDATA


class GetAclRequest(Request):
  OPCODE = OpCodes.GETACL


class SetAclRequest(Request):
  OPCODE = OpCodes.SETACL
