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

import httplib
import socket
import threading
import time

from zktraffic.endpoints.stats_server import StatsServer

from .common import consume_packets

import bottle
from twitter.common.http import HttpServer
from twitter.common.http.diagnostics import DiagnosticsEndpoints


def test_endpoints():
  class Server(HttpServer):
    pass

  server_addr = "127.0.0.1"
  server_port = 8080

  bottle.ServerAdapter.quiet = True

  stats = StatsServer("yolo", 2181, 1, 10, 100, 100, 100, False)
  server = Server()
  server.mount_routes(DiagnosticsEndpoints())
  server.mount_routes(stats)

  # FIXME(rgs): how do you get a free port in Travis?
  worker = threading.Thread(target=server.run, args=(server_addr, server_port))
  worker.setDaemon(True)
  worker.start()

  consume_packets('set_data', stats.sniffer)

  def ping(server, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      sock.connect((server, port))
      return True
    except socket.error:
      return False
    finally:
      sock.close()

  for i in range(0, 10):
    if ping(server_addr, server_port):
      break
    time.sleep(1)
  else:
    raise Exception("server didn't come up")

  conn = httplib.HTTPConnection("127.0.0.1:8080")
  conn.request("GET", "/json/info")
  resp = conn.getresponse()
  assert resp.status == 200
  assert "uptime" in resp.read()

  conn.close()
