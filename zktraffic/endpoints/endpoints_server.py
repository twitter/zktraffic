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


from zktraffic.base.sniffer import Sniffer, SnifferConfig

from twitter.common.http import HttpServer


class EndpointsServer(HttpServer):
  MAX_RESULTS = 10

  def __init__(
      self, iface, zkport, request_handler, reply_handler=None, event_handler=None):
    HttpServer.__init__(self)

    config = SnifferConfig(iface=iface)
    config.zookeeper_port = zkport
    config.update_filter()

    self._sniffer = Sniffer(config, request_handler, reply_handler, event_handler)
    self._sniffer.start()
