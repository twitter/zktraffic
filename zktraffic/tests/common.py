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


import os

from scapy.sendrecv import sniff


_resources_dir = os.path.join(
  os.path.realpath(os.path.dirname(__file__)),
  "resources"
)


def get_full_path(name):
  return os.path.join(_resources_dir, "%s.pcap" % (name))


def consume_packets(capture_file, sniffer):
  sniff(offline=get_full_path(capture_file), prn=sniffer.handle_packet)

def is_ci_env():
  # Travis CI: CI=true, TRAVIS=true
  # Circle CI: CI=true, CIRCLECI=true
  # Drone: CI=true, DRONE=true
  # Wercker: CI=true
  # Semaphore: CI=true, SEMAPHORE=true
  # Shippable: USER=shippable
  return os.environ.get('CI') or os.environ.get('USER') == 'shippable'
