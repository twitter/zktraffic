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

import unittest

from zktraffic.base.util import QuorumConfig


class ParseConfigTestCase(unittest.TestCase):
  def test_parse_config(self):
    config_string = ''.join((
      'server.1=localhost:2780:2783:participant;0.0.0.0:2181\n',
      'server.2=localhost:2781:2784:participant;0.0.0.0:2182\n',
      'server.3=localhost:2782:2785:participant;0.0.0.0:2183\n',
      'version=0\n',
      '\n',
      'unsupported_key=unsupported_value\n'
    ))
    config = QuorumConfig(config_string)
    l = config.entries
    self.assertIsInstance(l[0], QuorumConfig.Server)
    self.assertEqual(l[0].sid, 1)
    self.assertEqual(l[0].zab_fle_hostname, 'localhost')
    self.assertEqual(l[0].zab_port, 2780)
    self.assertEqual(l[0].fle_port, 2783)
    self.assertEqual(l[0].learner_type, 'participant')
    self.assertEqual(l[0].zk_hostname, '0.0.0.0')
    self.assertEqual(l[0].zk_port, 2181)

    self.assertIsInstance(l[1], QuorumConfig.Server)
    self.assertEqual(l[1].sid, 2)

    self.assertIsInstance(l[2], QuorumConfig.Server)
    self.assertEqual(l[2].sid, 3)

    self.assertIsInstance(l[3], QuorumConfig.Version)
    self.assertEqual(l[3].version, 0)

    self.assertIsInstance(l[4], QuorumConfig.Unsupported)

  def test_parse_config_with_bad_server(self):
    config_string = ''.join((
      'server.1=localhost:2780:2783:participant;0.0.0.0:2181\n',
      'server.2=localhost:2781\n',
      'server.3=localhost:2782:2785:participant;0.0.0.0:2183\n',
      'version=0\n'))
    self.assertRaises(QuorumConfig.BadConfig, QuorumConfig, config_string)


  def test_parse_config_with_bad_client(self):
    config_string = ''.join((
      'server.1=localhost:2780:2783:participant;0.0.0.0:2181\n',
      'server.2=localhost:2781:2784:participant;0.0.0.0:2182:2192\n',
      'server.3=localhost:2782:2785:participant;0.0.0.0:2183\n',
      'version=0\n'))
    self.assertRaises(QuorumConfig.BadConfig, QuorumConfig, config_string)

  def test_parse_config_with_bad_learner(self):
    config_string = ''.join((
      'server.1=localhost:2780:2783:participant;0.0.0.0:2181\n',
      'server.2=localhost:2781:2784:foobar;0.0.0.0:2182\n',
      'server.3=localhost:2782:2785:participant;0.0.0.0:2183\n',
      'version=0\n'))
    self.assertRaises(QuorumConfig.BadConfig, QuorumConfig, config_string)
