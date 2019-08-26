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
from setuptools import find_packages, setup
import sys


PYTHON3 = sys.version_info > (3, )
HERE = os.path.abspath(os.path.dirname(__file__))


def readme():
    with open(os.path.join(HERE, 'README.md')) as f:
        return f.read()


def get_version():
    with open(os.path.join(HERE, "zktraffic/__init__.py"), "r") as f:
        content = "".join(f.readlines())
    env = {}
    if PYTHON3:
        exec(content, env, env)
    else:
        compiled = compile(content, "get_version", "single")
        eval(compiled, env, env)
    return env["__version__"]


setup(name='zktraffic',
      version=get_version(),
      description='ZooKeeper protocol analyzer and stats gathering daemon',
      long_description=readme(),
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Networking',
      ],
      keywords='ZooKeeper Sniffer Stats',
      url='https://github.com/twitter/zktraffic',
      author='Raul Gutierrez Segales',
      author_email='rgs@twitter.com',
      license='Apache',
      packages=find_packages(),
      test_suite="zktraffic.tests",
      scripts=['bin/fle-dump', 'bin/zab-dump', 'bin/zk-dump', 'bin/zk-stats-daemon', 'bin/zk-omni-dump'],
      install_requires=[
          'ansicolors',
          'dpkt==1.9.2',
          'hexdump',
          'psutil==5.6.1',
          'scapy==2.4.2',
          'six==1.12.0',
          'tabulate',
          'twitter.common.app==0.3.11',
          'twitter.common.collections==0.3.11',
          'twitter.common.decorators==0.3.11',
          'twitter.common.exceptions==0.3.11',
          'twitter.common.http==0.3.11',
          'twitter.common.log==0.3.11',
      ],
      tests_require=[
          'dpkt==1.9.2',
          'mock',
          'nose',
          'psutil==5.6.1',
          'scapy==2.4.2',
          'six==1.12.0',
          'twitter.common.log',
      ],
      extras_require={
          'test': [
              'dpkt==1.9.2',
              'mock',
              'nose',
              'twitter.common.log',
              'scapy==2.4.2',
              'six==1.12.0',
          ],
      },
      include_package_data=True,
      zip_safe=False
)
