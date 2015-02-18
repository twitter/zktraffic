import socket

from zktraffic.cli.zk import get_ips

import mock

@mock.patch('socket.getaddrinfo', autospec=True, spec_set=True)
def test_get_ips(mock_getaddrinfo):
  mock_getaddrinfo.side_effect = [
      [(2, 1, 6, '', ('82.94.164.162', 80))],
      [(10, 1, 6, '', ('2001:888:2000:d::a2', 80, 0, 0))]]
  hostname = 'jimmeh.twitter.com'
  ips = get_ips(hostname)
  assert ips == set(['82.94.164.162', '2001:888:2000:d::a2'])
  assert mock_getaddrinfo.mock_calls == [
      mock.call(hostname, 0, socket.AF_INET, socket.SOCK_STREAM),
      mock.call(hostname, 0, socket.AF_INET6, socket.SOCK_STREAM)]
