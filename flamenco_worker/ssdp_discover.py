import logging
import socket
from http.client import HTTPResponse

DISCOVERY_MSG = (b'M-SEARCH * HTTP/1.1\r\n' +
                 b'ST: urn:flamenco:manager:0\r\n' +
                 b'MX: 3\r\n' +
                 b'MAN: "ssdp:discover"\r\n' +
                 b'HOST: 239.255.255.250:1900\r\n\r\n')

# We use site-local multicast, both in IPv6 and IPv4.
DESTINATIONS = {
    socket.AF_INET6: 'FF05::C',
    socket.AF_INET: '239.255.255.250',
}

log = logging.getLogger(__name__)


class DiscoveryFailed(Exception):
    """Raised when we cannot find a Manager through SSDP."""


class Response(HTTPResponse):
    # noinspection PyMissingConstructor
    def __init__(self, payload: bytes):
        from io import BytesIO

        self.fp = BytesIO(payload)
        self.debuglevel = 0
        self.strict = 0
        self.headers = self.msg = None
        self._method = None
        self.begin()


def interface_addresses():
    for dest in ('0.0.0.0', '::'):
        for family, _, _, _, sockaddr in socket.getaddrinfo(dest, None):
            yield family, sockaddr[0]


def unique(addresses):
    seen = set()
    for family_addr in addresses:
        if family_addr in seen:
            continue

        seen.add(family_addr)
        yield family_addr


def find_flamenco_manager(timeout=1, retries=5):
    log.info('Finding Flamenco Manager through UPnP/SSDP discovery.')

    socket.setdefaulttimeout(timeout)
    families_and_addresses = list(unique(interface_addresses()))

    for _ in range(retries):
        for family, addr in families_and_addresses:
            try:
                dest = DESTINATIONS[family]
            except KeyError:
                log.warning('Unknown address family %s, skipping', family)
                continue

            log.debug('Sending to %s %s' % (family, addr))

            sock = socket.socket(family, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            sock.bind((addr, 0))

            for _ in range(2):
                # sending it more than once will
                # decrease the probability of a timeout
                sock.sendto(DISCOVERY_MSG, (dest, 1900))

            try:
                data = sock.recv(1024)
            except socket.timeout:
                pass
            else:
                response = Response(data)
                return response.getheader('Location')

    raise DiscoveryFailed('Unable to find Flamenco Manager after %i tries' % retries)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    location = find_flamenco_manager()
    print('Found the service at %s' % location)
