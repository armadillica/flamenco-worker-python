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
        for family, _, _, _, _ in socket.getaddrinfo(dest, None):
            yield family


def find_flamenco_manager(timeout=1, retries=5):
    log.info('Finding Flamenco Manager through UPnP/SSDP discovery.')

    families_and_addresses = set(interface_addresses())

    for _ in range(retries):
        failed_families = 0

        for family in families_and_addresses:
            try:
                dest = DESTINATIONS[family]
            except KeyError:
                log.warning('Unknown address family %s, skipping', family)
                continue

            log.debug('Sending to %s, dest=%s', family, dest)

            sock = socket.socket(family, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.settimeout(timeout)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            sock.bind(('', 1901))

            try:
                for _ in range(2):
                    # sending it more than once will
                    # decrease the probability of a timeout
                    sock.sendto(DISCOVERY_MSG, (dest, 1900))
            except PermissionError:
                log.info('Failed sending UPnP/SSDP discovery message to %s, dest=%s', family, dest)
                failed_families += 1
                continue

            try:
                data = sock.recv(1024)
            except socket.timeout:
                pass
            else:
                response = Response(data)
                return response.getheader('Location')

        if failed_families >= len(families_and_addresses):
            log.error('Failed to send UPnP/SSDP discovery message '
                      'to every address family (IPv4/IPv6)')
            break

    raise DiscoveryFailed('Unable to find Flamenco Manager after %i tries' % retries)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    location = find_flamenco_manager()
    print('Found the service at %s' % location)
