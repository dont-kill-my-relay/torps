from functools import lru_cache
from ipaddress import ip_address, IPv4Address, IPv6Address

class IpRange:
    def __init__(self, start: str, end: str, country: str):
        self.start = ip_address(start)
        self.end = ip_address(end)
        self.country = country

    def __contains__(self, item):
        return self.start <= item <= self.end

    def __lt__(self, other):
        if type(other) == IPv4Address or type(other) == IPv6Address:
            return self.end < other
        return self.end < other.start

    def __gt__(self, other):
        if type(other) == IPv4Address or type(other) == IPv6Address:
            return self.start > other
        return self.start > other.end


def _parse_geoip_file(geoip_f):
    result = list()
    for line in geoip_f.readlines():
        if line.startswith('#'):
            continue
        values = line.split(',')
        if len(values) != 3:
            continue
        start, end, country = values
        country = country[:2] if country[:2] != '??' else None
        try:
            result.append(IpRange(ip_address(int(start)), ip_address(int(end)), country))
        except ValueError:
            result.append(IpRange(ip_address(start), ip_address(end), country))

    return result


class IpLocator:
    def __init__(self,
                 geoip_file: str = 'geoip',
                 geoip6_file: str = 'geoip6'):

        with open(geoip_file, 'r') as geoip_f:
            self._data = _parse_geoip_file(geoip_f)
            self._data.sort()

        with open(geoip6_file, 'r') as geoip_f:
            self._data6 = _parse_geoip_file(geoip_f)
            self._data6.sort()

    @lru_cache()
    def get_country_code(self, ip: str) -> str | None:
        ip = ip_address(ip)
        if ip.is_private:
            return None

        data = self._data if ip.version == 4 else self._data6
        i = len(data) // 2
        left = 0
        right = len(data) - 1

        while left <= right:
            if ip in data[i]:
                return data[i].country
            if ip < data[i]:
                right = i - 1
            else:
                left = i + 1
            i = (left + right) // 2

        return None

