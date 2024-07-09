import argparse
import base64
import datetime
import json
import multiprocessing
import os
from itertools import pairwise
from multiprocessing import Pool

from consensus_mining import download_command as download_cons
from consensus_reader import ConsensusReader
from descriptor_mining import download_command as download_descs
from descriptor_reader import DescriptorReader
from geoip import IpLocator
from utils import validate_timespan, days_between


def file_check(filename):
    if not os.path.exists(filename):
        raise argparse.ArgumentTypeError("{0} does not exist".format(filename))
    return filename


def excluded_day_check(day):
    day = int(day)
    if day < 31:
        day += 1
    return day


def parse_fingerprints(filename):
    with open(filename, 'r') as f:
        fingerprints = [line[:-1] for line in f]
    return fingerprints


parser = argparse.ArgumentParser(description="This script is meant to receive a list of relays fingerprints\
                                 and retrieve the latest known information about those relays, and\
                                 dump those info in a json file for use in TorPS relay injection")
parser.add_argument("--month", type=int, required=True)
parser.add_argument("--year", type=int, required=True)
parser.add_argument("--excluded_on_day", default=30, type=excluded_day_check, help="The day the relays were excluded")
parser.add_argument("--backtrack_search", type=int, default=2, help="From the day of exclusions, search up to --backtrack_search days\
                    before within consensuses for the presence of the rejected relay info")
parser.add_argument("--fingerprints", required=True, type=file_check, metavar="FILE",
                    help="path to the file containing the relay fingerprints")
parser.add_argument("--in_dir_prefix", type=str, default="cache")
parser.add_argument("--out", type=str, default="excluded_relays.json")


def build_data(dr, key, relay, ip_locator):
    data = dict()
    hex_id = base64.b64decode(relay['id'] + '=').hex().upper()
    try:
        last_seen = datetime.datetime.strptime(key, '%Y-%m-%d-%H-00-00-consensus')
    except ValueError:
        last_seen = datetime.datetime.strptime(key[:15], '%Y%m%d-%H%M%S')
    data = {
        'last_seen_in': last_seen,
        'version': relay.get('version', None),
        'flags': relay['flags'],
        'bandwidth': relay['bandwidth'],
        'nickname': relay['nickname'],
        'id': relay['id'],
        'country': ip_locator.get_country_code(relay['ip']),
        'ipv4': relay['ip'],
    }
    descriptor = dr.get_most_recent_descriptor(
        base64.b64decode(relay['digest'] + '=').hex().lower(),
        last_seen,
        1
    )
    if descriptor is not None:
        # print(f"Descriptor found for {hex_id} from {key}")
        data |= descriptor
        if data['version'] is None and data['platform'] is not None:
            data['version'] = data['platform'].split(' ')[1]
    else:
        print(f"Missing descriptor for a relay from {key}")

    return (hex_id, data)


def get_relays_data(start: datetime.datetime, end: datetime.datetime, cache_folder: str, bridge: bool = False) -> list[dict]:
    data = dict()

    cr = ConsensusReader(start=start, end=end, folder=cache_folder, lookahead=0, bridge=bridge)
    consensuses = cr.get_relays_timespan(start, end)
    dr = DescriptorReader(start=start, end=end, folder=cache_folder, bridge=bridge)
    ip_locator = IpLocator()

    for key in sorted(consensuses.keys()):
        for relay in consensuses[key]:
            (hex_id, new_data) = build_data(dr, key, relay, ip_locator)
            if hex_id in data:
                if data[hex_id]['last_seen_in'] < new_data['last_seen_in']:
                    data[hex_id].update(new_data)
            else:
                data[hex_id] = new_data
    return data


def get_data(start, end, cache_folder):
    days = list(pairwise(days_between(start, end)))
    with Pool(multiprocessing.cpu_count()) as pool:
        data = pool.starmap(
            get_relays_data,
            [(start, end, cache_folder, False) for start, end in days]
        )
    return data


if __name__ == "__main__":
    args = parser.parse_args()
    # Get our list of rejected relays
    fingerprints = parse_fingerprints(args.fingerprints)
    # Download the consensus and descriptors; check if not already done.
    do_download_cons = False
    do_download_descs = False
    cache_folder = f"{args.in_dir_prefix}_{args.month}_{args.year}"
    if not os.path.exists(cache_folder):
        os.makedirs(cache_folder)
        do_download_cons = True
    # Get the latest data about all relays in month
    excluded_on_day = args.excluded_on_day

    end = datetime.datetime(year=args.year, month=args.month, day=excluded_on_day, hour=23)
    start = end - datetime.timedelta(days=args.backtrack_search, hours=23)

    start = start.strftime("%Y-%m-%dT%H")
    end = end.strftime("%Y-%m-%dT%H")
    if do_download_cons:
        download_cons("relay", start, end, cache_folder)
    # Download needed descriptors
    if not os.path.exists(f"{cache_folder}/relay_descriptors"):
        do_download_descs = True
    if do_download_descs:
        download_descs("relay", start, end, cache_folder)

    (start, end) = validate_timespan(start, end)
    data = get_data(start, end, cache_folder)
    data.reverse()
    fingerprint_data = {}
    for fp in fingerprints:
        for day_data in data:
            if fp in day_data:
                fingerprint_data[fp] = day_data[fp]
                break
    if len(fingerprint_data) == len(fingerprints):
        print("All relay info have been found. Dumping them into a json file.")
    else:
        print(
            f"We're missing {len(fingerprints) - len(fingerprint_data)} relay info. Maybe increasing --backtrack_search would find these")
    with open(args.out, "w") as f:
        json.dump(fingerprint_data, f, default=str)
