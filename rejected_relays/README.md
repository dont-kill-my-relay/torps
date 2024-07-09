# Generate data about rejected relays

The code in this folder requires Python 3 to run (unlike the remaining code). 
It is recommended to create a separate virtual environment and install the dependencies with `pip install -r -requirements.txt`.

The script creates a data structure that TorPS can use to keep the excluded relays in the simulation.

## Data needed to run the script

1. Geo IP files [from Tor](https://gitlab.torproject.org/tpo/core/tor/-/tree/main/src/config?ref_type=heads). 
   Download the `geoip` and `geoip6` files in the current directory.
2. List of the excluded relays from the [EOL policy page](https://gitlab.torproject.org/tpo/network-health/team/-/wikis/Relay-EOL-policy#tor-eol-removals)

## Run the script

Example of a command:

```shell
python3 rejected_relays_to_json.py --month 12 --year 2021 --excluded_on_day 1 --fingerprints 12012021.txt
```

`--month`, `--year` and `--excluded_on_day` set the date for the exclusion.  
`--fingerprints` is the filename with the fingerprints of excluded relays.