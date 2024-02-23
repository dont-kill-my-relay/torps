import datetime
import os
import re

import stem.exit_policy


# TODO plug this code into process_consensuses.py

class DescriptorReader:
    patterns_iter = [
        re.compile(
            '^router (?P<nickname>\S*) (?P<address>\S*) (?P<or_port>\d*) (?P<socks_port>\d*) (?P<dir_port>\d*)$\n',
            re.MULTILINE),
        re.compile('^platform (?P<platform>.*)$\n', re.MULTILINE),
        re.compile('^bandwidth (?P<bandwidth_avg>\d*) (?P<bandwidth_burst>\d*) (?P<bandwidth_observed>\d*)$\n',
                   re.MULTILINE),
        re.compile('^published (?P<published>.*)$\n', re.MULTILINE),
        re.compile('^(opt )?fingerprint (?P<fingerprint>.*)$\n', re.MULTILINE),
        re.compile('^ntor-onion-key (?P<ntor_onion_key>.*)$\n', re.MULTILINE),
        re.compile('^uptime (?P<uptime>\d*)$\n', re.MULTILINE),
        re.compile('^(opt )?hibernating (?P<hibernating>[01])$\n', re.MULTILINE),
        re.compile('^(?P<type_annotation>@type .*$\n)', re.MULTILINE),
    ]

    patterns_all = {
        'exit_policy': re.compile('(^(?:reject|accept) \S*$)\n', re.MULTILINE),
        'family': re.compile('^family (.*$\n(?:^\$.*$\n)*)', re.MULTILINE)
    }

    @staticmethod
    def _get_descriptor_data_from_file(file_path):
        """
        Read the data from the given file and returns a dict with the keys passed in as argument (see regex on top)
        :param file_path: descriptor file to read
        :return: the content of the file
        """
        if not os.path.exists(file_path):
            return None

        with open(file_path, "r") as f:
            return f.read()

    def get_descriptor(self, file_path):
        keys_str = {'fingerprint', 'nickname', 'address', 'ntor_onion_key'}
        keys_bool = {'hibernating'}
        keys = {'exit_policy', 'family', 'type_annotation', 'published'}
        keys.update(keys_bool)
        keys.update(keys_str)
        descriptor = self._get_descriptor_data_from_file(file_path)
        if descriptor is None:
            return None
        result = {key: None for key in keys}
        for pattern in DescriptorReader.patterns_iter:
            for match in pattern.finditer(descriptor):
                result.update({k: v.replace(' ', '').decode() for k, v in match.groupdict().items() if k in keys_str})
                result.update({k: v for k, v in match.groupdict().items() if k in keys - keys_str})

        for key, pattern in [(k, p) for k, p in DescriptorReader.patterns_all.items() if keys is None or k in keys]:
            if key == 'family':
                found = pattern.findall(descriptor)
                # flatten list of lists
                found = {x.decode() for xs in [item.split() for item in found] for x in xs}
                result.update({key: found})
            else:
                result.update({key: pattern.findall(descriptor)})
        result['hibernating'] = result['hibernating'] == '1'
        result['exit_policy'] = stem.exit_policy.ExitPolicy(*result['exit_policy'])
        try:
            result['published'] = datetime.datetime.strptime(result['published'][:19], '%Y-%m-%d %H:%M:%S')
        except:
            print file_path
            return None
        return result


if __name__ == '__main__':
    print(DescriptorReader().get_descriptor('in/server-descriptors-2013-12/0fe56b6ef8315a9ed6b93aac6aa731815246f93b'))
