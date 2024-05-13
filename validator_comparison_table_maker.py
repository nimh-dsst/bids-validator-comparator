#! /usr/bin/env python3

import json
import pandas
from glob import glob
from copy import deepcopy

output = []

for i, ds_log_json in enumerate(sorted(glob('*.log.json'))):

    ds = ds_log_json.rstrip('.log.json')

    for val in ['legacy', 'schema']:

        ds_json = ds + '.' + val + '.json'

        try:
            with open(ds_json, 'r') as f:
                j = json.load(f)
        except:
            continue

        out = {}
        out['dataset'] = ds
        out['validator'] = val

        if val == 'legacy':

            for key in ['errors', 'warnings']:
                out['class'] = key
                issues = j['issues'][key]
                for issue in issues:

                    out['severity'] = issue['severity']
                    out['issue'] = issue['key']

                    try:
                        out['evidence'] = issue['files'][0]['evidence']
                    except:
                        out['evidence'] = None

                    try:
                        out['example_path'] = issue['files'][0]['file']['relativePath']
                    except:
                        out['example_path'] = None

                    output.append(deepcopy(out))

        elif val == 'schema':

            key = 'issues'
            out['class'] = None
            issues = j[key]

            for issue in issues:

                out['severity'] = issue['severity']
                out['issue'] = issue['key']

                try:
                    out['evidence'] = issue['files'][0]['evidence']
                except:
                    out['evidence'] = None

                out['example_path'] = issue['files'][0]['path']
                output.append(deepcopy(out))

df = pandas.DataFrame(output)
df.to_parquet('validator_comparison_table.parquet')
