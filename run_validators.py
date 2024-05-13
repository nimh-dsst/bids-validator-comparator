#! /usr/bin/env python3


# DEPENDENCIES
#   git annex
#   datalad
#   jq
#   deno
#   node
#   bids-validator


### WE NEED TO CAPTURE ###
# PER DATASET:
#   - .bidsignore contents
#   - size of files bidsignored
#   - count of files bidsignored
#   - du for size of dataset
#   - exact validator, schema, and dependency versions
#   - real, user, and sys compute times
#   - how many participants in each dataset
#   - how many sessions in each dataset
#   - how many tasks in each dataset


import json
import pathlib
import re
import subprocess
import time
from check_bidsignore import do_the_thing


openneuro_data_folder = pathlib.Path("/data/openneuro").resolve()
# openneuro_data_folder = pathlib.Path("/home/earlea/data").resolve()
dataset_text_file = pathlib.Path("/home/earlea/data/openneuro/remaining_266.txt").resolve()

# collect list of datasets from a line separated text file
with open(dataset_text_file, 'r') as f:
    dataset_folders = [pathlib.Path(openneuro_data_folder / line.strip()) for line in f.readlines()]

# # collect list of datasets in folder
# dataset_folders = [pathlib.Path(dataset).resolve() for dataset in openneuro_data_folder.glob('ds*')]
# dataset_folders.sort()

# check if a file is a valid json file
def is_valid_json_file(json_file):
    try:
        with open(json_file, 'r') as f:
            json.load(f)
        return True
    except (ValueError, IOError, FileNotFoundError):
        return False


# measure the time it takes to run a subprocess and collect the output and info
def measure_subprocess(command, **kwargs):
    rd = {}
    start_time = time.time()
    s = subprocess.run('time ' + command, **kwargs)
    stop_time = time.time()

    rd['cmd'] = command
    rd['cmd_return_code'] = s.returncode
    rd['cmd_time_seconds'] = stop_time - start_time
    rd['stdout'] = s.stdout
    rd['stderr'] = s.stderr

    real = re.compile(r'real\t(.+s)')
    user = re.compile(r'user\t(.+s)')
    syst = re.compile(r'sys\t(.+s)')
    timing = {
        'real': real.findall(rd['stderr'])[0],
        'user': user.findall(rd['stderr'])[0],
        'sys': syst.findall(rd['stderr'])[0]
    }

    if real is not None:
        rd['unix_time'] = timing
    else:
        rd['unix_time'] = None

    return rd

# iterate through dataset folders
for i, ds in enumerate(dataset_folders):
    # if i > 0:
    #     print("Stopping.")
    #     break

    print(f"Running validators on {ds}")
    output_dictionary = {
        'legacy_version': {},
        'schema_version': {},
        'datalad_version': {},
        'gitannex_version': {},
        'node_version': {},
        'jq_version': {},
        'legacy': {},
        'schema': {},
        'datalad_get': [],
        'datalad_remove': []
    }

    ### DATALAD COMMANDS ###

    # get the dataset subfolders
    the_glob = sorted([x for x in ds.glob('*') if x.name not in ['derivatives', '.gitattributes', '.git', '.datalad']])

    for g in the_glob:
        datalad_get_cmd = f'datalad get -d {str(ds)} {g.resolve()} --recursive'
        output_dictionary['datalad_get'].append(measure_subprocess(datalad_get_cmd, shell=True, capture_output=True, text=True, executable='/bin/bash'))

    ### VERSION COMMANDS ###

    # get the legacy validator version
    legacy_version_cmd = f'bids-validator --version'
    output_dictionary['legacy_version'] = measure_subprocess(legacy_version_cmd, shell=True, capture_output=True, text=True, executable='/bin/bash')

    # get the schema validator version
    schema_version_cmd = f'~/repo/bids-validator/bids-validator/bids-validator-deno --version'
    output_dictionary['schema_version'] = measure_subprocess(schema_version_cmd, shell=True, capture_output=True, text=True, executable='/bin/bash')

    # get the datalad version
    datalad_version_cmd = f'datalad --version'
    output_dictionary['datalad_version'] = measure_subprocess(datalad_version_cmd, shell=True, capture_output=True, text=True, executable='/bin/bash')

    # get the git-annex version
    gitannex_version_cmd = f'git-annex version'
    output_dictionary['gitannex_version'] = measure_subprocess(gitannex_version_cmd, shell=True, capture_output=True, text=True, executable='/bin/bash')

    # get the node version
    node_version_cmd = f'node --version'
    output_dictionary['node_version'] = measure_subprocess(node_version_cmd, shell=True, capture_output=True, text=True, executable='/bin/bash')

    # get the jq version
    jq_version_cmd = f'jq --version'
    output_dictionary['jq_version'] = measure_subprocess(jq_version_cmd, shell=True, capture_output=True, text=True, executable='/bin/bash')

    ### VALIDATOR COMMANDS ###

    # run the bids validator on each dataset
    legacy_validator_cmd = f'bids-validator {str(ds)} --json'
    output_dictionary['legacy'] = measure_subprocess(legacy_validator_cmd, shell=True, capture_output=True, text=True, executable='/bin/bash')

    # run deno
    schema_validator_cmd = f'~/repo/bids-validator/bids-validator/bids-validator-deno {str(ds)} --json'
    output_dictionary['schema'] = measure_subprocess(schema_validator_cmd, shell=True, capture_output=True, text=True, executable='/bin/bash')

    # write out the json files for the two bids validators
    for val in ['legacy', 'schema']:
        j_file = ds.parent / 'logs' / f"{ds.stem}.{val}.json"
        print(f"{ds}: Writing {j_file}")
        try:
            with open(j_file, 'w') as outfile:
                json.dump(json.loads(output_dictionary[val]['stdout']), outfile, indent=4)
                del output_dictionary[val]['stdout']
        except Exception as e:
            print(e)

    if ds.joinpath('.bidsignore').exists():
        try:
            with open(ds.parent / 'logs' / f'{ds.stem}.bidsignore.json', 'w') as j:
                j.write(json.dumps(do_the_thing(str(ds)), indent=4))
        except Exception as e:
            print(e)

    # remove the dataset regardless to preserve disk space on the system
    print(f'Removing {ds}')
    datalad_remove_cmd = f'datalad remove -d {str(ds)} {str(ds)} --recursive'
    output_dictionary['datalad_remove'] = measure_subprocess(datalad_remove_cmd, shell=True, capture_output=True, text=True, executable='/bin/bash')

    # write out the log for the dataset
    with open(ds.parent / 'logs' / f"{ds.stem}.log.json", 'w') as f:
        json.dump(output_dictionary, f, indent=4)
