#! /usr/bin/python3
import subprocess
import os
import argparse
import pathlib
import sys
import re
import fnmatch
import json
from pprint import pprint

def do_the_thing(dataset):
    # check for a .bidsignore file
    dataset = pathlib.Path(dataset)
    top_level_files = os.listdir(dataset)
    if '.bidsignore' in top_level_files:
        bidsignore = pathlib.Path(dataset) / '.bidsignore'
    else:
        bidsignore = None

    if not bidsignore:
        return None

    # make sure bidsignore is readable
    try:
        with open(bidsignore, 'r') as infile:
            bidsignore_content = infile.readlines()
    except (IOError, FileNotFoundError):
        raise IOError(f"Unable to load contents from {bidsignore}")

    bidsignore_content = [line.strip() for line in bidsignore_content if not re.search(r"^#", line)]
    # trim newlines from content
    bidsignore_content = [line.strip() for line in bidsignore_content if len(line.strip()) > 0]

    all_files = []
    # get all the filenames
    for root, folders, files in os.walk(dataset):
        for f in files:
            f = pathlib.Path(root) / pathlib.Path(f)
            all_files.append(str(f.relative_to(dataset)))

    # remove any files that are in the derivatives folder
    all_files = [f for f in all_files if not fnmatch.filter(f, '*derivatives*')]
    

    # bidsignored files
    bidsignored_files = []
    for line in bidsignore_content:
        if line.endswith('/'):
            line += '**'
        elif dataset.joinpath(line).is_dir():
            line += '/**'

        bidsignored_files = bidsignored_files +  fnmatch.filter(all_files, line)

    # make a set just to be sure
    bidsignored_files = list(set(bidsignored_files))

    # we also need to collect the hidden files and folders to get an accurate size compared 
    # to the output of the validator
    hidden_paths = {}
    total_hidden_size_in_bytes = 0
    for hidden in fnmatch.filter(os.listdir(dataset), '.*'):
        if pathlib.Path(hidden).is_file():
            try:
                hidden_size = os.stat(hidden).st_size
                hidden_paths[str(hidden)] = hidden_size
                total_hidden_size_in_bytes += hidden_size
            except FileNotFoundError:
                hidden_paths[hidden] = 0
        else:
            for root, folders, files in os.walk(dataset / hidden):
                for hidden_file in files:
                    hidden_path = pathlib.Path(root) / pathlib.Path(hidden_file)
                    if '.git/annex' not in str(hidden_path):
                        path_str = str(hidden_path.relative_to(dataset))
                        hidden_paths[path_str] = hidden_path.stat().st_size
                        total_hidden_size_in_bytes += hidden_path.stat().st_size


    return_dict = {}
    total_size_in_bytes = 0
    for ignored in bidsignored_files:
        full_path = dataset / pathlib.Path(ignored)
        path_str = str(full_path.relative_to(dataset))

        if full_path.is_file():
            try:
                the_size = full_path.stat().st_size
                return_dict[path_str] = the_size
                total_size_in_bytes += the_size

            except FileNotFoundError:
                return_dict[path_str] = 0

        else:
            for root, folders, files in os.walk(full_path):
                for the_file in files:
                    the_path = pathlib.Path(root) / pathlib.Path(the_file)
                    return_dict[str(the_path.relative_to(dataset))] = the_path.stat().st_size
                    total_size_in_bytes += the_path.stat().st_size


    final_dict =  {
            'ignored': {
                '.bidsignore': bidsignore_content, 
                'files': return_dict, 
                'size': total_size_in_bytes
                }, 
            'hidden': {
                'hidden_paths': fnmatch.filter(os.listdir(dataset), '.*'), 
                'files': hidden_paths, 
                'size': total_hidden_size_in_bytes
                }
            }

    return final_dict
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Collects list of files, folders, and their sizes corresponding to the contents of a bidsignore file.")
    parser.add_argument("dataset", help="path to dataset to check.")
    args = parser.parse_args()
    results = do_the_thing(args.dataset)
    if results == {}:
        print(f"No .bidsignore found at {args.dataset}")
    else:
        print(json.dumps(results, indent=4))

