#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import subprocess
import argparse
import json
import sys
from pathlib import Path


def is_git_root(path):
    """Check if a directory is a Git root."""
    git_file = Path(os.path.join(path, ".git"))
    return git_file.exists()


def list_contributors(velox_root, since, until):
    # Change directory to Velox root.
    os.chdir(velox_root)

    # Unpack mailmap.
    try:
        subprocess.run(
            ["base64", "-D", "-i", "velox/docs/mailmap_base64", "-o", "./.mailmap"]
        )
    except subprocess.CalledProcessError as e:
        print("Error decoding mailmap_base64:", e)
        sys.exit(-1)

    # Get a list of contributors in the specified range using git log.
    try:
        contributors = subprocess.check_output(
            ["git", "shortlog", "-se", "--since", since, "--until", until], text=True
        )
    except subprocess.CalledProcessError as e:
        print("Error listing git log:", e)
        sys.exit(-1)

    # Load affiliations map.
    try:
        affiliateMap = json.load(open("velox/docs/affiliations_map.txt"))
    except ValueError as e:
        print("Error loading affiliations_map.txt:", e)
        sys.exit(-1)

    # Output entry format is " 1  John <john@abc.com>" or " 1  John <ABC>"
    # Format contributor affiliation from the output.
    unknownAffiliations = []
    for line in contributors.splitlines():
        start = line.find("<") + 1
        end = line.find(">")
        affiliation = line[start:end]
        # Get affiliation from the email-domain and affiliateMap if present.
        if "@" in affiliation:
            domain = affiliation.split("@")[1]
            if domain in affiliateMap:
                affiliation = affiliateMap.get(domain)
            else:
                unknownAffiliations.append(affiliation)
                affiliation = ""
        # Append affiliation if found.
        if affiliation != "":
            print(line[0 : start - 2], "-", affiliation)
        else:
            print(line[0 : start - 2])

    print("Unknown affiliations found: ", unknownAffiliations)

    # Remove .mailmap.
    try:
        subprocess.run(["rm", "./.mailmap"])
    except subprocess.CalledProcessError as e:
        print("Error removing .mailmap:", e)
        sys.exit(-1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", type=str, default="June 01 2024")
    parser.add_argument("--until", type=str, default="June 30 2024")
    parser.add_argument(
        "--path", type=str, help="Velox root directory", default=Path.cwd()
    )
    args = parser.parse_args()

    if not is_git_root(args.path):
        print("Invalid Velox git root path:", args.path)
        sys.exit(-1)

    list_contributors(args.path, args.since, args.until)
    sys.exit()
