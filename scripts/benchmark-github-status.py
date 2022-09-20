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

import argparse
import os

from benchalerts import update_github_status_based_on_regressions


description = """
Analyze benchmark runs and post a GitHub Status whether there was a regression or not.

Required environment variables:
    GITHUB_API_TOKEN - a GitHub PAT with "repo:status" permission
    CONBENCH_URL - the URL to the Conbench server where benchmark results are stored

(see https://circleci.com/docs/env-vars#built-in-environment-variables for these)
    CIRCLE_SHA1
    CIRCLE_PROJECT_USERNAME
    CIRCLE_PROJECT_REPONAME
    CIRCLE_BUILD_URL
"""
parser = argparse.ArgumentParser(
    description=description, formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument(
    "--z-score-threshold",
    type=int,
    required=True,
    help="The (positive) z-score threshold. Benchmarks with a z-score more extreme "
    "than this threshold will be marked as regressions.",
)
args = parser.parse_args()


contender_sha = os.environ["CIRCLE_SHA1"]
org = os.environ["CIRCLE_PROJECT_USERNAME"]
repo = os.environ["CIRCLE_PROJECT_REPONAME"]
os.environ["BUILD_URL"] = os.environ["CIRCLE_BUILD_URL"]

res = update_github_status_based_on_regressions(
    contender_sha=contender_sha,
    z_score_threshold=args.z_score_threshold,
    repo=f"{org}/{repo}",
)
print(res)
