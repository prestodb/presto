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
import re
from pprint import pprint
from typing import Optional, Tuple

import benchalerts.pipeline_steps as steps
import requests
from benchalerts import AlertPipeline
from benchclients import log

DESCRIPTION = """
Analyze benchmark runs, post a GitHub Check whether there were regressions or not, and
if it's a merge-commit, post a comment back to the PR that merged the commit in.

Required environment variables:
    GITHUB_REPOSITORY - the default GITHUB_REPOSITORY given in GitHub Actions
    GITHUB_APP_ID - the ID of a GitHub App installed to this repo
    GITHUB_APP_PRIVATE_KEY - the private key file contents of the GitHub App
    CONBENCH_URL - the URL to the Conbench server where benchmark results are stored
"""


def parse_args() -> Tuple[str, str, str, float]:
    parser = argparse.ArgumentParser(
        description=DESCRIPTION, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--contender-sha",
        type=str,
        required=True,
        help="The SHA hash of the contender commit. The script will download benchmark "
        "information about this commit and post a GitHub Check of the full report to "
        "this commit.",
    )
    parser.add_argument(
        "--merge-commit-message",
        type=str,
        required=True,
        help="If the condender commit is a merge-commit, its message. If not, an empty "
        "string.",
    )
    parser.add_argument(
        "--z-score-threshold",
        type=float,
        required=True,
        help="The (positive) z-score threshold. Benchmarks with a z-score more extreme "
        "than this threshold will be marked as regressions.",
    )
    args = parser.parse_args()

    repo = os.environ["GITHUB_REPOSITORY"]

    return (
        repo,
        args.contender_sha,
        args.merge_commit_message,
        args.z_score_threshold,
    )


def _merge_commit_pr_number(commit_message: str) -> Optional[int]:
    """If this is a merge-commit run, get the number of the PR that was merged by
    grepping the commit message.

    Other alternatives to this strategy:

    - Hit /repos/{owner}/{repo}/commits/{commit_sha}/pulls to get the number. This
      doesn't work because of the way facebook-github-bot merges PRs.
    - Use the GitHub GraphQL API to search through recent "PR closed" events until you
      find the one that's associated with this commit. This does work but actually feels
      *more* likely to break than grepping.
    """
    if not commit_message:
        print("Merge-commit message not given; this must be a PR commit")
        return None

    # Look for e.g. (#123) at the end of the first line
    res = re.search(r"\(#(\d+)\)$", commit_message.split("\n")[0])

    if not res:
        print("Could not find the PR number in the following merge-commit message:")
        print(commit_message)
        return None

    print(f"Found a PR number (in the given merge-commit message): {repr(res[1])}")
    return int(res[1])


def main(
    repo: str,
    contender_sha: str,
    merge_commit_message: str,
    z_score_threshold: float,
):
    log.setLevel("DEBUG")
    pipeline_steps = [
        steps.GetConbenchZComparisonStep(
            commit_hash=contender_sha, z_score_threshold=z_score_threshold
        ),
        steps.GitHubCheckStep(repo=repo),
    ]

    # Only post a comment if this arg is non-empty (i.e. on merge-commits)
    if merge_commit_message:
        pr_number = _merge_commit_pr_number(merge_commit_message)
        pipeline_steps.append(
            steps.GitHubPRCommentAboutCheckStep(pr_number=pr_number, repo=repo)
        )

    pipeline = AlertPipeline(steps=pipeline_steps)
    pprint(pipeline.run_pipeline())


def test_grepping():
    """This is never called. Use for local dev."""
    _merge_commit_pr_number("")
    for commit in requests.get(
        "https://api.github.com/repos/facebookincubator/velox/commits"
    ).json():
        print(commit["sha"][:7])
        _merge_commit_pr_number(commit["commit"]["message"])


if __name__ == "__main__":
    repo, contender_sha, merge_commit_message, z_score_threshold = parse_args()
    main(
        repo=repo,
        contender_sha=contender_sha,
        merge_commit_message=merge_commit_message,
        z_score_threshold=z_score_threshold,
    )
