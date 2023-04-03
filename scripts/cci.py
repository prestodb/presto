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
import sys
import argparse
import util

CIRCLECI_V2 = "https://circleci.com/api/v2"
CIRCLECI_V1 = "https://circleci.com/api/v1.1"


def get_circleci_token():
    token = os.getenv("CIRCLECI_API_TOKEN")

    if token is None:
        print(
            """
            CIRCLECI_API_TOKEN is not set in the environment.  An API key is
            required to access CircleCi.  You can create an API token here :

                https://circleci.com/account/api

            Then set this value in your .profile or .bashrc.

                export CIRCLECI_API_TOKEN="Your Token Value Here"
""",
            file=sys.stderr,
        )

        sys.exit(1)

    return token


def circleci_url(token, url, compressed=False):
    return util.run(
        f"curl -s --header 'Circle-Token: {token}' \
       --header 'Accept: application/json'    \
       --header 'Content-Type: application/json' \
       '{url}'",
        compressed=compressed,
    )[1]


def get_pr_workflows(token, repo, pr):
    url = f"{CIRCLECI_V2}/insights/gh/{repo}/workflows/dist-compile?branch=pull/{pr}"

    return circleci_url(token, url).json()


def get_workflow_jobs(token, id):
    url = f"{CIRCLECI_V2}/workflow/{id}/job"

    return circleci_url(token, url).json()


def get_job_steps(token, repo, job):
    url = f"{CIRCLECI_V1}/project/gh/{repo}/{job}"

    return circleci_url(token, url).json()


def get_action_output(token, url):
    return circleci_url(token, url, compressed=True).json()


def output(args):
    github_upstream = util.run("git remote get-url --push upstream")[1].extract(
        r".*:(.*)[.]git"
    )
    github_user = util.run("git remote get-url --push origin")[1].extract(r".*:(.*)/")
    git_branch = util.run("git symbolic-ref --short HEAD")[1]

    if args.pr:
        pull_request = args.pr
    else:
        stdout = util.run(f"hub pr list -h {github_user}:{git_branch}")[1]
        if stdout == "":
            print(f"Error: no pull request for {github_user}:{git_branch}")
            sys.exit(1)

        pull_request = stdout.extract(r".*#([0-9]+) .*")

    if args.token is None:
        token = get_circleci_token()
    else:
        token = args.token

    pr_workflows = get_pr_workflows(token, github_upstream, pull_request)
    latest_workflow = sorted(
        pr_workflows["items"], key=lambda x: x.created_at, reverse=True
    )[0]
    latest_workflow_id = latest_workflow.id

    workflow_jobs = get_workflow_jobs(token, latest_workflow_id)

    for job in workflow_jobs["items"]:
        if job.status != args.job_status or args.job_name not in job.name:
            continue

        steps = get_job_steps(token, github_upstream, job.job_number).steps
        for step in steps:
            if args.step_name not in step.name.lower():
                continue

            for action in step.actions:
                if (args.step_status == "failed" and action.failed) or (
                    args.step_status != "failed" and action.failed is None
                ):
                    for item in get_action_output(token, action.output_url):
                        print(item.message)

    return 0


def help(args):
    parser.print_help()

    return 0


def parse_args():
    global parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="""CircleCi utility

    Output subcommand:

    To obtain the output from failiing build jobs for the PR associated with
    the current checkout:

    > cci.py output

    Use the ----job-name and --step-name option to obtain output from failing
    'check' jobs:

    > cci.py output --job-name check --step-name check

    Specify a particular job name for obtain output from a single job:

    > ./scripts/cci.py output --job-name format-check --step-name check

""",
    )
    parser.add_argument("--token", help="CircleCi API token")

    command = parser.add_subparsers(dest="command")
    output_command_parser = command.add_parser("output")
    output_command_parser.add_argument("--pr")
    output_command_parser.add_argument("--job-name", default="build")
    output_command_parser.add_argument("--job-status", default="failed")
    output_command_parser.add_argument("--step-name", default="build")
    output_command_parser.add_argument("--step-status", default="failed")

    parser.set_defaults(command="help")

    return parser.parse_args()


def main():
    args = parse_args()
    return globals()[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
