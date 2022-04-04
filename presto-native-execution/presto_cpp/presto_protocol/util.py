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

import gzip
import json
import os
import subprocess
import sys

import regex
import yaml


class attrdict(dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


class string(str):
    def extract(self, rexp):
        return regex.match(rexp, self).group(1)

    def json(self):
        return json.loads(self, object_hook=attrdict)


def run(command, compressed=False, **kwargs):
    if "input" in kwargs:
        input = kwargs["input"]

        if type(input) == list:
            input = "\n".join(input) + "\n"

        kwargs["input"] = input.encode("utf-8")

    reply = subprocess.run(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
    )

    if compressed:
        stdout = gzip.decompress(reply.stdout)
    else:
        stdout = reply.stdout

    stdout = (
        string(stdout.decode("utf-8", errors="ignore").strip())
        if stdout is not None
        else ""
    )
    stderr = (
        string(reply.stderr.decode("utf-8").strip()) if reply.stderr is not None else ""
    )

    if stderr != "":
        print(stderr, file=sys.stderr)

    return reply.returncode, stdout, stderr


def get_filename(filename):
    return os.path.basename(filename)


def get_fileextn(filename):
    split = os.path.splitext(filename)
    if len(split) <= 1:
        return ""

    return split[-1]


def script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))


def input_files(files):
    if len(files) == 1 and files[0] == "-":
        return [file.strip() for file in sys.stdin.readlines()]
    else:
        return files


def file_read(filename):
    with open(filename) as file:
        return file.read()


def load_yaml(filename):
    return string(json.dumps(yaml.load(file_read(filename), Loader=yaml.Loader))).json()


class ExtendedJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)

        return None


def to_json(obj):
    return json.dumps(obj, cls=ExtendedJsonEncoder)
