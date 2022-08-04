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

import datetime
import functools
import json
import logging
import os
import subprocess
from typing import Any, Dict, List, Optional, Tuple

import conbench.runner

logging.basicConfig(format="%(levelname)s: %(message)s")


def _now_formatted() -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.isoformat()


def github_info(velox_info: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "repository": "https://github.com/facebookincubator/velox",
        "commit": velox_info["velox_git_revision"],
    }


def velox_info() -> Dict[str, Any]:
    # TODO: undo the hard coding with ways to get the values from velox
    # c.f. https://github.com/ursacomputing/benchmarks/blob/033eee0951adbf41931a2de95caccbac887da6ff/benchmarks/_benchmark.py#L30-L48
    return {
        "velox_version": "0.0.1",
        "velox_compiler_id": None,
        "velox_compiler_version": None,
        "velox_compiler_flags": None,
        # This should work, though it would be much better to get the sha from velox directly so we know we're using the right one.
        "velox_git_revision": subprocess.check_output(["git", "rev-parse", "HEAD"])
        .decode("ascii")
        .strip(),
    }


class Benchmark(conbench.runner.Benchmark):
    arguments = []
    options = {"cpu_count": {"type": int}}

    def __init__(self):
        super().__init__()

    @functools.cached_property
    def velox_info(self) -> Dict[str, Any]:
        return velox_info()

    @functools.cached_property
    def velox_info_r(self) -> Dict[str, Any]:
        return self._get_velox_info_r()

    @functools.cached_property
    def github_info(self) -> Dict[str, Any]:
        return github_info(self.velox_info)

    def benchmark(
        self,
        f,
        extra_tags: Dict[str, Any],
        options: Dict[str, Any],
        case: Optional[tuple] = None,
    ):
        cpu_count = options.get("cpu_count", None)
        if cpu_count is not None:
            # TODO: how to set multithreading? Not needed for cpp micro, but might be for macrobenchmarks
            raise NotImplementedError()
        tags, info, context = self._get_tags_info_context(case, extra_tags)
        try:
            benchmark, output = self.conbench.benchmark(
                f,
                self.name,
                tags=tags,
                info=info,
                context=context,
                github=self.github_info,
                options=options,
                publish=os.environ.get("DRY_RUN") is None,
            )
        except Exception as e:
            benchmark, output = self._handle_error(e, self.name, tags, info, context)
        return benchmark, output

    def record(
        self,
        result,
        extra_tags: Dict[str, Any],
        extra_info,
        extra_context,
        options: Dict[str, Any],
        case: Optional[tuple] = None,
        name: Optional[str] = None,
        output=None,
    ):
        if name is None:
            name = self.name
        tags, info, context = self._get_tags_info_context(case, extra_tags)
        info.update(**extra_info)
        context.update(**extra_context)

        benchmark, output = self.conbench.record(
            result,
            name,
            tags=tags,
            info=info,
            context=context,
            github=self.github_info,
            options=options,
            output=output,
            publish=os.environ.get("DRY_RUN") is None,
        )
        return benchmark, output

    def execute_command(self, command):
        try:
            print(command)
            result = subprocess.run(command, check=True)
        except subprocess.CalledProcessError as e:
            print(e.stderr.decode("utf-8"))
            raise e

    def version_case(self, case: tuple) -> Optional[int]:
        """
        A method that takes a case and returns a version int

        :param case: A tuple with a value for each argument of the benchmark's parameters

        Overwrite this method to version cases. Incrementing versions will break case history
        because `case_version` will be appended to tags, which are considered in determining
        history. Returning `None` corresponds to no versioning.
        """
        return None

    def get_tags(
        self, options: Dict[str, Any], source: Optional[list] = None
    ) -> Dict[str, Any]:
        cpu_tag = {"cpu_count": options.get("cpu_count", None)}
        if source:
            return {**source.tags, **cpu_tag}
        else:
            return cpu_tag

    def _get_tags_info_context(
        self, case: tuple, extra_tags: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        info = {**self.velox_info}
        info.pop("velox_git_revision")

        context = {"velox_compiler_flags": info.pop("velox_compiler_flags")}

        tags = {**extra_tags}

        if case:
            case_tags = dict(zip(self.fields, case))
            for key in case_tags:
                if key not in tags:
                    tags[key] = case_tags[key]

            case_version = self.version_case(case)
            if case_version:
                tags["case_version"] = case_version

        return tags, info, context

    def _handle_error(
        self,
        e: Exception,
        name: str,
        tags: Dict[str, Any],
        info: Dict[str, Any],
        context: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], None]:
        output = None
        tags["name"] = name
        error = {
            "timestamp": _now_formatted(),
            "tags": tags,
            "info": info,
            "context": context,
            "error": str(e),
        }
        logging.exception(json.dumps(error))
        return error, output


@conbench.runner.register_list
class BenchmarkList(conbench.runner.BenchmarkList):
    def list(self, classes: Dict[str, Benchmark]) -> List[Benchmark]:
        """List of benchmarks to run for all cases & all sources."""

        def add(
            benchmarks, parts: List[str], flags: Dict[str, Any], exclude: List[str]
        ) -> None:
            command = " ".join(parts)
            if command not in exclude:
                benchmarks.append({"command": command, "flags": flags})

        benchmarks = []

        for name, benchmark in classes.items():
            if name.startswith("example"):
                continue

            instance, parts = benchmark(), [name]

            exclude = getattr(benchmark, "exclude", [])
            if "source" in getattr(benchmark, "arguments", []):
                parts.append("ALL")

            iterations = getattr(instance, "iterations", 3)
            if iterations:
                parts.append(f"--iterations={iterations}")

            if instance.cases:
                parts.append("--all=true")

            flags = getattr(instance, "flags", {})

            add(benchmarks, parts, flags, exclude)

        return sorted(benchmarks, key=lambda k: k["command"])
