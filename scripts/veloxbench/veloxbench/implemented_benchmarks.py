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

import json
import os
import tempfile

import conbench.runner

from .cpp_micro_benchmarks import LocalCppMicroBenchmarks
from ._benchmark import Benchmark


@conbench.runner.register_benchmark
class RecordCppMicroBenchmarks(LocalCppMicroBenchmarks, Benchmark):
    def run(self, **kwargs):
        with tempfile.TemporaryDirectory() as result_dir:
            # run benchmarks and save to a tempdir
            super().run(result_dir=result_dir, **kwargs)

            # iterate through files to make the suites
            with os.scandir(result_dir) as result_files:
                for result_file in result_files:
                    suite = result_file.name.replace(".json", "", 1)
                    with open(result_file.path, "r") as f:
                        results = json.load(f)
                    self.conbench.mark_new_batch()
                    for result in results:
                        # Folly benchmark exports line separators by mistake as
                        # an entry in the json file.
                        if result[1] == "-":
                            continue
                        yield self._record_result(suite, result, kwargs)

    def _record_result(self, suite, result, options):
        info, context = {}, {"benchmark_language": "C++"}
        # TODO: we should really name these in the json and not rely on position!
        tags = self._parse_benchmark_name(result[1])
        name = tags.pop("name")
        tags["suite"] = suite
        tags["source"] = self.name

        values = self._get_values(result)

        return self.record(
            values,
            tags,
            info,
            context,
            options=options,
            output=result,
            name=name,
        )
