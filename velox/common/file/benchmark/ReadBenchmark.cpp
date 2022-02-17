/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/common/file/benchmark/ReadBenchmark.h"

DEFINE_string(path, "", "Path of test file");
DEFINE_int64(
    file_size_gb,
    0,
    "Limits the test to the first --file_size_gb "
    "of --path. 0 means use the whole file");
DEFINE_int32(num_threads, 16, "Test paralelism");
DEFINE_int32(seed, 0, "Random seed, 0 means no seed");
DEFINE_bool(odirect, false, "Use O_DIRECT");

DEFINE_int32(
    bytes,
    0,
    "If 0, runs through a set of predefined read patterns. "
    "If non-0, this is the size of a single read. The reads are "
    "made in --num_in_run consecutive batchhes with --gap bytes between each read");
DEFINE_int32(gap, 0, "Gap between consecutive reads if --bytes is non-0");
DEFINE_int32(
    num_in_run,
    10,
    "Number of consecutive reads of --bytes separated by --gap bytes");
DEFINE_int32(
    measurement_size,
    100 << 20,
    "Total reads per thread when throughput for a --bytes/--gap/--/gap/"
    "--num_in_run combination");

namespace {
static bool notEmpty(const char* /*flagName*/, const std::string& value) {
  return !value.empty();
}
} // namespace

DEFINE_validator(path, &notEmpty);

namespace facebook::velox {

void ReadBenchmark::run() {
  if (FLAGS_bytes) {
    modes(FLAGS_bytes, FLAGS_gap, FLAGS_num_in_run);
    return;
  }
  modes(1100, 0, 10);
  modes(1100, 1200, 10);
  modes(16 * 1024, 0, 10);
  modes(16 * 1024, 10000, 10);
  modes(1000000, 0, 8);
  modes(1000000, 100000, 8);
}
} // namespace facebook::velox
