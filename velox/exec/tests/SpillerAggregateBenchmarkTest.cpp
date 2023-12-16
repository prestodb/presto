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

#include "velox/exec/tests/AggregateSpillBenchmarkBase.h"
#include "velox/serializers/PrestoSerializer.h"

#include <gflags/gflags.h>

using namespace facebook::velox;
using namespace facebook::velox::exec;

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memory::MemoryManager::initialize({});
  serializer::presto::PrestoVectorSerde::registerVectorSerde();
  filesystems::registerLocalFileSystem();

  auto spillerTypeName = FLAGS_spiller_benchmark_spiller_type;
  std::transform(
      spillerTypeName.begin(),
      spillerTypeName.end(),
      spillerTypeName.begin(),
      [](unsigned char c) { return std::toupper(c); });
  Spiller::Type spillerType;
  if (spillerTypeName == Spiller::typeName(Spiller::Type::kAggregateInput)) {
    spillerType = Spiller::Type::kAggregateInput;
  } else if (
      spillerTypeName == Spiller::typeName(Spiller::Type::kAggregateOutput)) {
    spillerType = Spiller::Type::kAggregateOutput;
  } else {
    VELOX_UNSUPPORTED(
        "The spiller type {} is not one of [AGGREGATE_INPUT, AGGREGATE_OUTPUT], the aggregate spiller dose not support it.",
        spillerTypeName);
  }
  auto test = std::make_unique<test::AggregateSpillBenchmarkBase>(spillerType);
  test->setUp();
  test->run();
  test->printStats();
  test->cleanup();

  return 0;
}
