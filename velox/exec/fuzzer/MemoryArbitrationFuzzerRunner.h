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
#pragma once

#include <gtest/gtest.h>

#include "velox/common/file/FileSystems.h"

#include "velox/exec/fuzzer/MemoryArbitrationFuzzer.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/serializers/PrestoSerializer.h"

namespace facebook::velox::exec::test {

class MemoryArbitrationFuzzerRunner {
 public:
  static int run(size_t seed) {
    serializer::presto::PrestoVectorSerde::registerVectorSerde();
    filesystems::registerLocalFileSystem();
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    memoryArbitrationFuzzer(seed);
    return RUN_ALL_TESTS();
  }
};

} // namespace facebook::velox::exec::test
