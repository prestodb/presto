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

#include <chrono>

#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/common/file/tests/FaultyFile.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/Closeable.h"
#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/MetricsLog.h"

namespace facebook::velox::dwio::common {
using namespace facebook::velox::io;

class FaultyFileSink : public LocalFileSink {
 public:
  FaultyFileSink(const std::string& faultyFilePath, const Options& options);

 private:
  const std::string faultyFilePath_;
};

void registerFaultyFileSinks();

} // namespace facebook::velox::dwio::common
