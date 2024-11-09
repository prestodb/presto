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

#include "velox/dwio/common/tests/FaultyFileSink.h"
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwio::common {
namespace {
using tests::utils::FaultyFileSystem;

std::unique_ptr<FileSink> createFaultyFileSink(
    const std::string& filePath,
    const FileSink::Options& options) {
  if (filePath.find("faulty:") == 0) {
    return std::make_unique<FaultyFileSink>(filePath, options);
  }
  return nullptr;
}
} // namespace

FaultyFileSink::FaultyFileSink(
    const std::string& faultyFilePath,
    const Options& options)
    : LocalFileSink{faultyFilePath.substr(7), options, false},
      faultyFilePath_(faultyFilePath) {
  auto fs = filesystems::getFileSystem(faultyFilePath_, nullptr);
  writeFile_ = fs->openFileForWrite(faultyFilePath_);
}

void registerFaultyFileSinks() {
  facebook::velox::dwio::common::FileSink::registerFactory(
      (createFaultyFileSink));
}
} // namespace facebook::velox::dwio::common
