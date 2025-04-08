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

#include "velox/python/file/PyFile.h"
#include <fmt/format.h>
#include <folly/String.h>
#include "velox/dwio/common/ReaderFactory.h"

namespace facebook::velox::py {
namespace {

dwio::common::FileFormat toFileFormat(std::string formatString) {
  folly::toLowerAscii(formatString);
  auto format = dwio::common::toFileFormat(formatString);

  if (format == dwio::common::FileFormat::UNKNOWN) {
    throw std::runtime_error(
        fmt::format("Unknown file format: {}", formatString));
  }
  return format;
}

} // namespace

PyFile::PyFile(std::string filePath, std::string formatString)
    : filePath_(std::move(filePath)),
      fileFormat_(toFileFormat(std::move(formatString))) {}

std::string PyFile::toString() const {
  return fmt::format("{} ({})", filePath_, fileFormat_);
}

PyType PyFile::getSchema() {
  // If the schema was read yet, we will need to open the file and read its
  // metadata.
  if (fileSchema_ == nullptr) {
    // Create ephemeral memory pool to open and read metadata from the file.
    auto rootPool = memory::memoryManager()->addRootPool();
    auto leafPool = rootPool->addLeafChild("py_get_file_schema");

    auto readFile = filesystems::getFileSystem(filePath_, nullptr)
                        ->openFileForRead(filePath_);
    auto input = std::make_unique<dwio::common::BufferedInput>(
        std::shared_ptr<ReadFile>(std::move(readFile)), *leafPool);
    auto reader =
        dwio::common::getReaderFactory(fileFormat_)
            ->createReader(
                std::move(input), dwio::common::ReaderOptions{leafPool.get()});
    fileSchema_ = reader->rowType();
  }
  return PyType{fileSchema_};
}

} // namespace facebook::velox::py
