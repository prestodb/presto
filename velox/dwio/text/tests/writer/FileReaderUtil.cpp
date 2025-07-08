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

#include "velox/dwio/text/tests/writer/FileReaderUtil.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::velox::text {

using dwio::common::SerDeOptions;

uint64_t readFile(const std::string& path, const std::string& name) {
  const auto fs = filesystems::getFileSystem(path, nullptr);
  auto filepath = fs::path(fmt::format("{}/{}", path, name));
  const auto& file = fs->openFileForRead(filepath.string());

  return file->size();
}

std::vector<std::vector<std::string>> parseTextFile(
    const std::string& path,
    const std::string& name,
    void* buffer,
    SerDeOptions serDeOptions) {
  const auto fs = filesystems::getFileSystem(path, nullptr);
  auto filepath = fs::path(fmt::format("{}/{}", path, name));
  const auto& file = fs->openFileForRead(filepath.string());

  std::string line;
  std::vector<std::vector<std::string>> table;

  auto fileSize = file->size();
  if (fileSize > 0) {
    file->pread(0, fileSize, buffer);
    std::string content(static_cast<char*>(buffer), fileSize);

    std::istringstream stream(content);
    while (std::getline(stream, line)) {
      std::vector<std::string> row =
          splitTextLine(line, serDeOptions.separators[0]);
      table.push_back(row);
    }
  }

  return table;
}

std::vector<std::string> splitTextLine(const std::string& str, char delimiter) {
  std::vector<std::string> result;
  std::size_t start = 0;
  std::size_t end = str.find(delimiter);

  while (end != std::string::npos) {
    result.push_back(str.substr(start, end - start));
    start = end + 1;
    end = str.find(delimiter, start);
  }

  result.push_back(str.substr(start)); // Add the last part
  return result;
}
} // namespace facebook::velox::text
