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

#include <boost/filesystem.hpp>
#include <fmt/format.h>
#include <algorithm>
#include <filesystem>
#include <random>

namespace facebook {
namespace velox {
namespace codegen {
namespace compiler_utils {
namespace filesystem {

/// Generates and manage temps filee
/// TODO: Track files and cleanup after process closes
class PathGenerator {
 public:
  PathGenerator() : rootTempFolder_(std::filesystem::temp_directory_path()) {}

  std::filesystem::path tempPath(
      const std::filesystem::path& rootDir,
      const std::string& prefix,
      const std::string& extension) {
    fmt::memory_buffer stringBuffer;

    fmt::format_to(
        stringBuffer,
        (rootDir / fileNameFormat).string(),
        fmt::arg("prefix", prefix),
        fmt::arg("ext", extension));

    // mkstemps expects a null terminated string.
    stringBuffer.push_back('\0');

    int returnValue = mkstemps(stringBuffer.begin(), extension.length());
    int errorCode = errno;
    if (returnValue == -1) {
      char buffer[1024];
      strerror_r(errorCode, buffer, 1024);
      throw std::logic_error(fmt::format(
          "Creating temp file failed with error code {}, and error message {}",
          errorCode,
          buffer));
    };
    return std::filesystem::path(stringBuffer.begin());
  }

  std::filesystem::path tempPath(
      const std::string& prefix,
      const std::string& extension) {
    return tempPath(rootTempFolder_, prefix, extension);
  }

 private:
  std::filesystem::path rootTempFolder_;
  const std::string fileNameFormat = "{prefix}XXXXXX{ext}";
};

} // namespace filesystem
} // namespace compiler_utils
} // namespace codegen
} // namespace velox
}; // namespace facebook
