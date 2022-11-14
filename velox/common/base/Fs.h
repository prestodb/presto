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

#include <optional>

#if __has_include("filesystem")
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

namespace facebook::velox::common {

/// Generates a file directory on local file system specified by 'dirPath'. The
/// generation will be recursive. Non-exist parent directories will also be
/// created. Returns true if creation is successful, false otherwise. Error
/// message will be printed if creation is unsuccessful, but the created
/// directories will not be removed on failure.
bool generateFileDirectory(const char* dirPath);

/// Creates a file with a generated file name in provided 'basePath'. The
/// generated file will have random chars in the file name to avoid duplication.
/// The full path of the file will be of the pattern
/// {basePath}/velox_{prefix}_XXXXXX where 'XXXXXX' is the randomly generated
/// chars. A nullopt will be returned if file creation fails.
std::optional<std::string> generateTempFilePath(
    const char* basePath,
    const char* prefix);

/// Creates a directory with a generated directory name in provided 'basePath'.
/// The generated directory will have random chars in it to avoid duplication.
/// The full path of the directory will be of the pattern
/// {basePath}/velox_{prefix}_XXXXXX where 'XXXXXX' is the randomly generated
/// chars. A nullopt will be returned if directory creation fails.
std::optional<std::string> generateTempFolderPath(
    const char* basePath,
    const char* prefix);
} // namespace facebook::velox::common
