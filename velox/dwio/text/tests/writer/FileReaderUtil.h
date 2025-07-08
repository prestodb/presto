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

#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/text/writer/TextWriter.h"

namespace facebook::velox::text {

using dwio::common::SerDeOptions;

uint64_t readFile(const std::string& filepath, const std::string& name);

std::vector<std::vector<std::string>> parseTextFile(
    const std::string& path,
    const std::string& name,
    void* buffer,
    SerDeOptions serDeOptions = SerDeOptions());

std::vector<std::string> splitTextLine(const std::string& str, char delimiter);
} // namespace facebook::velox::text
