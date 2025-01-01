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

namespace facebook::velox::text {

std::string readFile(const std::string& name) {
  std::ifstream file(name);
  std::string line;

  std::stringstream ss;
  while (std::getline(file, line)) {
    ss << line;
  }
  return ss.str();
}

std::vector<std::vector<std::string>> parseTextFile(const std::string& name) {
  std::ifstream file(name);
  std::string line;
  std::vector<std::vector<std::string>> table;

  while (std::getline(file, line)) {
    std::vector<std::string> row = splitTextLine(line, TextFileTraits::kSOH);
    table.push_back(row);
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
