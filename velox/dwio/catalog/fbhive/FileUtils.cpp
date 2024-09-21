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

#include "FileUtils.h"

#include <bitset>

#include <fmt/core.h>
#include <folly/container/Array.h>

#include "velox/dwio/common/exception/Exception.h"

namespace facebook {
namespace velox {
namespace dwio {
namespace catalog {
namespace fbhive {

namespace {

constexpr size_t HEX_WIDTH = 2;

constexpr auto charsToEscape = folly::make_array(
    '"',
    '#',
    '%',
    '\'',
    '*',
    '/',
    ':',
    '=',
    '?',
    '\\',
    '\x7F',
    '{',
    '[',
    ']',
    '^');

std::bitset<128> buildEscapeMap() {
  std::bitset<128> ret;
  for (size_t i = 1; i < 0x20; ++i) {
    ret.set(i);
  }
  for (auto& c : charsToEscape) {
    ret.set(static_cast<size_t>(c));
  }
  return ret;
}

const std::bitset<128>& getEscapeMap() {
  static auto ret = buildEscapeMap();
  return ret;
}

std::string toLower(const std::string& data) {
  std::string ret;
  ret.reserve(data.size());
  std::transform(
      data.begin(), data.end(), std::back_inserter(ret), [](auto& c) {
        return std::tolower(c);
      });
  return ret;
}

bool shouldEscape(char c) {
  auto& escapeMap = getEscapeMap();
  return c >= 0 && c < escapeMap.size() && escapeMap.test(c);
}

std::vector<std::pair<std::string, std::string>> extractPartitionKeyValues(
    const std::string& filePathSubstr,
    const std::function<void(
        const std::string& partitionPart,
        std::vector<std::pair<std::string, std::string>>& parsedParts)>&
        parserFunc) {
  std::vector<std::string> partitionParts{};
  folly::split('/', filePathSubstr, partitionParts);

  std::vector<std::pair<std::string, std::string>> entries{};
  entries.reserve(partitionParts.size());
  for (const auto& part : partitionParts) {
    parserFunc(part, entries);
  }
  return entries;
}

// Strong assumption that all expressions in the form of a=b means a partition
// key value pair in '/' separated tokens. We could have stricter validation
// on what a file path should look like, but it doesn't belong to this layer.
std::vector<std::pair<std::string, std::string>> extractPartitionKeyValues(
    const std::string& filePath) {
  return extractPartitionKeyValues(
      filePath,
      [](const std::string& partitionPart,
         std::vector<std::pair<std::string, std::string>>& parsedParts) {
        std::vector<std::string> tokens;
        folly::split('=', partitionPart, tokens);
        if (tokens.size() == 2) {
          parsedParts.emplace_back(std::make_pair(
              FileUtils::unescapePathName(tokens[0]),
              FileUtils::unescapePathName(tokens[1])));
        }
      });
}

size_t countEscape(const std::string& val) {
  size_t count = 0;
  for (auto& c : val) {
    if (shouldEscape(c)) {
      ++count;
    }
  }
  return count;
}

} // namespace

std::string FileUtils::escapePathName(const std::string& data) {
  std::string ret;
  ret.reserve(data.size() + countEscape(data) * HEX_WIDTH);
  std::for_each(data.begin(), data.end(), [&](auto& c) {
    if (shouldEscape(c)) {
      ret += fmt::format("%{:02X}", c);
    } else {
      ret += c;
    }
  });
  return ret;
}

std::string FileUtils::unescapePathName(const std::string& data) {
  std::string ret;
  ret.reserve(data.size());
  for (size_t i = 0; i < data.size(); ++i) {
    char c = data[i];
    if (c == '%' && i + HEX_WIDTH < data.size()) {
      std::string tmp{data.data() + i + 1, HEX_WIDTH};
      char* end;
      c = static_cast<char>(std::strtol(tmp.c_str(), &end, 16));
      DWIO_ENSURE(errno != ERANGE && end == tmp.data() + HEX_WIDTH);
      i += HEX_WIDTH;
    }
    ret.append(1, c);
  }
  return ret;
}

std::string FileUtils::makePartName(
    const std::vector<std::pair<std::string, std::string>>& entries,
    bool partitionPathAsLowerCase) {
  size_t size = 0;
  size_t escapeCount = 0;
  std::for_each(entries.begin(), entries.end(), [&](auto& pair) {
    auto keySize = pair.first.size();
    DWIO_ENSURE_GT(keySize, 0);
    size += keySize;
    escapeCount += countEscape(pair.first);

    auto valSize = pair.second.size();
    if (valSize == 0) {
      size += kDefaultPartitionValue.size();
    } else {
      size += valSize;
      escapeCount += countEscape(pair.second);
    }
  });

  std::string ret;
  ret.reserve(size + escapeCount * HEX_WIDTH + entries.size() - 1);

  std::for_each(entries.begin(), entries.end(), [&](auto& pair) {
    if (ret.size() > 0) {
      ret += "/";
    }
    if (partitionPathAsLowerCase) {
      ret += escapePathName(toLower(pair.first));
    } else {
      ret += escapePathName(pair.first);
    }

    ret += "=";
    if (pair.second.size() == 0) {
      ret += kDefaultPartitionValue;
    } else {
      ret += escapePathName(pair.second);
    }
  });

  return ret;
}

std::vector<std::pair<std::string, std::string>> FileUtils::parsePartKeyValues(
    const std::string& partName) {
  std::vector<std::string> parts;
  folly::split('/', partName, parts);
  std::vector<std::pair<std::string, std::string>> ret;
  ret.reserve(parts.size());
  std::for_each(parts.begin(), parts.end(), [&](auto& part) {
    std::vector<std::string> kv;
    folly::split('=', part, kv);
    DWIO_ENSURE_EQ(kv.size(), 2);
    ret.push_back({unescapePathName(kv[0]), unescapePathName(kv[1])});
  });
  return ret;
}

std::string FileUtils::extractPartitionName(const std::string& filePath) {
  const auto& partitionParts = extractPartitionKeyValues(filePath);
  return partitionParts.empty() ? "" : makePartName(partitionParts, false);
}

} // namespace fbhive
} // namespace catalog
} // namespace dwio
} // namespace velox
} // namespace facebook
