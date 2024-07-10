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

#include "velox/dwio/common/exception/Exceptions.h"

#include <folly/Likely.h>

#include <array>
#include <cstdio>
#include <string>

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

void verify_range(uint64_t v, uint64_t rangeMask) {
  auto mv = (v & rangeMask);
  if (UNLIKELY(mv != 0 && mv != rangeMask)) {
    corrupt("Integer data out of range");
  }
}

void verify(bool c, std::string fmt...) {
  if (UNLIKELY(!c)) {
    va_list ap;
    va_start(ap, fmt);
    auto s = error_string(fmt, ap);
    va_end(ap);
    throw ParseError(s);
  }
}

[[noreturn]] void corrupt(std::string fmt...) {
  va_list ap;
  va_start(ap, fmt);
  auto s = error_string(fmt, ap);
  va_end(ap);
  throw ParseError(s);
}

std::string error_string(std::string fmt, va_list ap) {
  std::array<char, 1024> buf;
  vsnprintf(buf.data(), buf.size(), fmt.data(), ap);
  buf[buf.size() - 1] = 0;
  return std::string(buf.data());
}

std::string format_error_string(std::string fmt...) {
  va_list ap;
  va_start(ap, fmt);
  auto s = error_string(fmt, ap);
  va_end(ap);
  return s;
}

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
