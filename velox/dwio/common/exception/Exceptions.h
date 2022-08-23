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

#include <cstdarg>
#include <exception>
#include <stdexcept>
#include <string>
#include "velox/dwio/common/exception/Exception.h"

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

class NotImplementedYet : public std::logic_error {
 public:
  explicit NotImplementedYet(const std::string& what_arg)
      : logic_error(what_arg) {}

  explicit NotImplementedYet(const char* what_arg) : logic_error(what_arg) {}

  ~NotImplementedYet() noexcept override = default;

  NotImplementedYet(const NotImplementedYet& what_arg)
      : logic_error(what_arg) {}

 private:
  NotImplementedYet& operator=(const NotImplementedYet&);
};

class StatsError : public std::runtime_error {
 public:
  explicit StatsError(const std::string& what_arg) : runtime_error(what_arg) {}

  explicit StatsError(const char* what_arg) : runtime_error(what_arg) {}

  ~StatsError() noexcept override = default;

  StatsError(const StatsError& what_arg) : runtime_error(what_arg) {}

 private:
  StatsError& operator=(const StatsError&);
};

class ParseError : public std::runtime_error {
 public:
  explicit ParseError(const std::string& what_arg) : runtime_error(what_arg) {}

  explicit ParseError(const char* what_arg) : runtime_error(what_arg) {}

  ~ParseError() noexcept override = default;

  ParseError(const ParseError& what_arg) : runtime_error(what_arg) {}

 private:
  ParseError& operator=(const ParseError&);
};

class EOFError : public std::runtime_error {
 public:
  explicit EOFError(const std::string& what_arg) : runtime_error(what_arg) {}

  explicit EOFError(const char* what_arg) : runtime_error(what_arg) {}

  ~EOFError() noexcept override = default;

  EOFError(const EOFError& what_arg) : runtime_error(what_arg) {}

 private:
  EOFError& operator=(const EOFError&);
};

void verify_range(uint64_t v, uint64_t rangeMask);

void verify(bool c, std::string fmt...);

void corrupt(std::string fmt...);

std::string error_string(std::string fmt, va_list ap);
std::string format_error_string(std::string fmt...);

template <typename E, typename Enable = void>
class exception_error;

template <class E>
class exception_error<
    E,
    typename std::enable_if_t<std::is_base_of_v<std::exception, E>>> {
 public:
  explicit exception_error(std::string fmt...) {
    va_list ap;
    va_start(ap, fmt);
    auto s = error_string(fmt, ap);
    va_end(ap);
    throw E(s);
  }
};

using not_implemented_yet = exception_error<NotImplementedYet>;
using parse_error = exception_error<ParseError>;
using logic_error = exception_error<std::logic_error>;
using runtime_error = exception_error<std::runtime_error>;
using EOF_error = exception_error<EOFError>;

} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
