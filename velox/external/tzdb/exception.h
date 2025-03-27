// -*- C++ -*-
//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

// For information see https://libcxx.llvm.org/DesignDocs/TimeZone.html

#pragma once

#include <version>

#include <chrono>
#include <sstream>
#include <stdexcept>
#include <string>
#include "velox/external/date/date.h"
#include "velox/external/tzdb/local_info.h"

namespace facebook::velox::tzdb {

class nonexistent_local_time : public std::runtime_error {
 public:
  template <class _Duration>
  nonexistent_local_time(
      const date::local_time<_Duration>& __time,
      const local_info& __info)
      : runtime_error{__create_message(__time, __info)} {
    // [time.zone.exception.nonexist]/2
    //   Preconditions: i.result == local_info::nonexistent is true.
    // The value of __info.result is not used.
    if (__info.result != local_info::nonexistent) {
      throw std::runtime_error(
          "creating an nonexistent_local_time from a local_info that is not non-existent");
    }
  }

  nonexistent_local_time(const nonexistent_local_time&) = default;
  nonexistent_local_time& operator=(const nonexistent_local_time&) = default;

  ~nonexistent_local_time() override; // exported as key function

 private:
  template <class _Duration>
  std::string __create_message(
      const date::local_time<_Duration>& __time,
      const local_info& __info) {
    using namespace facebook::velox::date;

    std::ostringstream os;
    os << __time << " is in a gap between\n"
       << date::local_seconds{__info.first.end.time_since_epoch()} +
            __info.first.offset
       << ' ' << __info.first.abbrev << " and\n"
       << date::local_seconds{__info.second.begin.time_since_epoch()} +
            __info.second.offset
       << ' ' << __info.second.abbrev << " which are both equivalent to\n";
    
    ::facebook::velox::date::operator<<(os, __info.first.end) << " UTC";
    return os.str();
  }
};

template <class _Duration>
[[noreturn]] void __throw_nonexistent_local_time(
    [[maybe_unused]] const date::local_time<_Duration>& __time,
    [[maybe_unused]] const local_info& __info) {
  throw nonexistent_local_time(__time, __info);
}

class ambiguous_local_time : public std::runtime_error {
 public:
  template <class _Duration>
  ambiguous_local_time(
      const date::local_time<_Duration>& __time,
      const local_info& __info)
      : runtime_error{__create_message(__time, __info)} {
    // [time.zone.exception.ambig]/2
    //   Preconditions: i.result == local_info::ambiguous is true.
    // The value of __info.result is not used.
    if (__info.result != local_info::ambiguous) {
      throw std::runtime_error(
          "creating an ambiguous_local_time from a local_info that is not ambiguous");
    }
  }

  ambiguous_local_time(const ambiguous_local_time&) = default;
  ambiguous_local_time& operator=(const ambiguous_local_time&) = default;

  ~ambiguous_local_time() override; // exported as key function

 private:
  template <class _Duration>
  std::string __create_message(
      const date::local_time<_Duration>& __time,
      const local_info& __info) {
    std::ostringstream os;
    using ::facebook::velox::date::operator<<;
    os << __time << " is ambiguous.  It could be\n"
       << __time << ' ' << __info.first.abbrev
       << " == " << __time - __info.first.offset << " UTC or\n"
       << __time << ' ' << __info.second.abbrev
       << " == " << __time - __info.second.offset << " UTC";
    return os.str();
  }
};

template <class _Duration>
[[noreturn]] void __throw_ambiguous_local_time(
    [[maybe_unused]] const date::local_time<_Duration>& __time,
    [[maybe_unused]] const local_info& __info) {
  throw ambiguous_local_time(__time, __info);
}

class invalid_time_zone : public std::runtime_error {
 public:
  invalid_time_zone(
      const std::string_view& __tz_name)
      : runtime_error{__create_message(__tz_name)} {}

  invalid_time_zone(const invalid_time_zone&) = default;
  invalid_time_zone& operator=(const invalid_time_zone&) = default;

  ~invalid_time_zone() override; // exported as key function

 private:
  std::string __create_message(
      const std::string_view& __tz_name) {
    std::ostringstream os;
    os << __tz_name << " not found in timezone database";
    return os.str();
  }
};

[[noreturn]] void __throw_invalid_time_zone(
    [[maybe_unused]] const std::string_view& __tz_name);

} // namespace facebook::velox::tzdb
