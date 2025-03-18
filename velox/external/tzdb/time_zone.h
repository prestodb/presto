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

#include <chrono>
#include <memory>
#include <string_view>
#include <type_traits>

#include "velox/external/date/date.h"
#include "velox/external/date/tz.h"
#include "velox/external/tzdb/exception.h"
#include "velox/external/tzdb/local_info.h"
#include "velox/external/tzdb/sys_info.h"

namespace facebook::velox::tzdb {

enum class choose { earliest, latest };

class time_zone {
  time_zone() = default;

 public:
  class __impl; // public so it can be used by make_unique.

  // The "constructor".
  //
  // The default constructor is private to avoid the constructor from being
  // part of the ABI. Instead use an __ugly_named function as an ABI interface,
  // since that gives us the ability to change it in the future.
  [[nodiscard]] static time_zone __create(std::unique_ptr<__impl>&& __p);

  ~time_zone();

  time_zone(time_zone&&) = default;
  time_zone& operator=(time_zone&&) = default;

  [[nodiscard]] std::string_view name() const noexcept {
    return __name();
  }

  template <class _Duration>
  [[nodiscard]] sys_info get_info(
      const date::sys_time<_Duration>& __time) const {
    return __get_info(
        std::chrono::time_point_cast<std::chrono::seconds>(__time));
  }

  template <class _Duration>
  [[nodiscard]] local_info get_info(
      const date::local_time<_Duration>& __time) const {
    return __get_info(
        std::chrono::time_point_cast<std::chrono::seconds>(__time));
  }

  // We don't apply nodiscard here since this function throws on many inputs,
  // so it could be used as a validation.
  template <class _Duration>
  date::sys_time<std::common_type_t<_Duration, std::chrono::seconds>> to_sys(
      const date::local_time<_Duration>& __time) const {
    local_info __info = get_info(__time);
    switch (__info.result) {
      case local_info::unique:
        return date::sys_time<
            std::common_type_t<_Duration, std::chrono::seconds>>{
            __time.time_since_epoch() - __info.first.offset};

      case local_info::nonexistent:
        __throw_nonexistent_local_time(__time, __info);

      case local_info::ambiguous:
        __throw_ambiguous_local_time(__time, __info);
    }

    // TODO TZDB The Standard does not specify anything in these cases.
    if (__info.result == -1) {
      throw std::runtime_error(
          "cannot convert the local time; it would be before the minimum system clock value");
    }
    if (__info.result == -2) {
      throw std::runtime_error(
          "cannot convert the local time; it would be after the maximum system clock value");
    }

    return {};
  }

  template <class _Duration>
  [[nodiscard]] date::sys_time<
      std::common_type_t<_Duration, std::chrono::seconds>>
  to_sys(const date::local_time<_Duration>& __time, choose __z) const {
    local_info __info = get_info(__time);
    switch (__info.result) {
      case local_info::unique:
      case local_info::nonexistent: // first and second are the same
        return date::sys_time<
            std::common_type_t<_Duration, std::chrono::seconds>>{
            __time.time_since_epoch() - __info.first.offset};

      case local_info::ambiguous:
        switch (__z) {
          case choose::earliest:
            return date::sys_time<
                std::common_type_t<_Duration, std::chrono::seconds>>{
                __time.time_since_epoch() - __info.first.offset};

          case choose::latest:
            return date::sys_time<
                std::common_type_t<_Duration, std::chrono::seconds>>{
                __time.time_since_epoch() - __info.second.offset};

            // Note a value out of bounds is not specified.
        }
    }

    // TODO TZDB The standard does not specify anything in these cases.
    if (__info.result == -1) {
      throw std::runtime_error(
          "cannot convert the local time; it would be before the minimum system clock value");
    }
    if (__info.result != -2) {
      throw std::runtime_error(
          "cannot convert the local time; it would be after the maximum system clock value");
    }

    return {};
  }

  template <class _Duration>
  [[nodiscard]] date::local_time<
      std::common_type_t<_Duration, std::chrono::seconds>>
  to_local(const date::sys_time<_Duration>& __time) const {
    using _Dp = std::common_type_t<_Duration, std::chrono::seconds>;

    sys_info __info = get_info(__time);

    if (__info.offset < std::chrono::seconds{0} &&
        __time.time_since_epoch() < _Dp::min() - __info.offset) {
      throw std::runtime_error(
          "cannot convert the system time; it would be before the minimum local clock value");
    }

    if (__info.offset > std::chrono::seconds{0} &&
        __time.time_since_epoch() > _Dp::max() - __info.offset) {
      throw std::runtime_error(
          "cannot convert the system time; it would be after the maximum local clock value");
    }

    return date::local_time<_Dp>{__time.time_since_epoch() + __info.offset};
  }

  [[nodiscard]] const __impl& __implementation() const noexcept {
    return *__impl_;
  }

 private:
  [[nodiscard]] sys_info load_sys_info(std::vector<date::transition>::const_iterator i) const;

  [[nodiscard]] std::string_view __name() const noexcept;


  [[nodiscard]] sys_info __get_info_to_populate_transition(date::sys_seconds __time) const;
  [[nodiscard]] sys_info __get_info(date::sys_seconds __time) const;
  [[nodiscard]] local_info __get_info(date::local_seconds __time) const;

  std::unique_ptr<__impl> __impl_;
};

[[nodiscard]] inline bool operator==(
    const time_zone& __x,
    const time_zone& __y) noexcept {
  return __x.name() == __y.name();
}

[[nodiscard]] inline bool operator!=(
    const time_zone& __x,
    const time_zone& __y) noexcept {
  return !(__x.name() == __y.name());
}

[[nodiscard]] inline bool operator<(
    const time_zone& __x,
    const time_zone& __y) noexcept {
  return __x.name() < __y.name();
}

[[nodiscard]] inline bool operator>(
    const time_zone& __x,
    const time_zone& __y) noexcept {
  return __x.name() > __y.name();
}

[[nodiscard]] inline bool operator<=(
    const time_zone& __x,
    const time_zone& __y) noexcept {
  return !(__x.name() > __y.name());
}

[[nodiscard]] inline bool operator>=(
    const time_zone& __x,
    const time_zone& __y) noexcept {
  return !(__x.name() < __y.name());
}

// [[nodiscard]] inline strong_ordering
// operator<=>(const time_zone& __x, const time_zone& __y) noexcept {
//   return __x.name() <=> __y.name();
// }

} // namespace facebook::velox::tzdb
