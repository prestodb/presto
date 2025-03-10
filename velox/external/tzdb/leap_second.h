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
#include "velox/external/date/date.h"

namespace facebook::velox::tzdb {

class leap_second {
 public:
  [[nodiscard]]
  explicit constexpr leap_second(
      date::sys_seconds __date,
      std::chrono::seconds __value)
      : __date_(__date), __value_(__value) {}

  leap_second(const leap_second&) = default;
  leap_second& operator=(const leap_second&) = default;

  [[nodiscard]] constexpr date::sys_seconds date() const noexcept {
    return __date_;
  }

  [[nodiscard]] constexpr std::chrono::seconds value() const noexcept {
    return __value_;
  }

 private:
  date::sys_seconds __date_;
  std::chrono::seconds __value_;

  // The function
  //   template<class Duration>
  //    requires three_way_comparable_with<sys_seconds, sys_time<Duration>>
  //    constexpr auto operator<=>(const leap_second& x, const
  //    sys_time<Duration>& y) noexcept;
  //
  // Has constraints that are recursive (LWG4139). The proposed resolution is
  // to make the funcion a hidden friend. For consistency make this change for
  // all comparison functions.

  friend constexpr bool operator==(
      const leap_second& __x,
      const leap_second& __y) {
    return __x.date() == __y.date();
  }

  // friend constexpr strong_ordering operator<=>(const leap_second& __x, const
  // leap_second& __y) {
  //   return __x.date() <=> __y.date();
  // }

  template <class _Duration>
  friend constexpr bool operator==(
      const leap_second& __x,
      const date::sys_time<_Duration>& __y) {
    return __x.date() == __y;
  }

  template <class _Duration>
  friend constexpr bool operator<(
      const leap_second& __x,
      const date::sys_time<_Duration>& __y) {
    return __x.date() < __y;
  }

  template <class _Duration>
  friend constexpr bool operator<(
      const date::sys_time<_Duration>& __x,
      const leap_second& __y) {
    return __x < __y.date();
  }

  template <class _Duration>
  friend constexpr bool operator>(
      const leap_second& __x,
      const date::sys_time<_Duration>& __y) {
    return __y < __x;
  }

  template <class _Duration>
  friend constexpr bool operator>(
      const date::sys_time<_Duration>& __x,
      const leap_second& __y) {
    return __y < __x;
  }

  template <class _Duration>
  friend constexpr bool operator<=(
      const leap_second& __x,
      const date::sys_time<_Duration>& __y) {
    return !(__y < __x);
  }

  template <class _Duration>
  friend constexpr bool operator<=(
      const date::sys_time<_Duration>& __x,
      const leap_second& __y) {
    return !(__y < __x);
  }

  template <class _Duration>
  friend constexpr bool operator>=(
      const leap_second& __x,
      const date::sys_time<_Duration>& __y) {
    return !(__x < __y);
  }

  template <class _Duration>
  friend constexpr bool operator>=(
      const date::sys_time<_Duration>& __x,
      const leap_second& __y) {
    return !(__x < __y);
  }

  // template <class _Duration>
  //   requires three_way_comparable_with<sys_seconds, sys_time<_Duration>>
  // _LIBCPP_HIDE_FROM_ABI friend constexpr auto operator<=>(const leap_second&
  // __x, const sys_time<_Duration>& __y) {
  //   return __x.date() <=> __y;
  // }
};

} // namespace facebook::velox::tzdb
