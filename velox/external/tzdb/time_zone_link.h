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

#include <string>
#include <string_view>

namespace facebook::velox::tzdb {

class time_zone_link {
 public:
  [[nodiscard]]
  explicit time_zone_link(std::string_view __name, std::string_view __target)
      : __name_{__name}, __target_{__target} {}

  time_zone_link(time_zone_link&&) = default;
  time_zone_link& operator=(time_zone_link&&) = default;

  [[nodiscard]] std::string_view name() const noexcept {
    return __name_;
  }
  [[nodiscard]] std::string_view target() const noexcept {
    return __target_;
  }

 private:
  std::string __name_;
  // TODO TZDB instead of the name we can store the pointer to a zone. These
  // pointers are immutable. This makes it possible to directly return a
  // pointer in the time_zone in the 'locate_zone' function.
  std::string __target_;
};

[[nodiscard]] inline bool operator==(
    const time_zone_link& __x,
    const time_zone_link& __y) noexcept {
  return __x.name() == __y.name();
}

[[nodiscard]] inline bool operator!=(
    const time_zone_link& __x,
    const time_zone_link& __y) noexcept {
  return !(__x.name() == __y.name());
}

[[nodiscard]] inline bool operator<(
    const time_zone_link& __x,
    const time_zone_link& __y) noexcept {
  return __x.name() < __y.name();
}

[[nodiscard]] inline bool operator>(
    const time_zone_link& __x,
    const time_zone_link& __y) noexcept {
  return __x.name() > __y.name();
}

[[nodiscard]] inline bool operator<=(
    const time_zone_link& __x,
    const time_zone_link& __y) noexcept {
  return !(__x.name() > __y.name());
}

[[nodiscard]] inline bool operator>=(
    const time_zone_link& __x,
    const time_zone_link& __y) noexcept {
  return !(__x.name() < __y.name());
}

// [[nodiscard]] inline strong_ordering
// operator<=>(const time_zone_link& __x, const time_zone_link& __y) noexcept {
//   return __x.name() <=> __y.name();
// }

} // namespace facebook::velox::tzdb
