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
#include <folly/Traits.h>
#include <chrono>
#include <string_view>
#include <type_traits>
#include <utility>
#include "velox/external/date/date.h"
#include "velox/external/tzdb/sys_info.h"
#include "velox/external/tzdb/time_zone.h"
#include "velox/external/tzdb/tzdb_list.h"

namespace facebook::velox::tzdb {

template <class>
struct zoned_traits {};

template <>
struct zoned_traits<const time_zone*> {
  [[nodiscard]] static const time_zone* default_zone() {
    return locate_zone("UTC");
  }
  [[nodiscard]] static const time_zone* locate_zone(std::string_view __name) {
    return facebook::velox::tzdb::locate_zone(__name);
  }
};

template <class _Duration, class _TimeZonePtr = const time_zone*>
class zoned_time {
  // [time.zone.zonedtime.ctor]/2
  // static_assert(__is_duration_v<_Duration>,
  //               "the program is ill-formed since _Duration is not a
  //               specialization of std::chrono::duration");

  // The wording uses the constraints like
  //   constructible_from<zoned_time,
  //   decltype(__traits::locate_zone(string_view{}))>
  // Using these constraints in the code causes the compiler to give an
  // error that the constraint depends on itself. To avoid that issue use
  // the fact it is possible to create this object from a _TimeZonePtr.
  using __traits = zoned_traits<_TimeZonePtr>;

 public:
  using duration = std::common_type_t<_Duration, std::chrono::seconds>;

  zoned_time() : __zone_{__traits::default_zone()}, __tp_{} {}

  zoned_time(const zoned_time&) = default;
  zoned_time& operator=(const zoned_time&) = default;

  zoned_time(const date::sys_time<_Duration>& __tp)
      : __zone_{__traits::default_zone()}, __tp_{__tp} {}

  explicit zoned_time(_TimeZonePtr __zone)
      : __zone_{std::move(__zone)}, __tp_{} {}

  explicit zoned_time(std::string_view __name)
      : __zone_{__traits::locate_zone(__name)}, __tp_{} {}

  template <class _Duration2>
  zoned_time(const zoned_time<_Duration2, _TimeZonePtr>& __zt)
      : __zone_{__zt.get_time_zone()}, __tp_{__zt.get_sys_time()} {}

  zoned_time(_TimeZonePtr __zone, const date::sys_time<_Duration>& __tp)
      : __zone_{std::move(__zone)}, __tp_{__tp} {}

  zoned_time(std::string_view __name, const date::sys_time<_Duration>& __tp)
      : zoned_time{__traits::locate_zone(__name), __tp} {}

  zoned_time(_TimeZonePtr __zone, const date::local_time<_Duration>& __tp)
      : __zone_{std::move(__zone)}, __tp_{__zone_->to_sys(__tp)} {}

  zoned_time(std::string_view __name, const date::local_time<_Duration>& __tp)
      : zoned_time{__traits::locate_zone(__name), __tp} {}

  zoned_time(
      _TimeZonePtr __zone,
      const date::local_time<_Duration>& __tp,
      choose __c)
      : __zone_{std::move(__zone)}, __tp_{__zone_->to_sys(__tp, __c)} {}

  zoned_time(
      std::string_view __name,
      const date::local_time<_Duration>& __tp,
      choose __c)
      : zoned_time{__traits::locate_zone(__name), __tp, __c} {}

  template <class _Duration2, class _TimeZonePtr2>
  zoned_time(
      _TimeZonePtr __zone,
      const zoned_time<_Duration2, _TimeZonePtr2>& __zt)
      : __zone_{std::move(__zone)}, __tp_{__zt.get_sys_time()} {}

  // per wording choose has no effect
  template <class _Duration2, class _TimeZonePtr2>
  zoned_time(
      _TimeZonePtr __zone,
      const zoned_time<_Duration2, _TimeZonePtr2>& __zt,
      choose)
      : __zone_{std::move(__zone)}, __tp_{__zt.get_sys_time()} {}

  template <class _Duration2, class _TimeZonePtr2>
  zoned_time(
      std::string_view __name,
      const zoned_time<_Duration2, _TimeZonePtr2>& __zt)
      : zoned_time{__traits::locate_zone(__name), __zt} {}

  template <class _Duration2, class _TimeZonePtr2>
  zoned_time(
      std::string_view __name,
      const zoned_time<_Duration2, _TimeZonePtr2>& __zt,
      choose __c)
      : zoned_time{__traits::locate_zone(__name), __zt, __c} {}

  zoned_time& operator=(const date::sys_time<_Duration>& __tp) {
    __tp_ = __tp;
    return *this;
  }

  zoned_time& operator=(const date::local_time<_Duration>& __tp) {
    // TODO TZDB This seems wrong.
    // Assigning a non-existent or ambiguous time will throw and not satisfy
    // the post condition. This seems quite odd; I constructed an object with
    // choose::earliest and that choice is not respected.
    // what did LEWG do with this.
    // MSVC STL and libstdc++ behave the same
    __tp_ = __zone_->to_sys(__tp);
    return *this;
  }

  [[nodiscard]] operator date::sys_time<duration>() const {
    return get_sys_time();
  }
  [[nodiscard]] explicit operator date::local_time<duration>() const {
    return get_local_time();
  }

  [[nodiscard]] _TimeZonePtr get_time_zone() const {
    return __zone_;
  }
  [[nodiscard]] date::local_time<duration> get_local_time() const {
    return __zone_->to_local(__tp_);
  }
  [[nodiscard]] date::sys_time<duration> get_sys_time() const {
    return __tp_;
  }
  [[nodiscard]] sys_info get_info() const {
    return __zone_->get_info(__tp_);
  }

 private:
  _TimeZonePtr __zone_;
  date::sys_time<duration> __tp_;
};

zoned_time() -> zoned_time<std::chrono::seconds>;

template <class _Duration>
zoned_time(date::sys_time<_Duration>)
    -> zoned_time<std::common_type_t<_Duration, std::chrono::seconds>>;

template <class _TimeZonePtrOrName>
using __time_zone_representation = std::conditional_t<
    std::is_convertible_v<_TimeZonePtrOrName, std::string_view>,
    const time_zone*,
    folly::remove_cvref_t<_TimeZonePtrOrName>>;

template <class _TimeZonePtrOrName>
zoned_time(_TimeZonePtrOrName&&)
    -> zoned_time<
        std::chrono::seconds,
        __time_zone_representation<_TimeZonePtrOrName>>;

template <class _TimeZonePtrOrName, class _Duration>
zoned_time(_TimeZonePtrOrName&&, date::sys_time<_Duration>)
    -> zoned_time<
        std::common_type_t<_Duration, std::chrono::seconds>,
        __time_zone_representation<_TimeZonePtrOrName>>;

template <class _TimeZonePtrOrName, class _Duration>
zoned_time(
    _TimeZonePtrOrName&&,
    date::local_time<_Duration>,
    choose = choose::earliest)
    -> zoned_time<
        std::common_type_t<_Duration, std::chrono::seconds>,
        __time_zone_representation<_TimeZonePtrOrName>>;

template <class _Duration, class _TimeZonePtrOrName, class _TimeZonePtr2>
zoned_time(
    _TimeZonePtrOrName&&,
    zoned_time<_Duration, _TimeZonePtr2>,
    choose = choose::earliest)
    -> zoned_time<
        std::common_type_t<_Duration, std::chrono::seconds>,
        __time_zone_representation<_TimeZonePtrOrName>>;

using zoned_seconds = zoned_time<std::chrono::seconds>;

template <class _Duration1, class _Duration2, class _TimeZonePtr>
bool operator==(
    const zoned_time<_Duration1, _TimeZonePtr>& __lhs,
    const zoned_time<_Duration2, _TimeZonePtr>& __rhs) {
  return __lhs.get_time_zone() == __rhs.get_time_zone() &&
      __lhs.get_sys_time() == __rhs.get_sys_time();
}

template <class CharT, class Traits, class Duration, class TimeZonePtr>
std::basic_ostream<CharT, Traits>& to_stream(
    std::basic_ostream<CharT, Traits>& os,
    const CharT* fmt,
    const zoned_time<Duration, TimeZonePtr>& tp) {
  using duration = typename zoned_time<Duration, TimeZonePtr>::duration;
  using LT = date::local_time<duration>;
  auto const st = tp.get_sys_time();
  auto const info = tp.get_time_zone()->get_info(st);
  return to_stream(
      os,
      fmt,
      LT{(st + info.offset).time_since_epoch()},
      &info.abbrev,
      &info.offset);
}

} // namespace facebook::velox::tzdb
