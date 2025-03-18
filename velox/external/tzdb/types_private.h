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
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "velox/external/date/date.h"

// TODO TZDB
// The helper classes in this header have no constructor but are loaded with
// dedicated parse functions. In the original design this header was public and
// the parsing was done in the dylib. In that design having constructors would
// expand the ABI interface. Since this header is now in the dylib that design
// should be reconsidered. (For now the design is kept as is, in case this
// header needs to be public for unforseen reasons.)

namespace facebook::velox::tzdb {

// Sun>=8   first Sunday on or after the eighth
// Sun<=25  last Sunday on or before the 25th
struct __constrained_weekday {
  [[nodiscard]] date::year_month_day operator()(
      date::year __year,
      date::month __month) const {
    auto __result = static_cast<date::sys_days>(
        date::year_month_day{__year, __month, __day});
    date::weekday __wd{static_cast<date::sys_days>(__result)};

    if (__comparison == __le)
      __result -= __wd - __weekday;
    else
      __result += __weekday - __wd;

    return __result;
  }

  date::weekday __weekday;
  enum __comparison_t { __le, __ge } __comparison;
  date::day __day;
};

// The on field has a few alternative presentations
//  5        the fifth of the month
//  lastSun  the last Sunday in the month
//  lastMon  the last Monday in the month
//  Sun>=8   first Sunday on or after the eighth
//  Sun<=25  last Sunday on or before the 25th
using __on = std::variant<date::day, date::weekday_last, __constrained_weekday>;

enum class __clock { __local, __standard, __universal };

struct __at {
  std::chrono::seconds __time{0};
  facebook::velox::tzdb::__clock __clock{
      facebook::velox::tzdb::__clock::__local};
};

struct __save {
  std::chrono::seconds __time;
  bool __is_dst;
};

// The names of the fields match the fields of a Rule.
struct __rule {
  date::year __from;
  date::year __to;
  date::month __in;
  facebook::velox::tzdb::__on __on;
  facebook::velox::tzdb::__at __at;
  facebook::velox::tzdb::__save __save;
  std::string __letters;
};

using __rules_storage_type =
    std::vector<std::pair<std::string, std::vector<__rule>>>; // TODO TZDB use
                                                              // flat_map;

struct __continuation {
  // Non-owning link to the RULE entries.
  __rules_storage_type* __rule_database_;

  std::chrono::seconds __stdoff;

  // The RULES is either a SAVE or a NAME.
  // The size_t is used as cache. After loading the rules they are
  // sorted and remain stable, then an index in the vector can be
  // used.
  // If this field contains - then standard time always
  // applies. This is indicated by the monostate.
  // TODO TZDB Investigate implantation the size_t based caching.
  using __rules_t = std::variant<
      std::monostate,
      facebook::velox::tzdb::__save,
      std::string /*, size_t*/>;

  __rules_t __rules;
  bool __has_forever_rules = false;
  std::pair<std::vector<__rule>::const_iterator, std::vector<__rule>::const_iterator> __forever_rules{};

  std::string __format;
  // TODO TZDB the until field can contain more than just a year.
  // Parts of the UNTIL, the optional parts are default initialized
  //    optional<year> __until_;
  date::year __year = date::year::min();
  date::month __in{date::January};
  facebook::velox::tzdb::__on __on{date::day{1}};
  facebook::velox::tzdb::__at __at{
      std::chrono::seconds{0},
      facebook::velox::tzdb::__clock::__local};
};

} // namespace facebook::velox::tzdb
