//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

// For information see https://libcxx.llvm.org/DesignDocs/TimeZone.html

#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <fmt/format.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "velox/external/date/date.h"
#include "velox/external/tzdb/time_zone_private.h"
#include "velox/external/tzdb/types_private.h"
#include "velox/external/tzdb/tzdb_list_private.h"
#include "velox/external/tzdb/tzdb_private.h"

// Contains a parser for the IANA time zone data files.
//
// These files can be found at https://data.iana.org/time-zones/ and are in the
// public domain. Information regarding the input can be found at
// https://data.iana.org/time-zones/tz-how-to.html and
// https://man7.org/linux/man-pages/man8/zic.8.html.
//
// As indicated at https://howardhinnant.github.io/date/tz.html#Installation
// For Windows another file seems to be required
// https://raw.githubusercontent.com/unicode-org/cldr/master/common/supplemental/windowsZones.xml
// This file seems to contain the mapping of Windows time zone name to IANA
// time zone names.
//
// However this article mentions another way to do the mapping on Windows
// https://devblogs.microsoft.com/oldnewthing/20210527-00/?p=105255
// This requires Windows 10 Version 1903, which was released in May of 2019
// and considered end of life in December 2020
// https://learn.microsoft.com/en-us/lifecycle/announcements/windows-10-1903-end-of-servicing
//
// TODO TZDB Implement the Windows mapping in tzdb::current_zone

namespace facebook::velox::tzdb {

using namespace std::chrono_literals;

// This function is weak so it can be overridden in the tests. The
// declaration is in the test header test/support/test_tzdb.h
std::string __libcpp_tzdb_directory() {
  struct stat sb;
  using namespace std;
#if !defined(__APPLE__)
  CONSTDATA auto tz_dir_default = "/usr/share/zoneinfo";
  CONSTDATA auto tz_dir_buildroot = "/usr/share/zoneinfo/uclibc";

  // Check special path which is valid for buildroot with uclibc builds
  if (stat(tz_dir_buildroot, &sb) == 0 && S_ISDIR(sb.st_mode))
    return tz_dir_buildroot;
  else if (stat(tz_dir_default, &sb) == 0 && S_ISDIR(sb.st_mode))
    return tz_dir_default;
  else
    throw runtime_error("discover_tz_dir failed to find zoneinfo\n");
#else // __APPLE__
  CONSTDATA auto timezone = "/etc/localtime";
  if (!(lstat(timezone, &sb) == 0 && S_ISLNK(sb.st_mode) && sb.st_size > 0))
    throw runtime_error("discover_tz_dir failed\n");
  string result;
  unique_ptr<char[]> rp(new char[sb.st_size]);
  const auto rp_length = readlink(timezone, rp.get(), sb.st_size);
  if (rp_length > 0)
    result = string(rp.get(), rp_length); // readlink doesn't null-terminate
  else
    throw system_error(errno, system_category(), "readlink() failed");
  auto i = result.find("zoneinfo");
  if (i == string::npos)
    throw runtime_error("discover_tz_dir failed to find zoneinfo\n");
  i = result.find('/', i);
  if (i == string::npos)
    throw runtime_error("discover_tz_dir failed to find '/'\n");
  return result.substr(0, i);
#endif // __APPLE__
}

//===----------------------------------------------------------------------===//
//                           Details
//===----------------------------------------------------------------------===//

[[nodiscard]] static bool __is_whitespace(int __c) {
  return __c == ' ' || __c == '\t';
}

static void __skip_optional_whitespace(std::istream& __input) {
  while (__is_whitespace(__input.peek()))
    __input.get();
}

static void __skip_mandatory_whitespace(std::istream& __input) {
  if (!__is_whitespace(__input.get()))
    std::__throw_runtime_error("corrupt tzdb: expected whitespace");

  __skip_optional_whitespace(__input);
}

[[nodiscard]] static bool __is_eol(int __c) {
  return __c == '\n' || __c == std::char_traits<char>::eof();
}

static void __skip_line(std::istream& __input) {
  while (!__is_eol(__input.peek())) {
    __input.get();
  }
  __input.get();
}

static void __skip(std::istream& __input, char __suffix) {
  if (std::tolower(__input.peek()) == __suffix)
    __input.get();
}

static void __skip(std::istream& __input, std::string_view __suffix) {
  for (auto __c : __suffix)
    if (std::tolower(__input.peek()) == __c)
      __input.get();
}

static void __matches(std::istream& __input, char __expected) {
  if (std::isalpha(__expected) && !std::islower(__expected)) {
    throw std::runtime_error("lowercase characters only here!");
  }
  char __c = __input.get();
  if (std::tolower(__c) != __expected)
    std::__throw_runtime_error(
        (std::string("corrupt tzdb: expected character '") + __expected +
         "', got '" + __c + "' instead")
            .c_str());
}

static void __matches(std::istream& __input, std::string_view __expected) {
  for (auto __c : __expected) {
    if (std::isalpha(__c) && !std::islower(__c)) {
      throw std::runtime_error("lowercase strings only here!");
    }
    char __actual = __input.get();
    if (std::tolower(__actual) != __c)
      std::__throw_runtime_error(
          (std::string("corrupt tzdb: expected character '") + __c +
           "' from string '" + std::string(__expected) + "', got '" + __actual +
           "' instead")
              .c_str());
  }
}

[[nodiscard]] static std::string __parse_string(std::istream& __input) {
  std::string __result;
  while (true) {
    int __c = __input.get();
    switch (__c) {
      case ' ':
      case '\t':
      case '\n':
        __input.unget();
        [[fallthrough]];
      case std::istream::traits_type::eof():
        if (__result.empty())
          std::__throw_runtime_error("corrupt tzdb: expected a string");

        return __result;

      default:
        __result.push_back(__c);
    }
  }
}

[[nodiscard]] static int64_t __parse_integral(
    std::istream& __input,
    bool __leading_zero_allowed) {
  int64_t __result = __input.get();
  if (__leading_zero_allowed) {
    if (__result < '0' || __result > '9')
      std::__throw_runtime_error("corrupt tzdb: expected a digit");
  } else {
    if (__result < '1' || __result > '9')
      std::__throw_runtime_error("corrupt tzdb: expected a non-zero digit");
  }
  __result -= '0';
  while (true) {
    if (__input.peek() < '0' || __input.peek() > '9')
      return __result;

    // In order to avoid possible overflows we limit the accepted range.
    // Most values parsed are expected to be very small:
    // - 8784 hours in a year
    // - 31 days in a month
    // - year no real maximum, these values are expected to be less than
    //   the range of the year type.
    //
    // However the leapseconds use a seconds after epoch value. Using an
    // int would run into an overflow in 2038. By using a 64-bit value
    // the range is large enough for the bilions of years. Limiting that
    // range slightly to make the code easier is not an issue.
    if (__result > (std::numeric_limits<int64_t>::max() / 16))
      std::__throw_runtime_error("corrupt tzdb: integral too large");

    __result *= 10;
    __result += __input.get() - '0';
  }
}

//===----------------------------------------------------------------------===//
//                          Calendar
//===----------------------------------------------------------------------===//

[[nodiscard]] static date::day __parse_day(std::istream& __input) {
  unsigned __result = __parse_integral(__input, false);
  if (__result > 31)
    std::__throw_runtime_error("corrupt tzdb day: value too large");
  return date::day{__result};
}

[[nodiscard]] static date::weekday __parse_weekday(std::istream& __input) {
  // TZDB allows the shortest unique name.
  switch (std::tolower(__input.get())) {
    case 'f':
      __skip(__input, "riday");
      return date::Friday;

    case 'm':
      __skip(__input, "onday");
      return date::Monday;

    case 's':
      switch (std::tolower(__input.get())) {
        case 'a':
          __skip(__input, "turday");
          return date::Saturday;

        case 'u':
          __skip(__input, "nday");
          return date::Sunday;
      }
      break;

    case 't':
      switch (std::tolower(__input.get())) {
        case 'h':
          __skip(__input, "ursday");
          return date::Thursday;

        case 'u':
          __skip(__input, "esday");
          return date::Tuesday;
      }
      break;
    case 'w':
      __skip(__input, "ednesday");
      return date::Wednesday;
  }

  std::__throw_runtime_error("corrupt tzdb weekday: invalid name");
}

[[nodiscard]] static date::month __parse_month(std::istream& __input) {
  // TZDB allows the shortest unique name.
  switch (std::tolower(__input.get())) {
    case 'a':
      switch (std::tolower(__input.get())) {
        case 'p':
          __skip(__input, "ril");
          return date::April;

        case 'u':
          __skip(__input, "gust");
          return date::August;
      }
      break;

    case 'd':
      __skip(__input, "ecember");
      return date::December;

    case 'f':
      __skip(__input, "ebruary");
      return date::February;

    case 'j':
      switch (std::tolower(__input.get())) {
        case 'a':
          __skip(__input, "nuary");
          return date::January;

        case 'u':
          switch (std::tolower(__input.get())) {
            case 'n':
              __skip(__input, 'e');
              return date::June;

            case 'l':
              __skip(__input, 'y');
              return date::July;
          }
      }
      break;

    case 'm':
      if (std::tolower(__input.get()) == 'a')
        switch (std::tolower(__input.get())) {
          case 'y':
            return date::May;

          case 'r':
            __skip(__input, "ch");
            return date::March;
        }
      break;

    case 'n':
      __skip(__input, "ovember");
      return date::November;

    case 'o':
      __skip(__input, "ctober");
      return date::October;

    case 's':
      __skip(__input, "eptember");
      return date::September;
  }
  std::__throw_runtime_error("corrupt tzdb month: invalid name");
}

[[nodiscard]] static date::year __parse_year_value(std::istream& __input) {
  bool __negative = __input.peek() == '-';
  if (__negative) [[unlikely]]
    __input.get();

  int64_t __result = __parse_integral(__input, true);
  if (__result > static_cast<int64_t>(date::year::max())) {
    if (__negative)
      std::__throw_runtime_error(
          "corrupt tzdb year: year is less than the minimum");

    std::__throw_runtime_error(
        "corrupt tzdb year: year is greater than the maximum");
  }

  return date::year{static_cast<int>(__negative ? -__result : __result)};
}

[[nodiscard]] static date::year __parse_year(std::istream& __input) {
  if (std::tolower(__input.peek()) != 'm') [[likely]]
    return __parse_year_value(__input);

  __input.get();
  switch (std::tolower(__input.peek())) {
    case 'i':
      __input.get();
      __skip(__input, 'n');
      [[fallthrough]];

    case ' ':
      // The m is minimum, even when that is ambiguous.
      return date::year::min();

    case 'a':
      __input.get();
      __skip(__input, 'x');
      return date::year::max();
  }

  std::__throw_runtime_error("corrupt tzdb year: expected 'min' or 'max'");
}

//===----------------------------------------------------------------------===//
//                        TZDB fields
//===----------------------------------------------------------------------===//

[[nodiscard]] static date::year __parse_to(
    std::istream& __input,
    date::year __only) {
  if (std::tolower(__input.peek()) != 'o')
    return __parse_year(__input);

  __input.get();
  __skip(__input, "nly");
  return __only;
}

[[nodiscard]] static __constrained_weekday::__comparison_t __parse_comparison(
    std::istream& __input) {
  switch (__input.get()) {
    case '>':
      __matches(__input, '=');
      return __constrained_weekday::__ge;

    case '<':
      __matches(__input, '=');
      return __constrained_weekday::__le;
  }
  std::__throw_runtime_error("corrupt tzdb on: expected '>=' or '<='");
}

[[nodiscard]] static facebook::velox::tzdb::__on __parse_on(
    std::istream& __input) {
  if (std::isdigit(__input.peek()))
    return __parse_day(__input);

  if (std::tolower(__input.peek()) == 'l') {
    __matches(__input, "last");
    return date::weekday_last(__parse_weekday(__input));
  }

  return __constrained_weekday{
      __parse_weekday(__input),
      __parse_comparison(__input),
      __parse_day(__input)};
}

[[nodiscard]] static std::chrono::seconds __parse_duration(
    std::istream& __input) {
  std::chrono::seconds __result{0};
  int __c = __input.peek();
  bool __negative = __c == '-';
  if (__negative) {
    __input.get();
    // Negative is either a negative value or a single -.
    // The latter means 0 and the parsing is complete.
    if (!std::isdigit(__input.peek()))
      return __result;
  }

  __result += std::chrono::hours(__parse_integral(__input, true));
  if (__input.peek() != ':')
    return __negative ? -__result : __result;

  __input.get();
  __result += std::chrono::minutes(__parse_integral(__input, true));
  if (__input.peek() != ':')
    return __negative ? -__result : __result;

  __input.get();
  __result += std::chrono::seconds(__parse_integral(__input, true));
  if (__input.peek() != '.')
    return __negative ? -__result : __result;

  __input.get();
  (void)__parse_integral(__input, true); // Truncate the digits.

  return __negative ? -__result : __result;
}

[[nodiscard]] static facebook::velox::tzdb::__clock __parse_clock(
    std::istream& __input) {
  switch (__input.get()) { // case sensitive
    case 'w':
      return facebook::velox::tzdb::__clock::__local;
    case 's':
      return facebook::velox::tzdb::__clock::__standard;

    case 'u':
    case 'g':
    case 'z':
      return facebook::velox::tzdb::__clock::__universal;
  }

  __input.unget();
  return facebook::velox::tzdb::__clock::__local;
}

[[nodiscard]] static bool __parse_dst(
    std::istream& __input,
    std::chrono::seconds __offset) {
  switch (__input.get()) { // case sensitive
    case 's':
      return false;

    case 'd':
      return true;
  }

  __input.unget();
  return __offset != 0s;
}

[[nodiscard]] static facebook::velox::tzdb::__at __parse_at(
    std::istream& __input) {
  return {__parse_duration(__input), __parse_clock(__input)};
}

[[nodiscard]] static facebook::velox::tzdb::__save __parse_save(
    std::istream& __input) {
  std::chrono::seconds __time = __parse_duration(__input);
  return {__time, __parse_dst(__input, __time)};
}

[[nodiscard]] static std::string __parse_letters(std::istream& __input) {
  std::string __result = __parse_string(__input);
  // Canonicalize "-" to "" since they are equivalent in the specification.
  return __result != "-" ? __result : "";
}

[[nodiscard]] static __continuation::__rules_t __parse_rules(
    std::istream& __input) {
  int __c = __input.peek();
  // A single -  is not a SAVE but a special case.
  if (__c == '-') {
    __input.get();
    if (__is_whitespace(__input.peek()))
      return std::monostate{};
    __input.unget();
    return __parse_save(__input);
  }

  if (std::isdigit(__c) || __c == '+')
    return __parse_save(__input);

  return __parse_string(__input);
}

[[nodiscard]] static auto __binary_find(
    const __rules_storage_type& __rules_db,
    const std::string& __rule_name) {
  auto __end = __rules_db.end();
  auto __ret = std::lower_bound(
      __rules_db.begin(),
      __rules_db.end(),
      __rule_name,
      [](const auto& rule, const auto& name) { return rule.first < name; });
  if (__ret == __end)
    return __end;

  // When the value does not match the predicate it's equal and a valid result
  // was found.
  return __rule_name >= __ret->first ? __ret : __end;
}

[[nodiscard]] static facebook::velox::tzdb::__continuation __parse_continuation(
    __rules_storage_type& __rules,
    std::istream& __input) {
  facebook::velox::tzdb::__continuation __result;

  __result.__rule_database_ = std::addressof(__rules);

  // Note STDOFF is specified as
  //   This field has the same format as the AT and SAVE fields of rule lines;
  // These fields have different suffix letters, these letters seem
  // not to be used so do not allow any of them.

  __result.__stdoff = __parse_duration(__input);
  __skip_mandatory_whitespace(__input);
  __result.__rules = __parse_rules(__input);
  __skip_mandatory_whitespace(__input);
  __result.__format = __parse_string(__input);
  __skip_optional_whitespace(__input);

  if (std::holds_alternative<std::string>(__result.__rules)) {
    const auto&  rule_name = std::get<std::string>(__result.__rules);
    const auto rules = __binary_find(__rules, rule_name);
    if (rules == std::end(__rules))
      std::__throw_runtime_error(
          (fmt::format("corrupt tzdb: rule '{}' does not exist", rule_name)).c_str());
    size_t numForeverRules = 0;
    std::pair<std::vector<__rule>::const_iterator, std::vector<__rule>::const_iterator>& foreverRules = __result.__forever_rules;
    for (auto iter = rules->second.cbegin(); iter < rules->second.cend(); ++iter) {
      if (iter->__to == date::year::max()) {
        // We found a forever rule that started to apply prior to year.
        numForeverRules++;
        if (numForeverRules == 1) {
          std::get<0>(foreverRules) = iter;
        } else if (numForeverRules == 2) {
          std::get<1>(foreverRules) = iter;
        } else {
          // If we find more than 2 forever rules we have to go
          // through the loop (currently this is impossible).
          break;
        }
      }
    }

    if (numForeverRules > 0) {
      if (numForeverRules != 2) {
        throw std::runtime_error(fmt::format("Found {} forever rules for time zone rule {}, expected 2", numForeverRules, rule_name));
      }

      __result.__has_forever_rules = true;
    }  
  }

  if (__is_eol(__input.peek()))
    return __result;
  __result.__year = __parse_year(__input);
  __skip_optional_whitespace(__input);

  if (__is_eol(__input.peek()))
    return __result;
  __result.__in = __parse_month(__input);
  __skip_optional_whitespace(__input);

  if (__is_eol(__input.peek()))
    return __result;
  __result.__on = __parse_on(__input);
  __skip_optional_whitespace(__input);

  if (__is_eol(__input.peek()))
    return __result;
  __result.__at = __parse_at(__input);

  return __result;
}

//===----------------------------------------------------------------------===//
//                   Time Zone Database entries
//===----------------------------------------------------------------------===//

static std::string __parse_version(std::istream& __input) {
  // The first line in tzdata.zi contains
  //    # version YYYYw
  // The parser expects this pattern
  // #\s*version\s*\(.*)
  // This part is not documented.
  __matches(__input, '#');
  __skip_optional_whitespace(__input);
  __matches(__input, "version");
  __skip_mandatory_whitespace(__input);
  return __parse_string(__input);
}

[[nodiscard]]
static __rule& __create_entry(
    __rules_storage_type& __rules,
    const std::string& __name) {
  auto __result = [&]() -> __rule& {
    auto rule = __rules.emplace(std::upper_bound(__rules.begin(), __rules.end(), __name, [](const auto& __left, const auto& __right) {
        return __left < __right.first;
      }), __name, std::vector<__rule>{});
    return rule->second.emplace_back();
  };

  if (__rules.empty())
    return __result();

  // Typically rules are in contiguous order in the database.
  // But there are exceptions, some rules are interleaved.
  if (__rules.back().first == __name)
    return __rules.back().second.emplace_back();

  if (auto __it = std::find_if(
          __rules.begin(),
          __rules.end(),
          [&__name](const auto& __r) { return __r.first == __name; });
      __it != __rules.end())
    return __it->second.emplace_back();

  return __result();
}

static void __parse_rule(
    tzdb& __tzdb,
    __rules_storage_type& __rules,
    std::istream& __input) {
  __skip_mandatory_whitespace(__input);
  std::string __name = __parse_string(__input);

  __rule& __rule = __create_entry(__rules, __name);

  __skip_mandatory_whitespace(__input);
  __rule.__from = __parse_year(__input);
  __skip_mandatory_whitespace(__input);
  __rule.__to = __parse_to(__input, __rule.__from);
  __skip_mandatory_whitespace(__input);
  __matches(__input, '-');
  __skip_mandatory_whitespace(__input);
  __rule.__in = __parse_month(__input);
  __skip_mandatory_whitespace(__input);
  __rule.__on = __parse_on(__input);
  __skip_mandatory_whitespace(__input);
  __rule.__at = __parse_at(__input);
  __skip_mandatory_whitespace(__input);
  __rule.__save = __parse_save(__input);
  __skip_mandatory_whitespace(__input);
  __rule.__letters = __parse_letters(__input);
  __skip_line(__input);
}

static void __parse_zone(
    tzdb& __tzdb,
    __rules_storage_type& __rules,
    std::istream& __input) {
  __skip_mandatory_whitespace(__input);
  auto __p =
      std::make_unique<time_zone::__impl>(__parse_string(__input), __rules);
  std::vector<facebook::velox::tzdb::__continuation>& __continuations = __p->__continuations();
  __skip_mandatory_whitespace(__input);

  do {
    // The first line must be valid, continuations are optional.
    __continuations.emplace_back(__parse_continuation(__rules, __input));
    __skip_line(__input);
    __skip_optional_whitespace(__input);
  } while (std::isdigit(__input.peek()) || __input.peek() == '-');

  std::filesystem::path __root = __libcpp_tzdb_directory();
  if (!std::filesystem::exists(__root/ __p->__name())) {
    // in case the zonefile does not exists
    return;
  }
  std::ifstream zone_file{__root / __p->__name()};
  date::populate_transitions(__p->transitions(), __p->ttinfos(), zone_file);

  __tzdb.zones.emplace_back(time_zone::__create(std::move(__p)));
}

static void __parse_link(tzdb& __tzdb, std::istream& __input) {
  __skip_mandatory_whitespace(__input);
  std::string __target = __parse_string(__input);
  __skip_mandatory_whitespace(__input);
  std::string __name = __parse_string(__input);
  __skip_line(__input);

  __tzdb.links.emplace_back(std::move(__name), std::move(__target));
}

template <int recordType>
static void __parse_tzdata_record_type(
    tzdb& __db,
    __rules_storage_type& __rules,
    std::istream& __input) {
  while (true) {
    int __c = std::tolower(__input.get());

    switch (__c) {
      case std::istream::traits_type::eof():
        return;

      case ' ':
      case '\t':
      case '\n':
        break;

      case '#':
        __skip_line(__input);
        break;

      case 'r':
        if constexpr (recordType == 'r') {
          __skip(__input, "ule");
          __parse_rule(__db, __rules, __input);
        } else {
          __skip_line(__input);
        }
        break;

      case 'l':
        if constexpr (recordType == 'l') {
          __skip(__input, "ink");
          __parse_link(__db, __input);
        } else {
          __skip_line(__input);
        }
        break;

      case 'z':
        if constexpr (recordType == 'z') {
          __skip(__input, "one");
          __parse_zone(__db, __rules, __input);
        } else {
          __skip_line(__input);
        }
        break;

      default:
        if constexpr (recordType == 'z') {
          // Only z (Zones) have continuation lines.
          // When parsing z, any token not matched by the earlier cases is invalid.
          // (continuations are consumed inside __parse_zone)
          std::__throw_runtime_error("corrupt tzdb: unexpected input");
        } else {
          __skip_line(__input);
        }
    }
  }
}

static void __parse_tzdata(
    tzdb& __db,
    __rules_storage_type& __rules,
    std::istream& __input) {
  // Parse tzdata.zi in order: Rules → Zones → Links.
  // Zones may reference Rules; Links alias Zones.
  const std::string buf{std::istreambuf_iterator<char>(__input), {}};
  { std::istringstream iss(buf); __parse_tzdata_record_type<'r'>(__db, __rules, iss); }
  { std::istringstream iss(buf); __parse_tzdata_record_type<'z'>(__db, __rules, iss); }
  { std::istringstream iss(buf); __parse_tzdata_record_type<'l'>(__db, __rules, iss); }
}

static void __parse_leap_seconds(
    std::vector<leap_second>& __leap_seconds,
    std::istream&& __input) {
  // The file stores dates since 1 January 1900, 00:00:00, we want
  // seconds since 1 January 1970.
  constexpr auto __offset =
      date::sys_days{date::year(1970) / date::January / 1} -
      date::sys_days{date::year(1900) / date::January / 1};

  struct __entry {
    __entry(date::sys_seconds __timestamp, std::chrono::seconds __value)
        : __timestamp(__timestamp), __value(__value) {}

    date::sys_seconds __timestamp;
    std::chrono::seconds __value;
  };
  std::vector<__entry> __entries;
  [&] {
    while (true) {
      switch (__input.peek()) {
        case std::istream::traits_type::eof():
          return;

        case ' ':
        case '\t':
        case '\n':
          __input.get();
          continue;

        case '#':
          __skip_line(__input);
          continue;
      }

      date::sys_seconds __date = date::sys_seconds{std::chrono::seconds{
                                     __parse_integral(__input, false)}} -
          __offset;
      __skip_mandatory_whitespace(__input);
      std::chrono::seconds __value{__parse_integral(__input, false)};
      __skip_line(__input);

      __entries.emplace_back(__date, __value);
    }
  }();
  // The Standard requires the leap seconds to be sorted. The file
  // leap-seconds.list usually provides them in sorted order, but that is not
  // guaranteed so we ensure it here.
  std::sort(
      __entries.begin(),
      __entries.end(),
      [](const auto& left_entry, const auto& right_entry) {
        return left_entry.__timestamp < right_entry.__timestamp;
      });

  // The database should contain the number of seconds inserted by a leap
  // second (1 or -1). So the difference between the two elements is stored.
  // std::ranges::views::adjacent has not been implemented yet.
  (void)std::adjacent_find(
      __entries.begin(),
      __entries.end(),
      [&](const __entry& __first, const __entry& __second) {
        __leap_seconds.emplace_back(
            __second.__timestamp, __second.__value - __first.__value);
        return false;
      });
}

void __init_tzdb(tzdb& __tzdb, __rules_storage_type& __rules) {
  std::filesystem::path __root = __libcpp_tzdb_directory();
  std::ifstream __tzdata{__root / "tzdata.zi"};

  __tzdb.version = __parse_version(__tzdata);
  __parse_tzdata(__tzdb, __rules, __tzdata);
  std::sort(__tzdb.zones.begin(), __tzdb.zones.end());
  std::sort(__tzdb.links.begin(), __tzdb.links.end());
  std::sort(
      __rules.begin(),
      __rules.end(),
      [](const auto& __left, const auto& __right) {
        return __left.first < __right.first;
      });

  // There are two files with the leap second information
  // - leapseconds as specified by zic
  // - leap-seconds.list the source data
  // The latter is much easier to parse, it seems Howard shares that
  // opinion.
  __parse_leap_seconds(
      __tzdb.leap_seconds, std::ifstream{__root / "leap-seconds.list"});
}

#ifdef _WIN32
[[nodiscard]] static const time_zone* __current_zone_windows(const tzdb& tzdb) {
  // TODO TZDB Implement this on Windows.
  std::__throw_runtime_error("unknown time zone");
}
#else // ifdef _WIN32
[[nodiscard]] static const time_zone* __current_zone_posix(const tzdb& tzdb) {
  // On POSIX systems there are several ways to configure the time zone.
  // In order of priority they are:
  // - TZ environment variable
  //   https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap08.html#tag_08
  //   The documentation is unclear whether or not it's allowed to
  //   change time zone information. For example the TZ string
  //     MST7MDT
  //   this is an entry in tzdata.zi. The value
  //     MST
  //   is also an entry. Is it allowed to use the following?
  //     MST-3
  //   Even when this is valid there is no time_zone record in the
  //   database. Since the library would need to return a valid pointer,
  //   this means the library needs to allocate and leak a pointer.
  //
  // - The time zone name is the target of the symlink /etc/localtime
  //   relative to /usr/share/zoneinfo/

  // The algorithm is like this:
  // - If the environment variable TZ is set and points to a valid
  //   record use this value.
  // - Else use the name based on the `/etc/localtime` symlink.

  if (const char* __tz = getenv("TZ"))
    if (const time_zone* __result = tzdb.__locate_zone(__tz))
      return __result;

  std::filesystem::path __path = "/etc/localtime";
  if (!std::filesystem::exists(__path))
    std::__throw_runtime_error(
        "tzdb: the symlink '/etc/localtime' does not exist");

  if (!std::filesystem::is_symlink(__path))
    std::__throw_runtime_error(
        "tzdb: the path '/etc/localtime' is not a symlink");

  std::filesystem::path __tz = std::filesystem::read_symlink(__path);
  // The path may be a relative path, in that case convert it to an absolute
  // path based on the proper initial directory.
  if (__tz.is_relative())
    __tz = std::filesystem::canonical("/etc" / __tz);

  std::string __name = std::filesystem::relative(__tz, "/usr/share/zoneinfo/");
  if (const time_zone* __result = tzdb.__locate_zone(__name))
    return __result;

  std::__throw_runtime_error(
      ("tzdb: the time zone '" + __name + "' is not found in the database")
          .c_str());
}
#endif // ifdef _WIN32

//===----------------------------------------------------------------------===//
//                           Public API
//===----------------------------------------------------------------------===//

tzdb_list& get_tzdb_list() {
  static tzdb_list __result{new tzdb_list::__impl()};
  return __result;
}

[[nodiscard]] const time_zone* tzdb::__current_zone() const {
#ifdef _WIN32
  return __current_zone_windows(*this);
#else
  return __current_zone_posix(*this);
#endif
}

const tzdb& reload_tzdb() {
  if (remote_version() == get_tzdb().version)
    return get_tzdb();

  return get_tzdb_list().__implementation().__load();
}

std::string remote_version() {
  std::filesystem::path __root = __libcpp_tzdb_directory();
  std::ifstream __tzdata{__root / "tzdata.zi"};
  return __parse_version(__tzdata);
}

} // namespace facebook::velox::tzdb
