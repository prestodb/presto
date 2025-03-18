//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

// For information see https://libcxx.llvm.org/DesignDocs/TimeZone.html

// TODO TZDB look at optimizations
//
// The current algorithm is correct but not efficient. For example, in a named
// rule based continuation finding the next rule does quite a bit of work,
// returns the next rule and "forgets" its state. This could be better.
//
// It would be possible to cache lookups. If a time for a zone is calculated its
// sys_info could be kept and the next lookup could test whether the time is in
// a "known" sys_info. The wording in the Standard hints at this slowness by
// "suggesting" this could be implemented on the user's side.

// TODO TZDB look at removing quirks
//
// The code has some special rules to adjust the timing at the continuation
// switches. This works correctly, but some of the places feel odd. It would be
// good to investigate this further and see whether all quirks are needed or
// that there are better fixes.
//
// These quirks often use a 12h interval; this is the scan interval of zdump,
// which implies there are no sys_info objects with a duration of less than 12h.

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <folly/Expected.h>
#include <algorithm>
#include <map>
#include <optional>

#include "velox/external/date/date.h"
#include "velox/external/date/tz.h"
#include "velox/external/tzdb/time_zone_private.h"
#include "velox/external/tzdb/types_private.h"

namespace facebook::velox::tzdb {

using namespace std::chrono_literals;

//===----------------------------------------------------------------------===//
//                           Details
//===----------------------------------------------------------------------===//

struct __sys_info {
  sys_info __info;
  bool __can_merge; // Can the returned sys_info object be merged with
};

// Return type for helper function to get a sys_info.
// - The expected result returns the "best" sys_info object. This object can be
//   before the requested time. Sometimes sys_info objects from different
//   continuations share their offset, save, and abbrev and these objects are
//   merged to one sys_info object. The __can_merge flag determines whether the
//   current result can be merged with the next result.
// - The unexpected result means no sys_info object was found and the time is
//   the time to be used for the next search iteration.
using __sys_info_result = folly::Expected<__sys_info, date::sys_seconds>;

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

// Format based on https://data.iana.org/time-zones/tz-how-to.html
//
// 1  a time zone abbreviation that is a string of three or more characters that
//    are either ASCII alphanumerics, "+", or "-"
// 2  the string "%z", in which case the "%z" will be replaced by a numeric time
//    zone abbreviation
// 3  a pair of time zone abbreviations separated by a slash ('/'), in which
//    case the first string is the abbreviation for the standard time name and
//    the second string is the abbreviation for the daylight saving time name
// 4  a string containing "%s", in which case the "%s" will be replaced by the
//    text in the appropriate Rule's LETTER column, and the resulting string
//    should be a time zone abbreviation
//
// Rule 1 is not strictly validated since America/Barbados uses a two letter
// abbreviation AT.
[[nodiscard]] static std::string __format(
    const facebook::velox::tzdb::__continuation& __continuation,
    const std::string& __letters,
    std::chrono::seconds __save) {
  bool __shift = false;
  std::string __result;
  for (char __c : __continuation.__format) {
    if (__shift) {
      switch (__c) {
        case 's':
          std::copy(
              __letters.begin(), __letters.end(), std::back_inserter(__result));
          break;

        case 'z': {
          if (__continuation.__format.size() != 2)
            std::__throw_runtime_error(
                fmt::format(
                    "corrupt tzdb FORMAT field: %z should be the entire contents, instead contains '{}'",
                    __continuation.__format)
                    .c_str());
          date::hh_mm_ss __offset{__continuation.__stdoff + __save};
          if (__offset.is_negative()) {
            __result += '-';
            __offset = date::hh_mm_ss{-(__continuation.__stdoff + __save)};
          } else
            __result += '+';

          if (__offset.minutes() != 0min)
            fmt::format_to(
                std::back_inserter(__result),
                "{:%H}{:%M}",
                __offset.hours(),
                __offset.minutes());
          else
            fmt::format_to(
                std::back_inserter(__result), "{:%H}", __offset.hours());
        } break;

        default:
          std::__throw_runtime_error(
              fmt::format(
                  "corrupt tzdb FORMAT field: invalid sequence '%{}' found, expected %s or %z",
                  __c)
                  .c_str());
      }
      __shift = false;

    } else if (__c == '/') {
      if (__save != 0s)
        __result.clear();
      else
        break;

    } else if (__c == '%') {
      __shift = true;
    } else if (__c == '+' || __c == '-' || std::isalnum(__c)) {
      __result.push_back(__c);
    } else {
      std::__throw_runtime_error(
          fmt::format(
              "corrupt tzdb FORMAT field: invalid character '{}' found, expected +, -, or an alphanumeric value",
              __c)
              .c_str());
    }
  }

  if (__shift)
    std::__throw_runtime_error(
        "corrupt tzdb FORMAT field: input ended with the start of the escape sequence '%'");

  if (__result.empty())
    std::__throw_runtime_error("corrupt tzdb FORMAT field: result is empty");

  return __result;
}

[[nodiscard]] static date::sys_seconds __to_sys_seconds(
    date::year_month_day __ymd,
    std::chrono::seconds __seconds) {
  std::chrono::seconds __result =
      static_cast<date::sys_days>(__ymd).time_since_epoch() + __seconds;
  return date::sys_seconds{__result};
}

template <class...>
struct False : std::bool_constant<false> {};

[[nodiscard]] static std::chrono::seconds __at_to_sys_seconds(
    const facebook::velox::tzdb::__continuation& __continuation) {
  switch (__continuation.__at.__clock) {
    case facebook::velox::tzdb::__clock::__local:
      return __continuation.__at.__time - __continuation.__stdoff -
          std::visit(
                 [](const auto& __value) {
                   using _Tp = std::decay_t<decltype(__value)>;
                   if constexpr (std::is_same_v<_Tp, std::monostate>)
                     return std::chrono::seconds{0};
                   else if constexpr (std::is_same_v<
                                          _Tp,
                                          facebook::velox::tzdb::__save>)
                     return std::chrono::duration_cast<std::chrono::seconds>(
                         __value.__time);
                   else if constexpr (std::is_same_v<_Tp, std::string>)
                     // For a named rule based continuation the SAVE depends on
                     // the RULE active at the end. This should be determined
                     // separately.
                     return std::chrono::seconds{0};
                   else
                     static_assert(False<_Tp>::value);

                   throw std::runtime_error("unreachable");
                 },
                 __continuation.__rules);

    case facebook::velox::tzdb::__clock::__universal:
      return __continuation.__at.__time;

    case facebook::velox::tzdb::__clock::__standard:
      return __continuation.__at.__time - __continuation.__stdoff;
  }
  throw std::runtime_error("unreachable");
}

[[nodiscard]] static date::year_month_day __to_year_month_day(
    date::year __year,
    date::month __month,
    facebook::velox::tzdb::__on __on) {
  return std::visit(
      [&](const auto& __value) {
        using _Tp = std::decay_t<decltype(__value)>;
        if constexpr (std::is_same_v<_Tp, date::day>)
          return date::year_month_day{__year, __month, __value};
        else if constexpr (std::is_same_v<_Tp, date::weekday_last>)
          return date::year_month_day{static_cast<date::sys_days>(
              date::year_month_weekday_last{__year, __month, __value})};
        else if constexpr (std::is_same_v<_Tp, __constrained_weekday>)
          return __value(__year, __month);
        else
         static_assert(False<_Tp>::value);

        throw std::runtime_error("unreachable");
      },
      __on);
}

[[nodiscard]] static date::sys_seconds __until_to_sys_seconds(
    const facebook::velox::tzdb::__continuation& __continuation) {
  // Does UNTIL contain the magic value for the last continuation?
  if (__continuation.__year == date::year::min())
    return date::sys_seconds::max();

  date::year_month_day __ymd = __to_year_month_day(
      __continuation.__year, __continuation.__in, __continuation.__on);
  return __to_sys_seconds(__ymd, __at_to_sys_seconds(__continuation));
}

// Holds the UNTIL time for a continuation with a named rule.
//
// Unlike continuations with an fixed SAVE named rules have a variable SAVE.
// This means when the UNTIL uses the local wall time the actual UNTIL value can
// only be determined when the SAVE is known. This class holds that abstraction.
class __named_rule_until {
 public:
  explicit __named_rule_until(const facebook::velox::tzdb::__continuation& __continuation)
      : __until_{__until_to_sys_seconds(__continuation)},
        __needs_adjustment_{
            // The last continuation of a ZONE has no UNTIL which basically is
            // until the end of _local_ time. This value is expressed by
            // sys_seconds::max(). Subtracting the SAVE leaves large value.
            // However SAVE can be negative, which would add a value to maximum
            // leading to undefined behaviour. In practice this often results in
            // an overflow to a very small value.
            __until_ != date::sys_seconds::max() &&
            __continuation.__at.__clock ==
                facebook::velox::tzdb::__clock::__local} {}

  // Gives the unadjusted until value, this is useful when the SAVE is not known
  // at all.
  date::sys_seconds __until() const noexcept {
    return __until_;
  }

  bool __needs_adjustment() const noexcept {
    return __needs_adjustment_;
  }

  // Returns the UNTIL adjusted for SAVE.
  date::sys_seconds operator()(std::chrono::seconds __save) const noexcept {
    return __until_ - __needs_adjustment_ * __save;
  }

 private:
  date::sys_seconds __until_;
  bool __needs_adjustment_;
};

[[nodiscard]] static std::chrono::seconds __at_to_seconds(
    std::chrono::seconds __stdoff,
    const __rule& __rule) {
  switch (__rule.__at.__clock) {
    case facebook::velox::tzdb::__clock::__local:
      // Local time and standard time behave the same. This is not
      // correct. Local time needs to adjust for the current saved time.
      // To know the saved time the rules need to be known and sorted.
      // This needs a time so to avoid the chicken and egg adjust the
      // saving of the local time later.
      return __rule.__at.__time - __stdoff;

    case facebook::velox::tzdb::__clock::__universal:
      return __rule.__at.__time;

    case facebook::velox::tzdb::__clock::__standard:
      return __rule.__at.__time - __stdoff;
  }
  throw std::runtime_error("unreachable");
}

[[nodiscard]] static date::sys_seconds __from_to_sys_seconds(
    std::chrono::seconds __stdoff,
    const __rule& __rule,
    date::year __year) {
  date::year_month_day __ymd =
      __to_year_month_day(__year, __rule.__in, __rule.__on);

  std::chrono::seconds __at = __at_to_seconds(__stdoff, __rule);
  return __to_sys_seconds(__ymd, __at);
}

[[nodiscard]] static date::sys_seconds __from_to_sys_seconds(
    std::chrono::seconds __stdoff,
    const __rule& __rule) {
  return __from_to_sys_seconds(__stdoff, __rule, __rule.__from);
}

[[nodiscard]] static const std::vector<__rule>& __get_rules(
    const __rules_storage_type& __rules_db,
    const std::string& __rule_name) {
  auto __result = __binary_find(__rules_db, __rule_name);
  if (__result == std::end(__rules_db))
    std::__throw_runtime_error(
        ("corrupt tzdb: rule '" + __rule_name + " 'does not exist").c_str());

  return __result->second;
}

// Returns the letters field for a time before the first rule.
//
// Per https://data.iana.org/time-zones/tz-how-to.html
// One wrinkle, not fully explained in zic.8.txt, is what happens when switching
// to a named rule. To what values should the SAVE and LETTER data be
// initialized?
//
// 1 If at least one transition has happened, use the SAVE and LETTER data from
//   the most recent.
// 2 If switching to a named rule before any transition has happened, assume
//   standard time (SAVE zero), and use the LETTER data from the earliest
//   transition with a SAVE of zero.
//
// This function implements case 2.
[[nodiscard]] static std::string __letters_before_first_rule(
    const std::vector<__rule>& __rules) {
  for (const auto& __rule : __rules) {
    if (__rule.__save.__time != 0s) {
      continue;
    }

    return __rule.__letters;
  }

  std::__throw_runtime_error("corrupt tzdb: rule has zero entries");
}

// Determines the information based on the continuation and the rules.
//
// There are several special cases to take into account
//
// === Entries before the first rule becomes active ===
// Asia/Hong_Kong
//   9 - JST 1945 N 18 2        // (1)
//   8 HK HK%sT                 // (2)
//   R HK 1946 o - Ap 21 0 1 S  // (3)
// There (1) is active until Novemer 18th 1945 at 02:00, after this time
// (2) becomes active. The first rule entry for HK (3) becomes active
// from April 21st 1945 at 01:00. In the period between (2) is active.
// This entry has an offset.
// This entry has no save, letters, or dst flag. So in the period
// after (1) and until (3) no rule entry is associated with the time.

[[nodiscard]] static sys_info __get_sys_info_before_first_rule(
    date::sys_seconds __begin,
    date::sys_seconds __end,
    const facebook::velox::tzdb::__continuation& __continuation,
    const std::vector<__rule>& __rules) {
  return sys_info{
      __begin,
      __end,
      __continuation.__stdoff,
      std::chrono::minutes(0),
      __format(__continuation, __letters_before_first_rule(__rules), 0s)};
}

// Returns the sys_info object for a time before the first rule.
// When this first rule has a SAVE of 0s the sys_info for the time before the
// first rule and for the first rule are identical and will be merged.
[[nodiscard]] static sys_info __get_sys_info_before_first_rule(
    date::sys_seconds __begin,
    date::sys_seconds __rule_end, // The end used when SAVE != 0s
    date::sys_seconds
        __next_end, // The end used when SAVE == 0s the times are merged
    const facebook::velox::tzdb::__continuation& __continuation,
    const std::vector<__rule>& __rules,
    std::vector<__rule>::const_iterator __rule) {
  if (__rule->__save.__time != 0s)
    return __get_sys_info_before_first_rule(
        __begin, __rule_end, __continuation, __rules);

  return sys_info{
      __begin,
      __next_end,
      __continuation.__stdoff,
      0min,
      __format(__continuation, __rule->__letters, 0s)};
}

[[nodiscard]] static std::chrono::seconds __at_to_seconds(
    std::chrono::seconds __stdoff,
    std::chrono::seconds __save,
    const __rule& __rule) {
  switch (__rule.__at.__clock) {
    case facebook::velox::tzdb::__clock::__local:
      return __rule.__at.__time - __stdoff - __save;

    case facebook::velox::tzdb::__clock::__universal:
      return __rule.__at.__time;

    case facebook::velox::tzdb::__clock::__standard:
      return __rule.__at.__time - __stdoff;
  }
  throw std::runtime_error("unreachable");
}

[[nodiscard]] static date::sys_seconds __rule_to_sys_seconds(
    std::chrono::seconds __stdoff,
    std::chrono::seconds __save,
    const __rule& __rule,
    date::year __year) {
  date::year_month_day __ymd =
      __to_year_month_day(__year, __rule.__in, __rule.__on);

  std::chrono::seconds __at = __at_to_seconds(__stdoff, __save, __rule);
  return __to_sys_seconds(__ymd, __at);
}

// Returns the first rule after __time.
// Note that a rule can be "active" in multiple years, this may result in an
// infinite loop where the same rule is returned every time, use __current to
// guard against that.
//
// When no next rule exists the returned time will be sys_seconds::max(). This
// can happen in practice. For example,
//
//   R So 1945 o - May 24 2 2 M
//   R So 1945 o - S 24 3 1 S
//   R So 1945 o - N 18 2s 0 -
//
// Has 3 rules that are all only active in 1945.
[[nodiscard]] static std::
    pair<date::sys_seconds, std::vector<__rule>::const_iterator>
    __next_rule(
        date::sys_seconds __time,
        std::chrono::seconds __stdoff,
        std::chrono::seconds __save,
        const std::vector<__rule>& __rules,
        std::vector<__rule>::const_iterator __current) {
  date::year __year =
      date::year_month_day{std::chrono::floor<date::days>(__time)}.year();

  // Note it would probably be better to store the pairs in a vector and then
  // use min() to get the smallest element
  std::map<date::sys_seconds, std::vector<__rule>::const_iterator> __candidates;
  // Note this evaluates all rules which is a waste of effort; when the entries
  // are beyond the current year's "next year" (where "next year" is not always
  // year + 1) the algorithm should end.
  for (auto __it = __rules.begin(); __it != __rules.end(); ++__it) {
    for (date::year __y = std::max(__year - date::years(1), __it->__from); __y <= __it->__to; ++__y) {
      // Adding the current entry for the current year may lead to infinite
      // loops due to the SAVE adjustment. Skip these entries.
      if (__y == __year && __it == __current)
        continue;

      date::sys_seconds __t =
          __rule_to_sys_seconds(__stdoff, __save, *__it, __y);
      if (__t <= __time)
        continue;

      if (__candidates.count(__t) > 0) {
        throw std::runtime_error("duplicated rule");
      }
      __candidates[__t] = __it;
      break;
    }
  }

  if (!__candidates.empty()) [[likely]] {
    auto __it = __candidates.begin();

    // When no rule is selected the time before the first rule and the first
    // rule should not be merged.
    if (__time == date::sys_seconds::min())
      return *__it;

    // There can be two constitutive rules that are the same. For example,
    // Hong Kong
    //
    // R HK 1973 o - D 30 3:30 1 S          (R1)
    // R HK 1965 1976 - Ap Su>=16 3:30 1 S  (R2)
    //
    // 1973-12-29 19:30:00 R1 becomes active.
    // 1974-04-20 18:30:00 R2 becomes active.
    // Both rules have a SAVE of 1 hour and LETTERS are S for both of them.
    while (__it != __candidates.end()) {
      if (__current->__save.__time != __it->second->__save.__time ||
          __current->__letters != __it->second->__letters)
        return *__it;

      ++__it;
    }
  }

  return {date::sys_seconds::max(), __rules.end()};
}

// Returns the first rule of a set of rules.
// This is not always the first of the listed rules. For example
//   R Sa 2008 2009 - Mar Su>=8 0 0 -
//   R Sa 2007 2008 - O Su>=8 0 1 -
// The transition in October 2007 happens before the transition in March 2008.
[[nodiscard]] static std::vector<__rule>::const_iterator __first_rule(
    std::chrono::seconds __stdoff,
    const std::vector<__rule>& __rules) {
  return __next_rule(
             date::sys_seconds::min(), __stdoff, 0s, __rules, __rules.end())
      .second;
}

[[nodiscard]] static __sys_info_result __get_sys_info_rule(
    date::sys_seconds __time,
    date::sys_seconds __continuation_begin,
    const facebook::velox::tzdb::__continuation& __continuation,
    const std::vector<__rule>& __rules) {
  auto __rule = __first_rule(__continuation.__stdoff, __rules);
  if (__rule == __rules.end()) {
    throw std::runtime_error("the set of rules has no first rule");
  }

  // Avoid selecting a time before the start of the continuation
  __time = std::max(__time, __continuation_begin);

  date::sys_seconds __rule_begin =
      __from_to_sys_seconds(__continuation.__stdoff, *__rule);

  // The time sought is very likely inside the current rule.
  // When the continuation's UNTIL uses the local clock there are edge cases
  // where this is not true.
  //
  // Start to walk the rules to find the proper one.
  //
  // For now we just walk all the rules TODO TZDB investigate whether a smarter
  // algorithm would work.
  auto __next = __next_rule(
      __rule_begin,
      __continuation.__stdoff,
      __rule->__save.__time,
      __rules,
      __rule);

  // Ignore small steps, this happens with America/Punta_Arenas for the
  // transition
  // -4:42:46 - SMT 1927 S
  // -5 x -05/-04 1932 S
  // ...
  //
  // R x 1927 1931 - S 1 0 1 -
  // R x 1928 1932 - Ap 1 0 0 -
  //
  // America/Punta_Arenas  Thu Sep  1 04:42:45 1927 UT = Thu Sep  1 00:42:45
  // 1927 -04 isdst=1 gmtoff=-14400 America/Punta_Arenas  Sun Apr  1 03:59:59
  // 1928 UT = Sat Mar 31 23:59:59 1928 -04 isdst=1 gmtoff=-14400
  // America/Punta_Arenas  Sun Apr  1 04:00:00 1928 UT = Sat Mar 31 23:00:00
  // 1928 -05 isdst=0 gmtoff=-18000
  //
  // Without this there will be a transition
  //   [1927-09-01 04:42:45, 1927-09-01 05:00:00) -05:00:00 0min -05

  if (date::sys_seconds __begin =
          __rule->__save.__time != 0s ? __rule_begin : __next.first;
      __time < __begin) {
    if (__continuation_begin == date::sys_seconds::min() ||
        __begin - __continuation_begin > 12h)
      return __sys_info{
          __get_sys_info_before_first_rule(
              __continuation_begin,
              __rule_begin,
              __next.first,
              __continuation,
              __rules,
              __rule),
          false};

    // Europe/Berlin
    // 1 c CE%sT 1945 May 24 2          (C1)
    // 1 So CE%sT 1946                  (C2)
    //
    // R c 1944 1945 - Ap M>=1 2s 1 S   (R1)
    //
    // R So 1945 o - May 24 2 2 M       (R2)
    //
    // When C2 becomes active the time would be before the first rule R2,
    // giving a 1 hour sys_info.
    std::chrono::seconds __save = __rule->__save.__time;
    __named_rule_until __continuation_end{__continuation};
    date::sys_seconds __sys_info_end =
        std::min(__continuation_end(__save), __next.first);

    return __sys_info{
        sys_info{
            __continuation_begin,
            __sys_info_end,
            __continuation.__stdoff + __save,
            std::chrono::duration_cast<std::chrono::minutes>(__save),
            __format(__continuation, __rule->__letters, __save)},
        __sys_info_end == __continuation_end(__save)};
  }

  // See above for America/Asuncion
  if (__rule->__save.__time == 0s && __time < __next.first) {
    return __sys_info{
        sys_info{
            __continuation_begin,
            __next.first,
            __continuation.__stdoff,
            0min,
            __format(__continuation, __rule->__letters, 0s)},
        false};
  }

  if (__rule->__save.__time != 0s) {
    // another fix for America/Punta_Arenas when not at the start of the
    // sys_info object.
    std::chrono::seconds __save = __rule->__save.__time;
    if (__continuation_begin >= __rule_begin - __save &&
        __time < __next.first) {
      return __sys_info{
          sys_info{
              __continuation_begin,
              __next.first,
              __continuation.__stdoff + __save,
              std::chrono::duration_cast<std::chrono::minutes>(__save),
              __format(__continuation, __rule->__letters, __save)},
          false};
    }
  }

  date::year year =
      date::year_month_day{std::chrono::floor<date::days>(__time)}.year();

  // Some time zones have rules that handle annual transitions
  // like daylight savings time that apply forever (no end). This can
  // make using the loop below really slow for years far in the
  // future. This handles jumping ahead to the year before __time
  // in such cases.

  // Make sure the continuation goes forever (otherwise we'll end early anyway).
  // Make sure the year is at least 1900, this prevents underflows
  // there are no rules that start prior to 1900 that have no end.
  if (__continuation.__year == date::year::min() && year >= date::year(1900)) {
    // Use the time from 2 years prior and check if forever rules
    // applied at that time
    year -= date::years(2);
    size_t numForeverRules = 0;
    // Currently there is no rule that has more than 2 forever
    // rules that alternate.
    std::pair<std::vector<tzdb::__rule>::const_iterator, std::vector<tzdb::__rule>::const_iterator> foreverRules{};
    bool isInForeverRules = true;
    for (auto iter = __rules.cbegin(); iter < __rules.cend(); ++iter) {
      if (iter->__to == date::year::max() && iter->__from <= year) {
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
      } else {
        if (iter->__to >= year) {
          // We found a non-forever rule that still applies in year.
          // In this case we can't skip forward (nor do we need to).
          isInForeverRules = false;
          break;
        }
      }
    }

    // If only forever rules applied 3 years ago and there were
    // exactly 2, then skip forward.
    if (isInForeverRules && numForeverRules == 2) {
      // We know that the forever rules applied 2 years ago, so if
      // we look at the times the rules transitioned 1 year ago we
      // know the local time prior to that transition was the other
      // forever rule.
      // We use 1 year ago to be conservative and because it simplifies
      // things (we only need to look at transitions in that year).
      date::year transitionYear = date::year_month_day{std::chrono::floor<date::days>(__time)}.year() - date::years(1);
      date::sys_seconds firstTransition = 
            __rule_to_sys_seconds(__continuation.__stdoff, std::get<1>(foreverRules)->__save.__time, *std::get<0>(foreverRules), transitionYear);
      date::sys_seconds secondTransition =
            __rule_to_sys_seconds(__continuation.__stdoff, std::get<0>(foreverRules)->__save.__time, *std::get<1>(foreverRules), transitionYear);

      // Pick whichever transition happened earlier in transition year.
      // Then we set the current rule to be the rule with the earlier
      // transition, and we set next rule to be the other rule with
      // a start time of the later transition.
      // This effectively skips us ahead to ~2 years prior to our target
      // time.
      if (firstTransition < secondTransition) {
        __rule = std::get<0>(foreverRules);
        __rule_begin = firstTransition;
        __next = __next_rule(
          __rule_begin,
          __continuation.__stdoff,
          __rule->__save.__time,
          __rules,
          __rule);
      } else {
        __rule = std::get<1>(foreverRules);
        __rule_begin = secondTransition;
        __next = __next_rule(
          __rule_begin,
          __continuation.__stdoff,
          __rule->__save.__time,
          __rules,
          __rule);
      }
    }
  }

  __named_rule_until __continuation_end{__continuation};
  while (__next.second != __rules.end()) {
    date::sys_seconds __end = __continuation_end(__rule->__save.__time);

    date::sys_seconds __sys_info_begin =
        std::max(__continuation_begin, __rule_begin);
    date::sys_seconds __sys_info_end = std::min(__end, __next.first);
    std::chrono::seconds __diff =
        std::chrono::abs(__sys_info_end - __sys_info_begin);

    if (__diff < 12h) {
      // Z America/Argentina/Buenos_Aires -3:53:48 - LMT 1894 O 31
      // -4:16:48 - CMT 1920 May
      // -4 - -04 1930 D
      // -4 A -04/-03 1969 O 5
      // -3 A -03/-02 1999 O 3
      // -4 A -04/-03 2000 Mar 3
      // ...
      //
      // ...
      // R A 1989 1992 - O Su>=15 0 1 -
      // R A 1999 o - O Su>=1 0 1 -
      // R A 2000 o - Mar 3 0 0 -
      // R A 2007 o - D 30 0 1 -
      // ...

      // The 1999 switch uses the same rule, but with a different stdoff.
      //   R A 1999 o - O Su>=1 0 1 -
      //     stdoff -3 -> 1999-10-03 03:00:00
      //     stdoff -4 -> 1999-10-03 04:00:00
      // This generates an invalid entry and this is evaluated as a transition.
      // Looking at the zdump like output in libc++ this generates jumps in
      // the UTC time.

      __rule = __next.second;
      __next = __next_rule(
          __next.first,
          __continuation.__stdoff,
          __rule->__save.__time,
          __rules,
          __rule);
      __end = __continuation_end(__rule->__save.__time);
      __sys_info_end = std::min(__end, __next.first);
    }

    if ((__time >= __rule_begin && __time < __next.first) ||
        __next.first >= __end) {
      __sys_info_begin = std::max(__continuation_begin, __rule_begin);
      __sys_info_end = std::min(__end, __next.first);

      return __sys_info{
          sys_info{
              __sys_info_begin,
              __sys_info_end,
              __continuation.__stdoff + __rule->__save.__time,
              std::chrono::duration_cast<std::chrono::minutes>(
                  __rule->__save.__time),
              __format(
                  __continuation, __rule->__letters, __rule->__save.__time)},
          __sys_info_end == __end};
    }

    __rule_begin = __next.first;
    __rule = __next.second;
    __next = __next_rule(
        __rule_begin,
        __continuation.__stdoff,
        __rule->__save.__time,
        __rules,
        __rule);
  }

  return __sys_info{
      sys_info{
          std::max(__continuation_begin, __rule_begin),
          __continuation_end(__rule->__save.__time),
          __continuation.__stdoff + __rule->__save.__time,
          std::chrono::duration_cast<std::chrono::minutes>(
              __rule->__save.__time),
          __format(__continuation, __rule->__letters, __rule->__save.__time)},
      true};
}

[[nodiscard]] static __sys_info_result __get_sys_info_basic(
    date::sys_seconds __time,
    date::sys_seconds __continuation_begin,
    const facebook::velox::tzdb::__continuation& __continuation,
    std::chrono::seconds __save) {
  date::sys_seconds __continuation_end = __until_to_sys_seconds(__continuation);
  return __sys_info{
      sys_info{
          __continuation_begin,
          __continuation_end,
          __continuation.__stdoff + __save,
          std::chrono::duration_cast<std::chrono::minutes>(__save),
          __format(__continuation, __continuation.__format, __save)},
      true};
}

[[nodiscard]] static __sys_info_result __get_sys_info(
    date::sys_seconds __time,
    date::sys_seconds __continuation_begin,
    const facebook::velox::tzdb::__continuation& __continuation,
    const __rules_storage_type& __rules_db) {
  return std::visit(
      [&](const auto& __value) {
        using _Tp = std::decay_t<decltype(__value)>;
        if constexpr (std::is_same_v<_Tp, std::string>)
          return __get_sys_info_rule(
              __time,
              __continuation_begin,
              __continuation,
              __get_rules(__rules_db, __value));
        else if constexpr (std::is_same_v<_Tp, std::monostate>)
          return __get_sys_info_basic(
              __time,
              __continuation_begin,
              __continuation,
              std::chrono::seconds(0));
        else if constexpr (std::is_same_v<_Tp, facebook::velox::tzdb::__save>)
          return __get_sys_info_basic(
              __time, __continuation_begin, __continuation, __value.__time);
        else
          static_assert(False<_Tp>::value);

        throw std::runtime_error("unreachable");
      },
      __continuation.__rules);
}

// The transition from one continuation to the next continuation may result in
// two constitutive continuations with the same "offset" information.
// [time.zone.info.sys]/3
//   The begin and end data members indicate that, for the associated time_zone
//   and time_point, the offset and abbrev are in effect in the range
//   [begin, end). This information can be used to efficiently iterate the
//   transitions of a time_zone.
//
// Note that this does considers a change in the SAVE field not to be a
// different sys_info, zdump does consider this different.
//   LWG XXXX The sys_info range should be affected by save
// matches the behaviour of the Standard and zdump.
//
// Iff the "offsets" are the same '__current.__end' is replaced with
// '__next.__end', which effectively merges the two objects in one object. The
// function returns true if a merge occurred.
[[nodiscard]] bool __merge_continuation(
    sys_info& __current,
    const sys_info& __next) {
  if (__current.end != __next.begin)
    return false;

  if (__current.offset != __next.offset || __current.abbrev != __next.abbrev ||
      __current.save != __next.save)
    return false;

  __current.end = __next.end;
  return true;
}

//===----------------------------------------------------------------------===//
//                           Public API
//===----------------------------------------------------------------------===//

[[nodiscard]] time_zone time_zone::__create(
    std::unique_ptr<time_zone::__impl>&& __p) {
  if (__p == nullptr) {
    throw std::runtime_error(
        "initialized time_zone without a valid pimpl object");
  }
  time_zone result;
  result.__impl_ = std::move(__p);

  // If there is more than one continuation and the second to last continuation
  // uses a string rule, make sure we have all the transitions populated for at
  // least 2 years after the start of the last continuation.
  //
  // Note that the end of the second to last continuation is the start of the last
  // continuation, and the last continuation is used forever.
  const auto& continuations = result.__impl_->__continuations();
  auto& transitions = result.__impl_->transitions();
  auto& ttinfos = result.__impl_->ttinfos();
  if (continuations.size() > 1 &&
      (std::holds_alternative<std::string>(continuations[continuations.size() - 2].__rules) || std::holds_alternative<std::string>(continuations[continuations.size() - 1].__rules))) {
    const auto endTime = __until_to_sys_seconds(continuations[continuations.size() - 2]) + date::years(2);
    if (endTime > transitions.back().timepoint) {
      auto sys_info = result.__get_info_to_populate_transition(transitions.back().timepoint);
      auto max = date::sys_seconds::max();
      while ((sys_info.end < max) && (endTime > transitions.back().timepoint)) {
        sys_info = result.__get_info_to_populate_transition(sys_info.end);
        ttinfos.emplace_back(sys_info.offset, sys_info.abbrev, sys_info.save > std::chrono::minutes(0));
        transitions.emplace_back(sys_info.end, &ttinfos.back());
      }
    }
  }

  // If the last continuation uses forever rules (rules that alternate every year,
  // like daylight savings time) make sure we have all the transitions populated for
  // at least 2 years after the start of the forever rules.
  if (continuations.back().__has_forever_rules) { 
    const auto& foreverRules = continuations.back().__forever_rules;   
    const auto endTime = __from_to_sys_seconds(continuations.back().__stdoff, *std::get<1>(foreverRules), std::get<0>(foreverRules)->__from + date::years(2));
    if (transitions.back().timepoint < endTime) {
    auto sys_info = result.__get_info_to_populate_transition(transitions.back().timepoint);
      while (transitions.back().timepoint < endTime) {
        sys_info = result.__get_info_to_populate_transition(sys_info.end);
        ttinfos.emplace_back(sys_info.offset, sys_info.abbrev, sys_info.save > std::chrono::minutes(0));
        transitions.emplace_back(sys_info.end, &ttinfos.back());
      }
    }
  }

  return result;
}

time_zone::~time_zone() = default;

[[nodiscard]] std::string_view time_zone::__name() const noexcept {
  return __impl_->__name();
}

sys_info
time_zone::load_sys_info(std::vector<date::transition>::const_iterator i) const
{
    using namespace std::chrono;
    const auto& transitions = __impl_->transitions();
    assert(!transitions.empty());
    assert(i != transitions.begin());

    sys_info r;
    r.begin = i[-1].timepoint;
    r.end = i != transitions.end() ? i->timepoint :
                                      date::sys_seconds(date::sys_days(date::year::max()/date::December/31));
    r.offset = i[-1].info->offset;
    r.save = i[-1].info->is_dst ? minutes{1} : minutes{0};
    r.abbrev = i[-1].info->abbrev;
    return r;
}

[[nodiscard]] sys_info time_zone::__get_info_to_populate_transition(date::sys_seconds __time) const {
  // This function could be way more efficient, but we only use it when initially
  // populating the time zone database.
  const auto& transitions = __impl_->transitions();
  const auto iter = upper_bound(transitions.begin(), transitions.end(), __time,
                                     [](const date::sys_seconds& x, const date::transition& t)
                                     {
                                         return x < t.timepoint;
                                     });
  if (iter != transitions.begin() && iter != transitions.end()) {
    return load_sys_info(iter);
  }
  
  std::optional<sys_info> __result;
  bool __valid_result =
      false; // true iff __result.has_value() is true and
             // __result.begin <= __time < __result.end is true.
  bool __can_merge = false;
  date::sys_seconds __continuation_begin = date::sys_seconds::min();
  // Iterates over the Zone entry and its continuations. Internally the Zone
  // entry is split in a Zone information and the first continuation. The last
  // continuation has no UNTIL field. This means the loop should always find a
  // continuation.
  //
  // For more information on background of zone information please consult the
  // following information
  //   [zic manual](https://www.man7.org/linux/man-pages/man8/zic.8.html)
  //   [tz source info](https://data.iana.org/time-zones/tz-how-to.html)
  //   On POSIX systems the zdump tool can be useful:
  //     zdump -v Asia/Hong_Kong
  //   Gives all transitions in the Hong Kong time zone.
  //
  // During iteration the result for the current continuation is returned. If
  // no continuation is applicable it will return the end time as "error". When
  // two continuations are contiguous and contain the "same" information these
  // ranges are merged as one range.
  // The merging requires keeping any result that occurs before __time,
  // likewise when a valid result is found the algorithm needs to test the next
  // continuation to see whether it can be merged. For example, Africa/Ceuta
  // Continuations
  //  0 s WE%sT 1929                   (C1)
  //  0 - WET 1967                     (C2)
  //  0 Sp WE%sT 1984 Mar 16           (C3)
  //
  // Rules
  //  R s 1926 1929 - O Sa>=1 24s 0 -  (R1)
  //
  //  R Sp 1967 o - Jun 3 12 1 S       (R2)
  //
  // The rule R1 is the last rule used in C1. The rule R2 is the first rule in
  // C3. Since R2 is the first rule this means when a continuation uses this
  // rule its value prior to R2 will be SAVE 0 LETTERS of the first entry with a
  // SAVE of 0, in this case WET.
  // This gives the following changes in the information.
  //   1928-10-07 00:00:00 C1 R1 becomes active: offset 0 save 0 abbrev WET
  //   1929-01-01 00:00:00 C2    becomes active: offset 0 save 0 abbrev WET
  //   1967-01-01 00:00:00 C3    becomes active: offset 0 save 0 abbrev WET
  //   1967-06-03 12:00:00 C3 R2 becomes active: offset 0 save 1 abbrev WEST
  //
  // The first 3 entries are contiguous and contain the same information, this
  // means the period [1928-10-07 00:00:00, 1967-06-03 12:00:00) should be
  // returned in one sys_info object.

  const auto& __continuations = __impl_->__continuations();
  const __rules_storage_type& __rules_db = __impl_->__rules_db();
  for (auto __it = __continuations.begin(); __it != __continuations.end();
       ++__it) {
    const auto& __continuation = *__it;

    if (date::year_month_day{std::chrono::floor<date::days>(__named_rule_until(__continuation).__until())}.year() + date::years(1) <  date::year_month_day{std::chrono::floor<date::days>(__time)}.year()) {
      continue;
    }

    __sys_info_result __sys_info = __get_sys_info(
        __time, __continuation_begin, __continuation, __rules_db);

    if (__sys_info) {
      if (__sys_info->__info.begin >= __sys_info->__info.end) {
        throw std::runtime_error("invalid sys_info range");
      }

      // Filters out dummy entries
      // Z America/Argentina/Buenos_Aires -3:53:48 - LMT 1894 O 31
      // ...
      // -4 A -04/-03 2000 Mar 3 (C1)
      // -3 A -03/-02            (C2)
      //
      // ...
      // R A 2000 o - Mar 3 0 0 -
      // R A 2007 o - D 30 0 1 -
      // ...
      //
      // This results in an entry
      //   [2000-03-03 03:00:00, 2000-03-03 04:00:00) -10800s 60min -03
      // for [C1 & R1, C1, R2) which due to the end of the continuation is an
      // one hour "sys_info". Instead the entry should be ignored and replaced
      // by [C2 & R1, C2 & R2) which is the proper range
      //   "[2000-03-03 03:00:00, 2007-12-30 03:00:00) -02:00:00 60min -02

      if (std::holds_alternative<std::string>(__continuation.__rules) &&
          __sys_info->__can_merge &&
          __sys_info->__info.begin + 12h > __sys_info->__info.end) {
        __continuation_begin = __sys_info->__info.begin;
        continue;
      }

      if (!__result) {
        // First entry found, always keep it.
        __result = __sys_info->__info;

        __valid_result = __time >= __result->begin && __time < __result->end;
        __can_merge = __sys_info->__can_merge;
      } else if (
          __can_merge && __merge_continuation(*__result, __sys_info->__info)) {
        // The results are merged, update the result state. This may
        // "overwrite" a valid sys_info object with another valid sys_info
        // object.
        __valid_result = __time >= __result->begin && __time < __result->end;
        __can_merge = __sys_info->__can_merge;
      } else {
        // Here things get interesting:
        // For example, America/Argentina/San_Luis
        //
        //   -3 A -03/-02 2008 Ja 21           (C1)
        //   -4 Sa -04/-03 2009 O 11           (C2)
        //
        //   R A 2007 o - D 30 0 1 -           (R1)
        //
        //   R Sa 2007 2008 - O Su>=8 0 1 -    (R2)
        //
        // Based on C1 & R1 the end time of C1 is 2008-01-21 03:00:00
        // Based on C2 & R2 the end time of C1 is 2008-01-21 02:00:00
        // In this case the earlier time is the real time of the transition.
        // However the algorithm used gives 2008-01-21 03:00:00.
        //
        // So we need to calculate the previous UNTIL in the current context and
        // see whether it's earlier.

        // The results could not be merged.
        // - When we have a valid result that result is the final result.
        // - Otherwise the result we had is before __time and the result we got
        //   is at a later time (possibly valid). This result is always better
        //   than the previous result.
        if (__valid_result) {
          return *__result;
        } else {
          if (__it == __continuations.begin()) {
            throw std::runtime_error(
                "the first rule should always seed the result");
          }
          const auto& __last = *(__it - 1);
          if (std::holds_alternative<std::string>(__last.__rules)) {
            // Europe/Berlin
            // 1 c CE%sT 1945 May 24 2          (C1)
            // 1 So CE%sT 1946                  (C2)
            //
            // R c 1944 1945 - Ap M>=1 2s 1 S   (R1)
            //
            // R So 1945 o - May 24 2 2 M       (R2)
            //
            // When C2 becomes active the time would be before the first rule
            // R2, giving a 1 hour sys_info. This is not valid and the results
            // need merging.

            if (__result->end != __sys_info->__info.begin) {
              // When the UTC gap between the rules is due to the change of
              // offsets adjust the new time to remove the gap.
              date::sys_seconds __end = __result->end - __result->offset;
              date::sys_seconds __begin =
                  __sys_info->__info.begin - __sys_info->__info.offset;
              if (__end == __begin) {
                __sys_info->__info.begin = __result->end;
              }
            }
          }

          __result = __sys_info->__info;
          __valid_result = __time >= __result->begin && __time < __result->end;
          __can_merge = __sys_info->__can_merge;
        }
      }
      __continuation_begin = __result->end;
    } else {
      __continuation_begin = __sys_info.error();
    }
  }
  if (__valid_result)
    return *__result;

  std::__throw_runtime_error("tzdb: corrupt db");
}

[[nodiscard]] sys_info time_zone::__get_info(date::sys_seconds __time) const {
  const auto& transitions = __impl_->transitions();
  auto iter = upper_bound(transitions.begin(), transitions.end(), __time,
                                     [](const date::sys_seconds& x, const date::transition& t)
                                     {
                                         return x < t.timepoint;
                                     });

  if (iter == transitions.begin()) {
    return sys_info{
      date::sys_days(date::year::min()/date::January/1),
      iter + 1 != transitions.end() ? iter[1].timepoint :
                                      date::sys_seconds(date::sys_days(date::year::max()/date::December/31)),
      iter[0].info->offset,
      iter[0].info->is_dst ? std::chrono::minutes{1} : std::chrono::minutes{0},
      iter[0].info->abbrev
    };
  }

  if (iter == transitions.end() && __impl_->__continuations().back().__has_forever_rules) {
    const auto& continuation = __impl_->__continuations().back();
    const auto& foreverRules = continuation.__forever_rules;
    date::year transitionYear = date::year_month_day{std::chrono::floor<date::days>(__time)}.year() - date::years(1);
    date::sys_seconds firstTransition = 
          __rule_to_sys_seconds(continuation.__stdoff, std::get<1>(foreverRules)->__save.__time, *std::get<0>(foreverRules), transitionYear);
    date::sys_seconds secondTransition =
          __rule_to_sys_seconds(continuation.__stdoff, std::get<0>(foreverRules)->__save.__time, *std::get<1>(foreverRules), transitionYear);

    const std::vector<__rule>::const_iterator* currentRule;
    const std::vector<__rule>::const_iterator* nextRule;
    date::sys_seconds currentTransition;
    date::sys_seconds nextTransition;
    
    if (firstTransition < secondTransition) {
      currentRule = &std::get<0>(foreverRules);
      nextRule = &std::get<1>(foreverRules);
      currentTransition = firstTransition;
      nextTransition = secondTransition;
    } else {
      currentRule = &std::get<1>(foreverRules);
      nextRule = &std::get<0>(foreverRules);
      currentTransition = secondTransition;
      nextTransition = firstTransition;
    }

    while (__time >= nextTransition) {
      transitionYear++;

      for (int i = 0; i < 2 && __time >= nextTransition; i++) {
        currentTransition = nextTransition;
        nextTransition = __rule_to_sys_seconds(continuation.__stdoff, (*nextRule)->__save.__time, *(*currentRule), transitionYear);
        std::swap(currentRule, nextRule);
      }
    }

    return sys_info{
      currentTransition,
      nextTransition,
      continuation.__stdoff + (*currentRule)->__save.__time,
      std::chrono::duration_cast<std::chrono::minutes>(
                  (*currentRule)->__save.__time),
      __format(
                  continuation, (*currentRule)->__letters, (*currentRule)->__save.__time)
    };
  }

  return load_sys_info(iter);
}

// Is the "__local_time" present in "__first" and "__second". If so the
// local_info has an ambiguous result.
[[nodiscard]] static bool __is_ambiguous(
    date::local_seconds __local_time,
    const sys_info& __first,
    const sys_info& __second) {
  date::local_seconds __end_first{
      __first.end.time_since_epoch() + __first.offset};
  date::local_seconds __begin_second{
      __second.begin.time_since_epoch() + __second.offset};

  return __local_time < __end_first && __local_time >= __begin_second;
}

// Determines the result of the "__local_time". This expects the object
// "__first" to be earlier in time than "__second".
[[nodiscard]] static local_info __get_info(
    date::local_seconds __local_time,
    const sys_info& __first,
    const sys_info& __second) {
  date::local_seconds __end_first{
      __first.end.time_since_epoch() + __first.offset};
  date::local_seconds __begin_second{
      __second.begin.time_since_epoch() + __second.offset};

  if (__local_time < __end_first) {
    if (__local_time >= __begin_second)
      // |--------|
      //        |------|
      //         ^
      return {local_info::ambiguous, __first, __second};

    // |--------|
    //          |------|
    //         ^
    return {local_info::unique, __first, sys_info{}};
  }

  if (__local_time < __begin_second)
    // |--------|
    //             |------|
    //           ^
    return {local_info::nonexistent, __first, __second};

  // |--------|
  //          |------|
  //           ^
  return {local_info::unique, __second, sys_info{}};
}

[[nodiscard]] local_info time_zone::__get_info(
    date::local_seconds __local_time) const {
  std::chrono::seconds __local_seconds = __local_time.time_since_epoch();

  /* An example of a typical year with a DST switch displayed in local time.
   *
   * At the first of April the time goes forward one hour. This means the
   * time marked with ~~ is not a valid local time. This is represented by the
   * nonexistent value in local_info.result.
   *
   * At the first of November the time goes backward one hour. This means the
   * time marked with ^^ happens twice. This is represented by the ambiguous
   * value in local_info.result.
   *
   * 2020.11.01                  2021.04.01              2021.11.01
   * offset +05                  offset +05              offset +05
   * save    0s                  save    1h              save    0s
   * |------------//----------|
   *                             |---------//--------------|
   *                                                    |-------------
   *                           ~~                        ^^
   *
   * These shifts can happen due to changes in the current time zone for a
   * location. For example, Indian/Kerguelen switched only once. In 1950 from an
   * offset of 0 hours to an offset of +05 hours.
   *
   * During all these shifts the UTC time will not have gaps.
   */

  // The code needs to determine the system time for the local time. There is no
  // information available. Assume the offset between system time and local time
  // is 0s. This gives an initial estimate.
  date::sys_seconds __guess{__local_seconds};
  sys_info __info = __get_info(__guess);

  // At this point the offset can be used to determine an estimate for the local
  // time. Before doing that, determine the offset and validate whether the
  // local time is the range [chrono::local_seconds::min(),
  // chrono::local_seconds::max()).
  if (__local_seconds < 0s && __info.offset > 0s)
    if (__local_seconds - date::local_seconds::min().time_since_epoch() <
        __info.offset)
      return {-1, __info, {}};

  if (__local_seconds > 0s && __info.offset < 0s)
    if (date::local_seconds::max().time_since_epoch() - __local_seconds <
        -__info.offset)
      return {-2, __info, {}};

  // Based on the information found in the sys_info, the local time can be
  // converted to a system time. This resulting time can be in the following
  // locations of the sys_info:
  //
  //                             |---------//--------------|
  //                           1   2.1      2.2         2.3  3
  //
  // 1. The estimate is before the returned sys_info object.
  //    The result is either non-existent or unique in the previous sys_info.
  // 2. The estimate is in the sys_info object
  //    - If the sys_info begin is not sys_seconds::min(), then it might be at
  //      2.1 and could be ambiguous with the previous or unique.
  //    - If sys_info end is not sys_seconds::max(), then it might be at 2.3
  //      and could be ambiguous with the next or unique.
  //    - Else it is at 2.2 and always unique. This case happens when a
  //      time zone has no transitions. For example, UTC or GMT+1.
  // 3. The estimate is after the returned sys_info object.
  //    The result is either non-existent or unique in the next sys_info.
  //
  // There is no specification where the "middle" starts. Similar issues can
  // happen when sys_info objects are "short", then "unique in the next" could
  // become "ambiguous in the next and the one following". Theoretically there
  // is the option of the following time-line
  //
  // |------------|
  //           |----|
  //       |-----------------|
  //
  // However the local_info object only has 2 sys_info objects, so this option
  // is not tested.

  date::sys_seconds __sys_time{__local_seconds - __info.offset};
  if (__sys_time < __info.begin)
    // Case 1 before __info
    return facebook::velox::tzdb::__get_info(
        __local_time, __get_info(__info.begin - 1s), __info);

  if (__sys_time >= __info.end)
    // Case 3 after __info
    return facebook::velox::tzdb::__get_info(
        __local_time, __info, __get_info(__info.end));

  // Case 2 in __info
  if (__info.begin != date::sys_days(date::year::min()/date::January/1)) {
    // Case 2.1 Not at the beginning, when not ambiguous the result should test
    // case 2.3.
    sys_info __prev = __get_info(__info.begin - 1s);
    if (__is_ambiguous(__local_time, __prev, __info))
      return {local_info::ambiguous, __prev, __info};
  }

  if (__info.end == date::sys_days(date::year::max()/date::December/31))
    // At the end so it's case 2.2
    return {local_info::unique, __info, sys_info{}};

  // This tests case 2.2 or case 2.3.
  return facebook::velox::tzdb::__get_info(
      __local_time, __info, __get_info(__info.end));
}

} // namespace facebook::velox::tzdb
