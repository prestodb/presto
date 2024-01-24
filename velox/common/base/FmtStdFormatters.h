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

#include <fmt/format.h>
#if FMT_VERSION >= 100100
#include <fmt/std.h>
#endif

#include <atomic>
#include <bitset>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <vector>

template <typename Char>
struct fmt::formatter<std::errc, Char>
    : formatter<std::underlying_type<std::errc>::type, Char> {
  template <typename FormatContext>
  auto format(std::errc v, FormatContext& ctx) const -> decltype(ctx.out()) {
    using underlying_type = std::underlying_type<std::errc>::type;
    return formatter<underlying_type, Char>::format(
        static_cast<underlying_type>(v), ctx);
  }
};

#if FMT_VERSION < 100100
// This should be 100101 but FMT_VERSION was not bumped in 10.1.1
// but under a month has passed since 10.1.0 release so we can assume 10.1.1
//
// Backport from fmt 10.1.1 see fmtlib/fmt#3574
// Formats std::atomic
template <typename T, typename Char>
struct fmt::formatter<
    std::atomic<T>,
    Char,
    std::enable_if_t<fmt::is_formattable<T, Char>::value>>
    : formatter<T, Char> {
  template <typename FormatContext>
  auto format(const std::atomic<T>& v, FormatContext& ctx) const
      -> decltype(ctx.out()) {
    return formatter<T, Char>::format(v.load(), ctx);
  }
};
#endif

#if FMT_VERSION < 100100
// Backport from fmt 10.1 see fmtlib/fmt#3570
// Formats std::vector<bool>
namespace fmt::detail {
template <typename T, typename Enable = void>
struct has_flip : std::false_type {};

template <typename T>
struct has_flip<T, std::void_t<decltype(std::declval<T>().flip())>>
    : std::true_type {};

template <typename T>
struct is_bit_reference_like {
  static constexpr const bool value = std::is_convertible<T, bool>::value &&
      std::is_nothrow_assignable<T, bool>::value && has_flip<T>::value;
};

#ifdef _LIBCPP_VERSION

// Workaround for libc++ incompatibility with C++ standard.
// According to the Standard, `bitset::operator[] const` returns bool.
template <typename C>
struct is_bit_reference_like<std::__bit_const_reference<C>> {
  static constexpr const bool value = true;
};

#endif
} // namespace fmt::detail

// We can't use std::vector<bool, Allocator>::reference and
// std::bitset<N>::reference because the compiler can't deduce Allocator and N
// in partial specialization.
template <typename BitRef, typename Char>
struct fmt::formatter<
    BitRef,
    Char,
    std::enable_if_t<fmt::detail::is_bit_reference_like<BitRef>::value>>
    : formatter<bool, Char> {
  template <typename FormatContext>
  FMT_CONSTEXPR auto format(const BitRef& v, FormatContext& ctx) const
      -> decltype(ctx.out()) {
    return formatter<bool, Char>::format(v, ctx);
  }
};
#endif
