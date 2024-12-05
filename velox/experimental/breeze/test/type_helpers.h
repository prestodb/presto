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

/*
 * Copyright (c) 2024 by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */

// Here we define a simple compile-time string that can be reified into a
// null-terminated c-string, as well as some helpers for the manipulation
// thereof.
template <char... cs>
struct TypeString {
  static constexpr char value[] = {cs..., 0};
};

template <const char* s,
          typename Seq = std::make_index_sequence<std::string_view{s}.size()>>
struct MakeTypeString;
template <const char* s, size_t... Seq>
struct MakeTypeString<s, std::index_sequence<Seq...>> {
  using type = TypeString<s[Seq]...>;
};

template <typename T1, typename T2>
struct StrPlus;
template <char... s1, char... s2>
struct StrPlus<TypeString<s1...>, TypeString<s2...>> {
  using type = TypeString<s1..., s2...>;
};

template <typename Sep, typename... Ts>
struct StrJoin;
template <typename Sep, typename T>
struct StrJoin<Sep, T> {
  using type = T;
};
template <typename Sep, typename First, typename... Rest>
struct StrJoin<Sep, First, Rest...> {
  using type = typename StrPlus<
      First,
      typename StrPlus<Sep, typename StrJoin<Sep, Rest...>::type>::type>::type;
};

// Type to string. Here we also define the TypeString representation of some
// commonly used types.
template <typename T>
struct TypeToStr;
template <>
struct TypeToStr<int> {
  static constexpr const char value[] = "int";
  using type = typename MakeTypeString<value>::type;
};
template <>
struct TypeToStr<unsigned> {
  static constexpr const char value[] = "uint";
  using type = typename MakeTypeString<value>::type;
};
template <>
struct TypeToStr<float> {
  static constexpr const char value[] = "float";
  using type = typename MakeTypeString<value>::type;
};
template <>
struct TypeToStr<char> {
  static constexpr const char value[] = "char";
  using type = typename MakeTypeString<value>::type;
};
template <>
struct TypeToStr<unsigned char> {
  static constexpr const char value[] = "uchar";
  using type = typename MakeTypeString<value>::type;
};

// Unsigned integer to TypeString
template <size_t N, bool show_zero = true>
struct IntToStr {
  using type = typename StrPlus<typename IntToStr<N / 10, false>::type,
                                TypeString<'0' + (N % 10)>>::type;
};
template <>
struct IntToStr<0, true> {
  using type = TypeString<'0'>;
};
template <>
struct IntToStr<0, false> {
  using type = TypeString<>;
};

// The below declarations mirror types defined for usage in device-side code.
// Since they are ineligible for inclusion in host-side code such as the tests
// (in split-source frameworks at least), they are instead forward declared
// here for association with their string representations through `TypeToStr`.
// The string representations are used for computing the names of the kernel
// functions.
namespace breeze {
namespace functions {
struct ReduceOpAdd;
struct ReduceOpMin;
struct ReduceOpMax;
struct ScanOpAdd;
}  // namespace functions
namespace algorithms {
using ReduceOpAdd = functions::ReduceOpAdd;
using ReduceOpMin = functions::ReduceOpMin;
using ReduceOpMax = functions::ReduceOpMax;
using ScanOpAdd = functions::ScanOpAdd;
}  // namespace algorithms
}  // namespace breeze

template <>
struct TypeToStr<breeze::functions::ReduceOpAdd> {
  constexpr static const char value[] = "add";
  using type = typename MakeTypeString<value>::type;
};
template <>
struct TypeToStr<breeze::functions::ReduceOpMin> {
  constexpr static const char value[] = "min";
  using type = typename MakeTypeString<value>::type;
};
template <>
struct TypeToStr<breeze::functions::ReduceOpMax> {
  constexpr static const char value[] = "max";
  using type = typename MakeTypeString<value>::type;
};
template <>
struct TypeToStr<breeze::functions::ScanOpAdd> {
  constexpr static const char value[] = "add";
  using type = typename MakeTypeString<value>::type;
};

namespace breeze {
namespace utils {
class NullType {};
}  // namespace utils
}  // namespace breeze

template <>
struct TypeToStr<breeze::utils::NullType> {
  constexpr static const char value[] = "null";
  using type = typename MakeTypeString<value>::type;
};

// Helpers for constructing the compute kernel's name. Kernels are named
// according to the format: {op_name}_{types}_{N}x...x{M}
// where the numeric instance shape (BLOCK_THREADS, ITEMS_PER_THREAD, etc.)
// are joined with 'x'.
//
// Note that any `const char*` template parameters must be given as a constexpr
// character array rather than a string literal. This roundabout approach is
// required due to limitations of C++17 wherein template pointers cannot refer
// to or be subobjects of string literals, and so require the strings be put
// into a static character array before usage. In C++20 support was added for
// certain class types as non-type template parameters, which can be used to get
// around this restriction.

template <const char* op_base, typename... Variants>
constexpr const char* add_op_variant =
    StrJoin<TypeString<'_'>, typename MakeTypeString<op_base>::type,
            typename TypeToStr<Variants>::type...>::type::value;

template <size_t... vs>
using instance_shape_t =
    typename StrJoin<TypeString<'x'>, typename IntToStr<vs>::type...>::type;

template <const char* op_name, size_t... shape>
constexpr const char* add_instance_shape =
    StrJoin<TypeString<'_'>, typename MakeTypeString<op_name>::type,
            instance_shape_t<shape...>>::type::value;
