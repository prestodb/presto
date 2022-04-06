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

#include <memory>
#include <utility>

namespace facebook::velox::util {

template <typename T>
struct is_shared_ptr : std::false_type {};

template <typename T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type {};

template <template <typename> class Transformer, typename E>
struct tuple_xform {};

template <template <typename> class Transformer, typename... E>
struct tuple_xform<Transformer, std::tuple<E...>> {
  using type = std::tuple<typename Transformer<E>::type...>;
};

// the following is c++17 backfill
namespace detail {
template <size_t...>
struct seq {};

template <size_t N, size_t... S>
struct gens : gens<N - 1, N - 1, S...> {};

template <size_t... S>
struct gens<0, S...> {
  typedef seq<S...> type;
};

template <typename F, typename Tuple, size_t... S>
auto apply_impl(F&& func, Tuple&& params, seq<S...>) {
  return func(std::get<S>(std::forward<Tuple>(params))...);
}

} // namespace detail

template <typename F, typename Tuple>
auto apply(F&& func, Tuple&& params) {
  return detail::apply_impl(
      std::forward<F>(func),
      std::forward<Tuple>(params),
      typename detail::gens<std::tuple_size<
          typename std::remove_reference<Tuple>::type>::value>::type());
}

template <class... Ts>
struct overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

namespace detail {

template <
    size_t Idx,
    typename Tuple,
    typename Func,
    std::enable_if_t<Idx >= std::tuple_size<Tuple>::value, int32_t> = 0>
void forEachWithIndex(Func&& func, Tuple&& tup) {}

template <
    size_t Idx,
    typename Tuple,
    typename Func,
    std::enable_if_t<Idx<std::tuple_size<Tuple>::value, int32_t> = 0> void
        forEachWithIndex(Func&& func, Tuple&& tup) {
  func(Idx, std::get<Idx>(tup));
  forEachWithIndex<Idx + 1>(std::forward<Func>(func), std::forward<Tuple>(tup));
}

} // namespace detail

template <typename Func, typename Tuple>
void forEachWithIndex(Func&& func, Tuple&& tup) {
  detail::forEachWithIndex<0>(
      std::forward<Func>(func), std::forward<Tuple>(tup));
}

namespace detail {
template <typename...>
struct voider {
  using type = void;
};

template <typename... T>
using void_t = typename voider<T...>::type;

template <typename T, typename U = void>
struct is_mappish_impl : std::false_type {};

template <typename T>
struct is_mappish_impl<
    T,
    void_t<
        typename T::key_type,
        typename T::mapped_type,
        decltype(std::declval<
                 T&>()[std::declval<const typename T::key_type&>()])>>
    : std::true_type {};
} // namespace detail

template <typename T>
struct is_mappish : detail::is_mappish_impl<T>::type {};

template <typename T>
struct is_smart_pointer {
  static const bool value = false;
};

template <typename T>
struct is_smart_pointer<std::shared_ptr<T>> {
  static const bool value = true;
};

template <typename T>
struct is_smart_pointer<std::unique_ptr<T>> {
  static const bool value = true;
};

template <typename T>
struct is_smart_pointer<std::weak_ptr<T>> {
  static const bool value = true;
};

// Checks if a class C provides a method which returns TRet and takes TArgs as
// parameters. TResolver is a struct created using DECLARE_METHOD_RESOLVER,
// which contains the name of the method.
template <typename C, class TResolver, typename TRet, typename... TArgs>
struct has_method {
 private:
  template <typename T>
  static constexpr auto check(T*) -> typename std::is_same<
      decltype(std::declval<TResolver>().template resolve<T>(
          std::declval<TArgs>()...)),
      TRet>::type {
    return {};
  }

  template <typename>
  static constexpr std::false_type check(...) {
    return std::false_type();
  }

  using type = decltype(check<C>(nullptr));

 public:
  static constexpr bool value = type::value;
};

// Declares a method resolver to be used with has_method.
#define DECLARE_METHOD_RESOLVER(Name, MethodName)              \
  struct Name {                                                \
    template <class __T, typename... __TArgs>                  \
    constexpr auto resolve(__TArgs&&... args) const            \
        -> decltype(std::declval<__T>().MethodName(args...)) { \
      return {};                                               \
    }                                                          \
  };

// Calling Name::resolve<T>::type will return T::TypeName if T::TypeName
// exists, and otherwise will return T::OtherTypeName (it's existence is not
// checked)
#define DECLARE_CONDITIONAL_TYPE_NAME(Name, TypeName, OtherTypeName) \
  struct Name {                                                      \
    template <typename __T, typename = void>                         \
    struct resolve {                                                 \
      using type = typename __T::OtherTypeName;                      \
    };                                                               \
                                                                     \
    template <typename __T>                                          \
    struct resolve<                                                  \
        __T,                                                         \
        std::void_t<decltype(sizeof(typename __T::TypeName))>> {     \
      using type = typename __T::TypeName;                           \
    };                                                               \
  };
} // namespace facebook::velox::util
