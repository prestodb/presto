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

#include <boost/algorithm/string.hpp>
#include "folly/Likely.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/core/Metaprogramming.h"
#include "velox/core/QueryConfig.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::velox::core {

// most UDFs are determinisic, hence this default value
template <class T, class = void>
struct udf_is_deterministic : std::true_type {};

template <class T>
struct udf_is_deterministic<
    T,
    util::detail::void_t<decltype(T::is_deterministic)>>
    : std::integral_constant<bool, T::is_deterministic> {};

// Most functions are producing ASCII results for ASCII inputs, but we assume
// they are not unless specified explicitly.
template <class T, class = void>
struct udf_is_default_ascii_behavior : std::false_type {};

template <class T>
struct udf_is_default_ascii_behavior<
    T,
    util::detail::void_t<decltype(T::is_default_ascii_behavior)>>
    : std::integral_constant<bool, T::is_default_ascii_behavior> {};

template <class T, class = void>
struct udf_reuse_strings_from_arg : std::integral_constant<int32_t, -1> {};

template <class T>
struct udf_reuse_strings_from_arg<
    T,
    util::detail::void_t<decltype(T::reuse_strings_from_arg)>>
    : std::integral_constant<int32_t, T::reuse_strings_from_arg> {};

// If a UDF doesn't declare a default help(),
template <class T, class = void>
struct udf_help {
  std::string operator()() {
    return "Not available";
  }
};

template <class T>
struct udf_help<T, util::detail::void_t<decltype(T::help)>> {
  std::string operator()() {
    return T::help();
  }
};

// Has the value true, unless a Variadic Type appears anywhere but at the end
// of the parameters.
template <typename... TArgs>
struct ValidateVariadicArgs {
  using arg_types = std::tuple<TArgs...>;
  static constexpr int num_args = std::tuple_size<arg_types>::value;
  template <int32_t POSITION>
  using arg_at = typename std::tuple_element<POSITION, arg_types>::type;

  template <int32_t POSITION, typename... Args>
  static constexpr bool isValidArg() {
    if constexpr (POSITION >= num_args - 1) {
      return true;
    } else {
      return !isVariadicType<arg_at<POSITION>>::value &&
          isValidArg<POSITION + 1, Args...>();
    }
  }

 public:
  static constexpr bool value = isValidArg<0, TArgs...>();
};

// todo(youknowjack): need a better story for types for UDFs. Mapping
//                    c++ types <-> Velox types is imprecise (e.g. string vs
//                    binary) and difficult to change.
class ISimpleFunctionMetadata {
 public:
  virtual std::shared_ptr<const Type> returnType() const = 0;
  virtual std::vector<std::shared_ptr<const Type>> argTypes() const = 0;
  virtual std::string getName() const = 0;
  virtual bool isDeterministic() const = 0;
  virtual int32_t reuseStringsFromArg() const = 0;
  virtual std::shared_ptr<exec::FunctionSignature> signature() const = 0;
  virtual std::string helpMessage(const std::string& name) const = 0;

  virtual ~ISimpleFunctionMetadata() = default;
};

template <typename T, typename = int32_t>
struct udf_has_name : std::false_type {};

template <typename T>
struct udf_has_name<T, decltype(&T::name, 0)> : std::true_type {};

template <typename Arg>
struct CreateType {
  static std::shared_ptr<const Type> create() {
    return CppToType<Arg>::create();
  }
};

template <typename Underlying>
struct CreateType<Variadic<Underlying>> {
  static std::shared_ptr<const Type> create() {
    return CppToType<Underlying>::create();
  }
};

template <typename Fun, typename TReturn, typename... Args>
class SimpleFunctionMetadata : public ISimpleFunctionMetadata {
 public:
  using return_type = TReturn;
  using arg_types = std::tuple<Args...>;
  template <size_t N>
  using type_at = typename std::tuple_element<N, arg_types>::type;
  static constexpr int num_args = std::tuple_size<arg_types>::value;

 public:
  std::string getName() const final {
    if constexpr (udf_has_name<Fun>::value) {
      return Fun::name;
    } else {
      throw std::runtime_error(
          "Unable to find simple function name. Either define a 'name' "
          "member in the function class, or specify a function alias at "
          "registration time.");
    }
  }

  bool isDeterministic() const final {
    return udf_is_deterministic<Fun>();
  }

  int32_t reuseStringsFromArg() const final {
    return udf_reuse_strings_from_arg<Fun>();
  }

  std::shared_ptr<const Type> returnType() const final {
    return returnType_;
  }

  // Will convert Args to std::shared_ptr<const Type>.
  // Note that if the last arg is Variadic, this will return a
  // std::shared_ptr<const Type> matching the underlying type for that
  // argument.
  // You can check if that argument is Variadic by calling isVariadic()
  // on this object.
  std::vector<std::shared_ptr<const Type>> argTypes() const final {
    std::vector<std::shared_ptr<const Type>> args(num_args);
    auto it = args.begin();
    ((*it++ = CreateType<Args>::create()), ...);
    for (const auto& arg : args) {
      CHECK_NOTNULL(arg.get());
    }
    return args;
  }

  static constexpr bool isVariadic() {
    if constexpr (num_args == 0) {
      return false;
    } else {
      return isVariadicType<type_at<num_args - 1>>::value;
    }
  }

  explicit SimpleFunctionMetadata(std::shared_ptr<const Type> returnType)
      : returnType_(
            returnType ? std::move(returnType) : CppToType<TReturn>::create()) {
    verifyReturnTypeCompatibility();
  }
  ~SimpleFunctionMetadata() override = default;

  std::shared_ptr<exec::FunctionSignature> signature() const final {
    auto builder =
        exec::FunctionSignatureBuilder().returnType(typeToString(returnType()));

    for (const auto& arg : argTypes()) {
      builder.argumentType(typeToString(arg));
    }

    if (isVariadic()) {
      builder.variableArity();
    }

    return builder.build();
  }

  std::string helpMessage(const std::string& name) const final {
    std::string s{name};
    s.append("(");
    bool first = true;
    for (auto& arg : argTypes()) {
      if (!first) {
        s.append(", ");
      }
      first = false;
      s.append(arg->toString());
    }

    if (isVariadic()) {
      s.append("...");
    }

    s.append(")");
    return s;
  }

 private:
  void verifyReturnTypeCompatibility() {
    VELOX_USER_CHECK(
        CppToType<TReturn>::create()->kindEquals(returnType_),
        "return type override mismatch");
  }

  // convert type to a string representation that is recognized in
  // FunctionSignature.
  static std::string typeToString(const TypePtr& type) {
    std::ostringstream out;
    out << boost::algorithm::to_lower_copy(std::string(type->kindName()));
    if (type->size()) {
      out << "(";
      for (auto i = 0; i < type->size(); i++) {
        if (i > 0) {
          out << ",";
        }
        out << typeToString(type->childAt(i));
      }
      out << ")";
    }
    return out.str();
  }

  const std::shared_ptr<const Type> returnType_;
};

// wraps a UDF object to provide the inheritance
// this is basically just boilerplate-avoidance
template <typename Fun, typename Exec, typename TReturn, typename... TArgs>
class UDFHolder final
    : public core::SimpleFunctionMetadata<Fun, TReturn, TArgs...> {
  Fun instance_;

 public:
  using Metadata = core::SimpleFunctionMetadata<Fun, TReturn, TArgs...>;

  using exec_return_type = typename Exec::template resolver<TReturn>::out_type;
  using optional_exec_return_type = std::optional<exec_return_type>;

  template <typename T>
  using exec_arg_type = typename Exec::template resolver<T>::in_type;
  using exec_arg_types =
      std::tuple<typename Exec::template resolver<TArgs>::in_type...>;
  template <typename T>
  using optional_exec_arg_type = std::optional<exec_arg_type<T>>;

  DECLARE_METHOD_RESOLVER(call_method_resolver, call);
  DECLARE_METHOD_RESOLVER(callNullable_method_resolver, callNullable);
  DECLARE_METHOD_RESOLVER(callAscii_method_resolver, callAscii);
  DECLARE_METHOD_RESOLVER(initialize_method_resolver, initialize);

  // Check which of the call(), callNullable(), callAscii(), and initialize()
  // methods are available in the UDF object.
  static constexpr bool udf_has_call = util::has_method<
      Fun,
      call_method_resolver,
      bool,
      exec_return_type,
      const exec_arg_type<TArgs>&...>::value;

  static constexpr bool udf_has_callNullable = util::has_method<
      Fun,
      callNullable_method_resolver,
      bool,
      exec_return_type,
      const exec_arg_type<TArgs>*...>::value;

  static constexpr bool udf_has_callAscii = util::has_method<
      Fun,
      callAscii_method_resolver,
      bool,
      exec_return_type,
      const exec_arg_type<TArgs>&...>::value;

  static constexpr bool udf_has_initialize = util::has_method<
      Fun,
      initialize_method_resolver,
      void,
      const core::QueryConfig&,
      const exec_arg_type<TArgs>*...>::value;

  static_assert(
      udf_has_call || udf_has_callNullable,
      "UDF must implement at least one of `call` or `callNullable`");

  static_assert(
      ValidateVariadicArgs<TArgs...>::value,
      "Variadic can only be used as the last argument to a UDF");

  // Initialize could be supported with variadicArgs
  static_assert(
      !(udf_has_initialize && Metadata::isVariadic()),
      "Initialize is not supported for UDFs with VariadicArgs.");

  static constexpr bool is_default_null_behavior = !udf_has_callNullable;
  static constexpr bool has_ascii = udf_has_callAscii;
  static constexpr bool is_default_ascii_behavior =
      udf_is_default_ascii_behavior<Fun>();

  template <typename T>
  struct ptrfy {
    using type = const T*;
  };
  template <typename T>
  struct optify {
    using type = std::optional<T>;
  };

  using nullable_exec_arg_types =
      typename util::tuple_xform<ptrfy, exec_arg_types>::type;
  using optional_exec_arg_types =
      typename util::tuple_xform<optify, exec_arg_types>::type;

  template <size_t N>
  using exec_type_at = typename std::tuple_element<N, exec_arg_types>::type;

  explicit UDFHolder(std::shared_ptr<const Type> returnType)
      : Metadata(std::move(returnType)), instance_{} {}

  FOLLY_ALWAYS_INLINE void initialize(
      const core::QueryConfig& config,
      const typename Exec::template resolver<TArgs>::in_type*... constantArgs) {
    if constexpr (udf_has_initialize) {
      return instance_.initialize(config, constantArgs...);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      typename Exec::template resolver<TReturn>::out_type& out,
      const typename Exec::template resolver<TArgs>::in_type&... args) {
    if constexpr (udf_has_call) {
      return instance_.call(out, args...);
    } else {
      return instance_.callNullable(out, (&args)...);
    }
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      exec_return_type& out,
      const typename Exec::template resolver<TArgs>::in_type*... args) {
    if constexpr (udf_has_callNullable) {
      return instance_.callNullable(out, args...);
    } else {
      // default null behavior
      const bool isAllSet = (args && ...);
      if (LIKELY(isAllSet)) {
        return instance_.call(out, (*args)...);
      } else {
        return false;
      }
    }
  }

  FOLLY_ALWAYS_INLINE bool callAscii(
      typename Exec::template resolver<TReturn>::out_type& out,
      const typename Exec::template resolver<TArgs>::in_type&... args) {
    if constexpr (udf_has_callAscii) {
      return instance_.callAscii(out, args...);
    } else if constexpr (udf_has_call) {
      return instance_.call(out, args...);
    } else {
      return instance_.callNullable(out, (&args)...);
    }
  }
};

} // namespace facebook::velox::core
