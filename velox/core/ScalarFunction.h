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

#include "folly/Likely.h"
#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/core/Metaprogramming.h"
#include "velox/core/QueryConfig.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::velox::core {

class ArgumentsCtx {
 public:
  /* implicit */ ArgumentsCtx(std::vector<std::shared_ptr<const Type>> argTypes)
      : types_(std::move(argTypes)) {}

  const std::vector<std::shared_ptr<const Type>>& types() const {
    return types_;
  }

  bool operator==(const ArgumentsCtx& rhs) const {
    if (types_.size() != rhs.types_.size()) {
      return false;
    }
    return std::equal(
        std::begin(types_),
        std::end(types_),
        std::begin(rhs.types()),
        [](const std::shared_ptr<const Type>& l,
           const std::shared_ptr<const Type>& r) { return l->kindEquals(r); });
  }

  bool operator!=(const ArgumentsCtx& rhs) const {
    return !(*this == rhs);
  }

 private:
  std::vector<std::shared_ptr<const Type>> types_;
};

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

// This class is used as a key to resolve UDFs. UDF registry stores the function
// implementation with the function signatures (FunctionKey) that it supports.
// When it compiles the input expressions, expression engines evaluate the input
// and construct FunctionKey with type resolving all inputs, to find a UDF
// implementation that can handle this.
class FunctionKey {
 public:
  FunctionKey(std::string name, std::vector<std::shared_ptr<const Type>> params)
      : name_{std::move(name)}, argumentsCtx_{std::move(params)} {
    for (const auto& param : argumentsCtx_.types()) {
      CHECK_NOTNULL(param.get());
    }
  }

  std::string toString() const {
    std::string buf{name_};
    buf.append("( ");
    for (const auto& type : argumentsCtx_.types()) {
      buf.append(type->toString());
      buf.append(" ");
    }
    buf.append(")");
    return buf;
  }

  friend std::ostream& operator<<(std::ostream& stream, const FunctionKey& k) {
    stream << k.toString();
    return stream;
  }

  bool operator==(const FunctionKey& rhs) const {
    auto& lhs = *this;
    if (lhs.name_ != rhs.name_) {
      return false;
    }
    return lhs.argumentsCtx_ == rhs.argumentsCtx_;
  }

  const std::string& name() const {
    return name_;
  }

  const std::vector<std::shared_ptr<const Type>>& types() const {
    return argumentsCtx_.types();
  }

 private:
  std::string name_;
  ArgumentsCtx argumentsCtx_;
};

// todo(youknowjack): add a dynamic execution mode
// todo(youknowjack): need a better story for types for UDFs. Mapping
//                    c++ types <-> Velox types is imprecise (e.g. string vs
//                    binary) and difficult to change.
// TODO: separate metadata and execution parts of this class so that metadata
// could be accessed without instantiating UDF object.
// Right now `callDynamic` is the only execution-related bit and we just
// override it as NYI in ScalarFunctionMetadata which is not very clean but
// works.
class IScalarFunction {
 public:
  virtual std::shared_ptr<const Type> returnType() const = 0;
  virtual std::vector<std::shared_ptr<const Type>> argTypes() const = 0;
  virtual std::string getName() const = 0;
  virtual bool isDeterministic() const = 0;
  virtual int32_t reuseStringsFromArg() const = 0;
  virtual variant callDynamic(const std::vector<variant>& inputs) = 0;

  FunctionKey key() const;
  std::string signature() const;

  virtual ~IScalarFunction() = default;
};

template <typename Fun, typename TReturn, typename... Args>
class ScalarFunctionMetadata : public IScalarFunction {
 public:
  using return_type = TReturn;
  using arg_types = std::tuple<Args...>;
  template <size_t N>
  using type_at = typename std::tuple_element<N, arg_types>::type;
  static constexpr int num_args = std::tuple_size<arg_types>::value;

 public:
  std::string getName() const final {
    return Fun::name;
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

  std::vector<std::shared_ptr<const Type>> argTypes() const final {
    std::vector<std::shared_ptr<const Type>> args(num_args);
    auto it = args.begin();
    ((*it++ = CppToType<Args>::create()), ...);
    for (const auto& arg : args) {
      CHECK_NOTNULL(arg.get());
    }
    return args;
  }

  explicit ScalarFunctionMetadata(std::shared_ptr<const Type> returnType)
      : returnType_(
            returnType ? std::move(returnType) : CppToType<TReturn>::create()) {
    verifyReturnTypeCompatibility();
  }
  ~ScalarFunctionMetadata() override = default;

  variant callDynamic(const std::vector<variant>& /* inputs */) override {
    VELOX_NYI("ScalarFunctionMetadata shouldn't be used for evaluation");
  }

 private:
  void verifyReturnTypeCompatibility() {
    VELOX_USER_CHECK(
        CppToType<TReturn>::create()->kindEquals(returnType_),
        "return type override mismatch");
  }

  const std::shared_ptr<const Type> returnType_;
};

// wraps a UDF object to provide the inheritance
// this is basically just boilerplate-avoidance
template <typename Fun, typename Exec, typename TReturn, typename... TArgs>
class UDFHolder final
    : public core::ScalarFunctionMetadata<Fun, TReturn, TArgs...> {
  Fun instance_;

 public:
  using Metadata = core::ScalarFunctionMetadata<Fun, TReturn, TArgs...>;

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

  variant callDynamic(const std::vector<variant>& inputs) final {
    exec_return_type result;
    bool isSet = evalDynamic<0>(result, inputs);
    if (isSet) {
      return Exec::template resolver<TReturn>::toVariant(result);
    } else {
      return variant{CppToType<TReturn>::typeKind};
    }
  }

 private:
  template <int32_t offset>
  bool evalDynamic(
      exec_return_type& result,
      const std::vector<variant>& /* toUnpack */,
      const typename Exec::template resolver<TArgs>::in_type*... unpacked) {
    // TODO: This could be possibly be optimized if we knew if the function was
    // is_default_null_behavior.  Otherwise we might be optifying only to turn
    // around and unoptify.
    return callNullable(result, unpacked...);
  }

  template <
      int32_t offset,
      typename... Unpacked,
      typename std::enable_if_t<offset != Metadata::num_args, int32_t> = 0>
  bool evalDynamic(
      exec_return_type& result,
      const std::vector<variant>& toUnpack,
      const Unpacked*... unpacked) {
    auto& d = toUnpack.at(offset);
    if (d.isNull()) {
      const exec_type_at<offset>* nullPtr = nullptr;
      return evalDynamic<offset + 1>(result, toUnpack, unpacked..., nullPtr);
    } else {
      auto converted = Exec::template resolver<
          typename Metadata::template type_at<offset>>::fromVariant(d);
      return evalDynamic<offset + 1>(result, toUnpack, unpacked..., &converted);
    }
  }
};

} // namespace facebook::velox::core

namespace std {
template <>
struct hash<facebook::velox::core::ArgumentsCtx> {
  using argument_type = facebook::velox::core::ArgumentsCtx;
  using result_type = std::size_t;

  result_type operator()(const argument_type& key) const noexcept {
    size_t val = 0;
    for (const auto& type : key.types()) {
      val = val * 31 + type->hashKind();
    }
    return val;
  }
};

template <>
struct hash<facebook::velox::core::FunctionKey> {
  using argument_type = facebook::velox::core::FunctionKey;
  using result_type = std::size_t;
  result_type operator()(const argument_type& key) const noexcept {
    size_t val = std::hash<std::string>{}(key.name());
    for (const auto& type : key.types()) {
      val = val * 31 + type->hashKind();
    }
    return val;
  }
};

template <>
struct equal_to<facebook::velox::core::FunctionKey> {
  using result_type = bool;
  using first_argument_type = facebook::velox::core::FunctionKey;
  using second_argument_type = first_argument_type;

  bool operator()(
      const first_argument_type& lhs,
      const second_argument_type& rhs) const {
    return lhs == rhs;
  }
};
} // namespace std

template <>
struct fmt::formatter<facebook::velox::core::FunctionKey> {
  constexpr auto parse(format_parse_context& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const facebook::velox::core::FunctionKey& k, FormatContext& ctx) {
    return format_to(ctx.out(), "{}", k.toString());
  }
};
