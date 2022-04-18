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

// Most UDFs are deterministic, hence this default value.
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

// Information collected during TypeAnalysis.
struct TypeAnalysisResults {
  struct Stats {
    // Whether a variadic is encountered.
    bool hasVariadic = false;

    // Whether a generic is encountered.
    bool hasGeneric = false;

    // Whether a variadic of generic is encountered.
    // E.g: Variadic<T> or Variadic<Array<T1>>.
    bool hasVariadicOfGeneric = false;

    // The number of types that are neither generic, nor variadic.
    size_t concreteCount = 0;

    // Set a priority based on the collected information. Lower priorities are
    // picked first during function resolution. Each signature get a rank out
    // of 4, those ranks form a Lattice ordering.
    // rank 1: generic free and variadic free.
    //    e.g: int, int, int -> int.
    // rank 2: has variadic but generic free.
    //    e.g: Variadic<int> -> int.
    // rank 3: has generic but no variadic of generic.
    //    e.g: Any, Any, -> int.
    // rank 4: has variadic of generic.
    //    e.g: Variadic<Any> -> int.

    // If two functions have the same rank, then concreteCount is used to
    // to resolve the ordering.
    // e.g: consider the two functions:
    //    1. int, Any, Variadic<int> -> has rank 3. concreteCount =2
    //    2. int, Any, Any     -> has rank 3. concreteCount =1
    // in this case (1) is picked.
    // e.g: (Any, int) will be picked before (Any, Any)
    // e.g: Variadic<Array<Any>> is picked before Variadic<Any>.
    uint32_t getRank() {
      if (!hasGeneric && !hasVariadic) {
        return 1;
      }

      if (hasVariadic && !hasGeneric) {
        VELOX_DCHECK(!hasVariadicOfGeneric);
        return 2;
      }

      if (hasGeneric && !hasVariadicOfGeneric) {
        return 3;
      }

      if (hasVariadicOfGeneric) {
        VELOX_DCHECK(hasVariadic);
        VELOX_DCHECK(hasGeneric);
        return 4;
      }

      VELOX_UNREACHABLE("unreachable");
    }

    uint32_t computePriority() {
      // This assumes we wont have signature longer than 1M argument.
      return getRank() * 1000000 - concreteCount;
    }
  } stats;

  // String representaion of the type in the FunctionSignatureBuilder.
  std::ostringstream out;

  // Set of generic variables used in the type.
  std::set<std::string> variables;

  std::string typeAsString() {
    return out.str();
  }

  void resetTypeString() {
    out.str(std::string());
  }
};

// A set of structs used to perform analysis on a static type to
// collect information needed for signatrue construction.
template <typename T>
struct TypeAnalysis {
  void run(TypeAnalysisResults& results) {
    // This should only handle primitives and OPAQUE.
    static_assert(
        CppToType<T>::isPrimitiveType ||
        CppToType<T>::typeKind == TypeKind::OPAQUE);
    results.stats.concreteCount++;
    results.out << boost::algorithm::to_lower_copy(
        std::string(CppToType<T>::name));
  }
};

template <typename T>
struct TypeAnalysis<Generic<T>> {
  void run(TypeAnalysisResults& results) {
    if constexpr (std::is_same<T, AnyType>::value) {
      results.out << "any";
    } else {
      auto variableType = fmt::format("__user_T{}", T::getId());
      results.out << variableType;
      results.variables.insert(variableType);
    }
    results.stats.hasGeneric = true;
  }
};

template <typename K, typename V>
struct TypeAnalysis<Map<K, V>> {
  void run(TypeAnalysisResults& results) {
    results.stats.concreteCount++;
    results.out << "map(";
    TypeAnalysis<K>().run(results);
    results.out << ", ";
    TypeAnalysis<V>().run(results);
    results.out << ")";
  }
};

template <typename V>
struct TypeAnalysis<Variadic<V>> {
  void run(TypeAnalysisResults& results) {
    // We need to split, pass a clean results then merge results to correctly
    // compute `hasVariadicOfGeneric`.
    TypeAnalysisResults tmp;
    TypeAnalysis<V>().run(tmp);

    // Combine the child results.
    results.stats.hasVariadic = true;
    results.stats.hasGeneric = results.stats.hasGeneric || tmp.stats.hasGeneric;
    results.stats.hasVariadicOfGeneric =
        tmp.stats.hasGeneric || results.stats.hasVariadicOfGeneric;

    results.stats.concreteCount += tmp.stats.concreteCount;
    results.variables.insert(tmp.variables.begin(), tmp.variables.end());
    results.out << tmp.typeAsString();
  }
};

template <typename V>
struct TypeAnalysis<Array<V>> {
  void run(TypeAnalysisResults& results) {
    results.stats.concreteCount++;
    results.out << "array(";
    TypeAnalysis<V>().run(results);
    results.out << ")";
  }
};

template <typename... T>
struct TypeAnalysis<Row<T...>> {
  using child_types = std::tuple<T...>;

  template <size_t N>
  using child_type_at = typename std::tuple_element<N, child_types>::type;

  void run(TypeAnalysisResults& results) {
    results.stats.concreteCount++;
    results.out << "row(";
    // This expression applies the lambda for each row child type.
    bool first = true;
    (
        [&]() {
          if (!first) {
            results.out << ", ";
          }
          first = false;
          TypeAnalysis<T>().run(results);
        }(),
        ...);
    results.out << ")";
  }
};

// TODO: remove once old writers deprecated.
template <typename V>
struct TypeAnalysis<ArrayWriterT<V>> {
  void run(TypeAnalysisResults& results) {
    TypeAnalysis<Array<V>>().run(results);
  }
};

template <typename K, typename V>
struct TypeAnalysis<MapWriterT<K, V>> {
  void run(TypeAnalysisResults& results) {
    TypeAnalysis<Map<K, V>>().run(results);
  }
};

template <typename... T>
struct TypeAnalysis<RowWriterT<T...>> {
  void run(TypeAnalysisResults& results) {
    TypeAnalysis<Row<T...>>().run(results);
  }
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
  virtual uint32_t priority() const = 0;
  virtual const std::shared_ptr<exec::FunctionSignature> signature() const = 0;
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
    return CreateType<Underlying>::create();
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
  uint32_t priority() const override {
    return priority_;
  }

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
    auto analysis = analyzeSignatureTypes();
    buildSignature(analysis);
    priority_ = analysis.stats.computePriority();
    verifyReturnTypeCompatibility();
  }

  ~SimpleFunctionMetadata() override = default;

  const std::shared_ptr<exec::FunctionSignature> signature() const override {
    return signature_;
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
  struct SignatureTypesAnalysisResults {
    std::vector<std::string> argsTypes;
    std::string outputType;
    std::set<std::string> variables;
    TypeAnalysisResults::Stats stats;
  };

  SignatureTypesAnalysisResults analyzeSignatureTypes() {
    std::vector<std::string> argsTypes;

    TypeAnalysisResults results;
    TypeAnalysis<return_type>().run(results);
    std::string outputType = results.typeAsString();

    (
        [&]() {
          // Clear string representation but keep other collected information
          // to accumulate.
          results.resetTypeString();
          TypeAnalysis<Args>().run(results);
          argsTypes.push_back(results.typeAsString());
        }(),
        ...);

    return SignatureTypesAnalysisResults{
        std::move(argsTypes),
        std::move(outputType),
        std::move(results.variables),
        std::move(results.stats)};
  }

  void buildSignature(const SignatureTypesAnalysisResults& analysis) {
    auto builder = exec::FunctionSignatureBuilder();

    builder.returnType(analysis.outputType);

    for (const auto& arg : analysis.argsTypes) {
      builder.argumentType(arg);
    }

    for (const auto& variable : analysis.variables) {
      builder.typeVariable(variable);
    }

    if (isVariadic()) {
      builder.variableArity();
    }
    signature_ = builder.build();
  }

  void verifyReturnTypeCompatibility() {
    VELOX_USER_CHECK(
        CppToType<TReturn>::create()->kindEquals(returnType_),
        "return type override mismatch");
  }

  const std::shared_ptr<const Type> returnType_;
  std::shared_ptr<exec::FunctionSignature> signature_;
  uint32_t priority_;
};

// wraps a UDF object to provide the inheritance
// this is basically just boilerplate-avoidance
template <typename Fun, typename Exec, typename TReturn, typename... TArgs>
class UDFHolder final
    : public core::SimpleFunctionMetadata<Fun, TReturn, TArgs...> {
  Fun instance_;

 public:
  using Metadata = core::SimpleFunctionMetadata<Fun, TReturn, TArgs...>;

  template <typename T>
  using exec_resolver = typename Exec::template resolver<T>;

  using exec_return_type = typename exec_resolver<TReturn>::out_type;
  using optional_exec_return_type = std::optional<exec_return_type>;

  template <typename T>
  using exec_arg_type = typename exec_resolver<T>::in_type;
  using exec_arg_types = std::tuple<typename exec_resolver<TArgs>::in_type...>;
  template <typename T>
  using optional_exec_arg_type = std::optional<exec_arg_type<T>>;

  DECLARE_CONDITIONAL_TYPE_NAME(
      null_free_in_type_resolver,
      null_free_in_type,
      in_type);

  template <typename T>
  using exec_no_nulls_arg_type =
      typename null_free_in_type_resolver::template resolve<
          exec_resolver<T>>::type;

  DECLARE_METHOD_RESOLVER(call_method_resolver, call);
  DECLARE_METHOD_RESOLVER(callNullable_method_resolver, callNullable);
  DECLARE_METHOD_RESOLVER(callNullFree_method_resolver, callNullFree);
  DECLARE_METHOD_RESOLVER(callAscii_method_resolver, callAscii);
  DECLARE_METHOD_RESOLVER(initialize_method_resolver, initialize);

  // Check which flavor of the call() method is provided by the UDF object. UDFs
  // are required to provide at least one of the following methods:
  //
  // - bool|void call(...)
  // - bool|void callNullable(...)
  // - bool|void callNullFree(...)
  //
  // Each of these methods can return either bool or void. Returning void means
  // that the UDF is assumed never to return null values.
  //
  // Optionally, UDFs can also provide the following methods:
  //
  // - bool|void callAscii(...)
  // - void initialize(...)

  // call():
  static constexpr bool udf_has_call_return_bool = util::has_method<
      Fun,
      call_method_resolver,
      bool,
      exec_return_type,
      const exec_arg_type<TArgs>&...>::value;
  static constexpr bool udf_has_call_return_void = util::has_method<
      Fun,
      call_method_resolver,
      void,
      exec_return_type,
      const exec_arg_type<TArgs>&...>::value;
  static constexpr bool udf_has_call =
      udf_has_call_return_bool | udf_has_call_return_void;
  static_assert(
      !(udf_has_call_return_bool && udf_has_call_return_void),
      "Provided call() methods need to return either void OR bool.");

  // callNullable():
  static constexpr bool udf_has_callNullable_return_bool = util::has_method<
      Fun,
      callNullable_method_resolver,
      bool,
      exec_return_type,
      const exec_arg_type<TArgs>*...>::value;
  static constexpr bool udf_has_callNullable_return_void = util::has_method<
      Fun,
      callNullable_method_resolver,
      void,
      exec_return_type,
      const exec_arg_type<TArgs>*...>::value;
  static constexpr bool udf_has_callNullable =
      udf_has_callNullable_return_bool | udf_has_callNullable_return_void;
  static_assert(
      !(udf_has_callNullable_return_bool && udf_has_callNullable_return_void),
      "Provided callNullable() methods need to return either void OR bool.");

  // callNullFree():
  static constexpr bool udf_has_callNullFree_return_bool = util::has_method<
      Fun,
      callNullFree_method_resolver,
      bool,
      exec_return_type,
      const exec_no_nulls_arg_type<TArgs>&...>::value;
  static constexpr bool udf_has_callNullFree_return_void = util::has_method<
      Fun,
      callNullFree_method_resolver,
      void,
      exec_return_type,
      const exec_no_nulls_arg_type<TArgs>&...>::value;
  static constexpr bool udf_has_callNullFree =
      udf_has_callNullFree_return_bool | udf_has_callNullFree_return_void;
  static_assert(
      !(udf_has_callNullFree_return_bool && udf_has_callNullFree_return_void),
      "Provided callNullFree() methods need to return either void OR bool.");

  // callAscii():
  static constexpr bool udf_has_callAscii_return_bool = util::has_method<
      Fun,
      callAscii_method_resolver,
      bool,
      exec_return_type,
      const exec_arg_type<TArgs>&...>::value;
  static constexpr bool udf_has_callAscii_return_void = util::has_method<
      Fun,
      callAscii_method_resolver,
      void,
      exec_return_type,
      const exec_arg_type<TArgs>&...>::value;
  static constexpr bool udf_has_callAscii =
      udf_has_callAscii_return_bool | udf_has_callAscii_return_void;
  static_assert(
      !(udf_has_callAscii_return_bool && udf_has_callAscii_return_void),
      "Provided callAscii() methods need to return either void OR bool.");

  // Assert that the return type for callAscii() matches call()'s.
  static_assert(
      !((udf_has_callAscii_return_bool && udf_has_call_return_void) ||
        (udf_has_callAscii_return_void && udf_has_call_return_bool)),
      "The return type for callAscii() must match the return type for call().");

  // initialize():
  static constexpr bool udf_has_initialize = util::has_method<
      Fun,
      initialize_method_resolver,
      void,
      const core::QueryConfig&,
      const exec_arg_type<TArgs>*...>::value;

  static_assert(
      udf_has_call || udf_has_callNullable || udf_has_callNullFree,
      "UDF must implement at least one of `call`, `callNullable`, or `callNullFree`");

  static_assert(
      ValidateVariadicArgs<TArgs...>::value,
      "Variadic can only be used as the last argument to a UDF");

  // Initialize could be supported with variadicArgs
  static_assert(
      !(udf_has_initialize && Metadata::isVariadic()),
      "Initialize is not supported for UDFs with VariadicArgs.");

  // Default null behavior means assuming null output if any of the inputs is
  // null, without calling the function implementation.
  static constexpr bool is_default_null_behavior = !udf_has_callNullable;

  // If any of the the provided "call" flavors can produce null (in case any of
  // them return bool). This is only false if all the call methods provided for
  // a function return void.
  static constexpr bool can_produce_null_output = udf_has_call_return_bool |
      udf_has_callNullable_return_bool | udf_has_callNullFree_return_bool |
      udf_has_callAscii_return_bool;

  // This is true when callNullFree is implemented, but not call or
  // callNullable. In this case if any input is NULL or any complex type in
  // the input contains a NULL element (recursively) the function will return
  // NULL directly, skipping evaluation.
  static constexpr bool is_default_contains_nulls_behavior =
      !udf_has_call && !udf_has_callNullable;
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
      const typename exec_resolver<TArgs>::in_type*... constantArgs) {
    if constexpr (udf_has_initialize) {
      return instance_.initialize(config, constantArgs...);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      exec_return_type& out,
      const typename exec_resolver<TArgs>::in_type&... args) {
    if constexpr (udf_has_call) {
      return callImpl(out, args...);
    } else if constexpr (udf_has_callNullable) {
      return callNullableImpl(out, (&args)...);
    } else {
      VELOX_UNREACHABLE(
          "call should never be called if the UDF does not "
          "implement call or callNullable.");
    }
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      exec_return_type& out,
      const typename exec_resolver<TArgs>::in_type*... args) {
    if constexpr (udf_has_callNullable) {
      return callNullableImpl(out, args...);
    } else if constexpr (udf_has_call) {
      // Default null behavior.
      const bool isAllSet = (args && ...);
      if (LIKELY(isAllSet)) {
        return callImpl(out, (*args)...);
      } else {
        return false;
      }
    } else {
      VELOX_UNREACHABLE(
          "callNullable should never be called if the UDF does not "
          "implement callNullable or call.");
    }
  }

  FOLLY_ALWAYS_INLINE bool callAscii(
      exec_return_type& out,
      const typename exec_resolver<TArgs>::in_type&... args) {
    if constexpr (udf_has_callAscii) {
      return callAsciiImpl(out, args...);
    } else {
      return call(out, args...);
    }
  }

  FOLLY_ALWAYS_INLINE bool callNullFree(
      exec_return_type& out,
      const exec_no_nulls_arg_type<TArgs>&... args) {
    if constexpr (udf_has_callNullFree) {
      return callNullFreeImpl(out, args...);
    } else {
      VELOX_UNREACHABLE(
          "callNullFree should never be called if the UDF does not implement callNullFree.");
    }
  }

  // Helper functions to handle void vs bool return type.

  FOLLY_ALWAYS_INLINE bool callImpl(
      typename Exec::template resolver<TReturn>::out_type& out,
      const typename Exec::template resolver<TArgs>::in_type&... args) {
    static_assert(udf_has_call);
    if constexpr (udf_has_call_return_bool) {
      return instance_.call(out, args...);
    } else {
      instance_.call(out, args...);
      return true;
    }
  }

  FOLLY_ALWAYS_INLINE bool callNullableImpl(
      exec_return_type& out,
      const typename Exec::template resolver<TArgs>::in_type*... args) {
    static_assert(udf_has_callNullable);
    if constexpr (udf_has_callNullable_return_bool) {
      return instance_.callNullable(out, args...);
    } else {
      instance_.callNullable(out, args...);
      return true;
    }
  }

  FOLLY_ALWAYS_INLINE bool callAsciiImpl(
      typename Exec::template resolver<TReturn>::out_type& out,
      const typename Exec::template resolver<TArgs>::in_type&... args) {
    static_assert(udf_has_callAscii);
    if constexpr (udf_has_callAscii_return_bool) {
      return instance_.callAscii(out, args...);
    } else {
      instance_.callAscii(out, args...);
      return true;
    }
  }

  FOLLY_ALWAYS_INLINE bool callNullFreeImpl(
      typename Exec::template resolver<TReturn>::out_type& out,
      const exec_no_nulls_arg_type<TArgs>&... args) {
    static_assert(udf_has_callNullFree);
    if constexpr (udf_has_callNullFree_return_bool) {
      return instance_.callNullFree(out, args...);
    } else {
      instance_.callNullFree(out, args...);
      return true;
    }
  }
};

} // namespace facebook::velox::core
