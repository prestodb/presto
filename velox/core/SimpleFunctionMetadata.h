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
#include <folly/Likely.h>
#include <optional>

#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/core/Metaprogramming.h"
#include "velox/core/QueryConfig.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/type/SimpleFunctionApi.h"
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

// Canonical name of the function.
template <class T, class = void>
struct udf_canonical_name {
  static constexpr exec::FunctionCanonicalName value =
      exec::FunctionCanonicalName::kUnknown;
};

template <class T>
struct udf_canonical_name<
    T,
    util::detail::void_t<decltype(T::canonical_name)>> {
  static constexpr exec::FunctionCanonicalName value = T::canonical_name;
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

  void addVariable(exec::SignatureVariable&& variable) {
    if (!variablesInformation.count(variable.name())) {
      variablesInformation.emplace(variable.name(), variable);
    } else {
      VELOX_CHECK(
          variable == variablesInformation.at(variable.name()),
          "Cant assign different properties to the same variable {}",
          variable.name());
    }
  }

  /// String representation of the type in the FunctionSignatureBuilder.
  std::ostringstream out;

  /// Physical type, e.g. BIGINT() for Date and ARRAY(BIGINT()) for
  // Array<Date>. UNKNOWN() if type is generic or opaque.
  TypePtr physicalType;

  /// Set of generic variables used in the type.
  std::map<std::string, exec::SignatureVariable> variablesInformation;

  std::string typeAsString() {
    return out.str();
  }

  void resetTypeString() {
    out.str(std::string());
  }
};

namespace detail {
// This function is a dummy wrapper around boost::algorithm::to_lower_copy.
// Implementation is put into cpp file to avoid pretty expensive include
// boost/algorithm/string/case_conv.hpp that otherwise would be leaked into
// all dependent cpp files.
// TODO: This feels a bit ugly, another approach to explore is to just
// reimplement boost::algorithm::to_lower_copy with a for loop and std::tolower
// in the header to keep it all in header. But this requires much more testing:
// unit test to compare old and new way (do we need cover non-ASCII?),
// possibly even benchmarks to make sure perf is the same.
std::string strToLowerCopy(const std::string& str);
} // namespace detail

// A set of structs used to perform analysis on a static type to
// collect information needed for signature construction.
template <typename T>
struct TypeAnalysis {
  void run(TypeAnalysisResults& results) {
    // This should only handle primitives and OPAQUE.
    static_assert(
        SimpleTypeTrait<T>::isPrimitiveType ||
        SimpleTypeTrait<T>::typeKind == TypeKind::OPAQUE);
    results.stats.concreteCount++;
    results.out << detail::strToLowerCopy(
        std::string(SimpleTypeTrait<T>::name));
    if constexpr (
        SimpleTypeTrait<T>::typeKind == TypeKind::OPAQUE ||
        SimpleTypeTrait<T>::typeKind == TypeKind::UNKNOWN) {
      results.physicalType = UNKNOWN();
    } else {
      results.physicalType = createScalarType(SimpleTypeTrait<T>::typeKind);
    }
  }
};

template <typename T, bool comparable, bool orderable>
struct TypeAnalysis<Generic<T, comparable, orderable>> {
  void run(TypeAnalysisResults& results) {
    if constexpr (std::is_same_v<T, AnyType>) {
      results.out << "any";
    } else {
      auto typeVariableName = fmt::format("__user_T{}", T::getId());
      results.out << typeVariableName;
      results.addVariable(exec::SignatureVariable(
          typeVariableName,
          std::nullopt,
          exec::ParameterType::kTypeParameter,
          false,
          orderable,
          comparable));
    }
    results.stats.hasGeneric = true;
    results.physicalType = UNKNOWN();
  }
};

template <typename P, typename S>
struct TypeAnalysis<ShortDecimal<P, S>> {
  void run(TypeAnalysisResults& results) {
    results.stats.concreteCount++;

    const auto p = P::name();
    const auto s = S::name();
    results.out << fmt::format("decimal({},{})", p, s);
    results.addVariable(exec::SignatureVariable(
        p, std::nullopt, exec::ParameterType::kIntegerParameter));
    results.addVariable(exec::SignatureVariable(
        s, std::nullopt, exec::ParameterType::kIntegerParameter));
    results.physicalType = BIGINT();
  }
};

template <typename P, typename S>
struct TypeAnalysis<LongDecimal<P, S>> {
  void run(TypeAnalysisResults& results) {
    results.stats.concreteCount++;

    const auto p = P::name();
    const auto s = S::name();
    results.out << fmt::format("decimal({},{})", p, s);
    results.addVariable(exec::SignatureVariable(
        p, std::nullopt, exec::ParameterType::kIntegerParameter));
    results.addVariable(exec::SignatureVariable(
        s, std::nullopt, exec::ParameterType::kIntegerParameter));
    results.physicalType = HUGEINT();
  }
};

template <typename K, typename V>
struct TypeAnalysis<Map<K, V>> {
  void run(TypeAnalysisResults& results) {
    results.stats.concreteCount++;
    results.out << "map(";
    TypeAnalysis<K>().run(results);
    auto keyType = results.physicalType;
    results.out << ", ";
    TypeAnalysis<V>().run(results);
    auto valueType = results.physicalType;
    results.out << ")";
    results.physicalType = MAP(keyType, valueType);
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
    for (auto& [_, variable] : tmp.variablesInformation) {
      results.addVariable(std::move(variable));
    }
    results.out << tmp.typeAsString();
    results.physicalType = tmp.physicalType;
  }
};

template <typename V>
struct TypeAnalysis<Array<V>> {
  void run(TypeAnalysisResults& results) {
    results.stats.concreteCount++;
    results.out << "array(";
    TypeAnalysis<V>().run(results);
    results.out << ")";
    results.physicalType = ARRAY(results.physicalType);
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
    std::vector<TypePtr> fieldTypes;
    // This expression applies the lambda for each row child type.
    bool first = true;
    (
        [&]() {
          if (!first) {
            results.out << ", ";
          }
          first = false;
          TypeAnalysis<T>().run(results);
          fieldTypes.push_back(results.physicalType);
        }(),
        ...);
    results.out << ")";
    results.physicalType = ROW(std::move(fieldTypes));
  }
};

template <typename T>
struct TypeAnalysis<CustomType<T>> {
  void run(TypeAnalysisResults& results) {
    results.stats.concreteCount++;
    results.out << T::typeName;

    TypeAnalysisResults tmp;
    TypeAnalysis<typename T::type>().run(tmp);
    results.physicalType = tmp.physicalType;
  }
};

class ISimpleFunctionMetadata {
 public:
  virtual ~ISimpleFunctionMetadata() = default;

  // Return the return type of the function if its independent on the input
  // types, otherwise return null.
  virtual TypePtr tryResolveReturnType() const = 0;
  virtual std::string getName() const = 0;
  virtual bool isDeterministic() const = 0;
  virtual uint32_t priority() const = 0;
  virtual const std::shared_ptr<exec::FunctionSignature> signature() const = 0;
  virtual const TypePtr& resultPhysicalType() const = 0;
  virtual const std::vector<TypePtr>& argPhysicalTypes() const = 0;
  virtual bool physicalSignatureEquals(
      const ISimpleFunctionMetadata& other) const = 0;
  virtual std::string helpMessage(const std::string& name) const = 0;
};

template <typename T, typename = int32_t>
struct udf_has_name : std::false_type {};

template <typename T>
struct udf_has_name<T, decltype(&T::name, 0)> : std::true_type {};

template <
    typename Fun,
    typename TReturn,
    typename ConstantChecker,
    typename... Args>
class SimpleFunctionMetadata : public ISimpleFunctionMetadata {
 public:
  using return_type = TReturn;
  using arg_types = std::tuple<Args...>;
  template <size_t N>
  using type_at = typename std::tuple_element<N, arg_types>::type;
  static constexpr int num_args = std::tuple_size<arg_types>::value;

 public:
  template <typename T>
  struct CreateType {
    static auto create(const exec::FunctionSignaturePtr& signature) {
      auto type = velox::exec::SignatureBinder::tryResolveType(
          signature->returnType(), {}, {});
      return type;
    }
  };

  // Signature does not capture the type index of the opaque type.
  template <typename T>
  struct CreateType<std::shared_ptr<T>> {
    // We override the type with the concrete specialization here!
    // using NativeType = std::shared_ptr<T>;
    static auto create(const exec::FunctionSignaturePtr& signature) {
      return OpaqueType::create<T>();
    }
  };

  virtual TypePtr tryResolveReturnType() const final {
    return CreateType<return_type>::create(signature());
  }

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

  static constexpr bool isVariadic() {
    if constexpr (num_args == 0) {
      return false;
    } else {
      return isVariadicType<type_at<num_args - 1>>::value;
    }
  }

  explicit SimpleFunctionMetadata(
      const std::vector<exec::SignatureVariable>& constraints) {
    auto analysis = analyzeSignatureTypes(constraints);

    buildSignature(analysis);
    priority_ = analysis.stats.computePriority();
    resultPhysicalType_ = analysis.resultPhysicalType;
    argPhysicalTypes_ = analysis.argPhysicalTypes;
  }

  ~SimpleFunctionMetadata() override = default;

  const exec::FunctionSignaturePtr signature() const override {
    return signature_;
  }

  const TypePtr& resultPhysicalType() const override {
    return resultPhysicalType_;
  }

  const std::vector<TypePtr>& argPhysicalTypes() const override {
    return argPhysicalTypes_;
  }

  bool physicalSignatureEquals(
      const ISimpleFunctionMetadata& other) const override {
    if (!resultPhysicalType_->kindEquals(other.resultPhysicalType())) {
      return false;
    }

    if (argPhysicalTypes_.size() != other.argPhysicalTypes().size()) {
      return false;
    }

    for (auto i = 0; i < argPhysicalTypes_.size(); ++i) {
      if (!argPhysicalTypes_[i]->kindEquals(other.argPhysicalTypes()[i])) {
        return false;
      }
    }

    return true;
  }

  std::string helpMessage(const std::string& name) const final {
    // return fmt::format("{}({})", name, signature_->toString());
    std::string s{name};
    s.append("(");
    bool first = true;
    for (auto& arg : signature_->argumentTypes()) {
      if (!first) {
        s.append(", ");
      }
      first = false;
      s.append(boost::algorithm::to_upper_copy(arg.toString()));
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
    std::map<std::string, exec::SignatureVariable> variables;
    TypeAnalysisResults::Stats stats;
    TypePtr resultPhysicalType;
    std::vector<TypePtr> argPhysicalTypes;
  };

  SignatureTypesAnalysisResults analyzeSignatureTypes(
      const std::vector<exec::SignatureVariable>& constraints) {
    std::vector<std::string> argsTypes;

    TypeAnalysisResults results;
    TypeAnalysis<return_type>().run(results);
    std::string outputType = results.typeAsString();
    const auto resultPhysicalType = results.physicalType;
    std::vector<TypePtr> argPhysicalTypes;

    (
        [&]() {
          // Clear string representation but keep other collected information
          // to accumulate.
          results.resetTypeString();
          TypeAnalysis<Args>().run(results);
          argsTypes.push_back(results.typeAsString());
          argPhysicalTypes.push_back(results.physicalType);
        }(),
        ...);

    for (const auto& constraint : constraints) {
      VELOX_CHECK(
          !constraint.constraint().empty(),
          "Constraint must be set for variable {}",
          constraint.name());

      results.variablesInformation.erase(constraint.name());
      results.variablesInformation.emplace(constraint.name(), constraint);
    }

    return SignatureTypesAnalysisResults{
        std::move(argsTypes),
        std::move(outputType),
        std::move(results.variablesInformation),
        std::move(results.stats),
        resultPhysicalType,
        argPhysicalTypes};
  }

  void buildSignature(const SignatureTypesAnalysisResults& analysis) {
    auto builder = exec::FunctionSignatureBuilder();

    builder.returnType(analysis.outputType);
    int32_t position = 0;
    for (const auto& arg : analysis.argsTypes) {
      if (ConstantChecker::isConstant[position++]) {
        builder.constantArgumentType(arg);
      } else {
        builder.argumentType(arg);
      }
    }

    for (const auto& [_, variable] : analysis.variables) {
      builder.variable(variable);
    }

    if (isVariadic()) {
      builder.variableArity();
    }
    signature_ = builder.build();
  }

  exec::FunctionSignaturePtr signature_;
  uint32_t priority_;
  TypePtr resultPhysicalType_;
  std::vector<TypePtr> argPhysicalTypes_;
};

// wraps a UDF object to provide the inheritance
// this is basically just boilerplate-avoidance
template <
    typename Fun,
    typename Exec,
    typename TReturn,
    typename ConstantChecker,
    typename... TArgs>
class UDFHolder {
  Fun instance_;

 public:
  using return_type = TReturn;
  using arg_types = std::tuple<TArgs...>;
  template <size_t N>
  using type_at = typename std::tuple_element<N, arg_types>::type;
  static constexpr int num_args = std::tuple_size<arg_types>::value;

  using udf_struct_t = Fun;
  using Metadata =
      core::SimpleFunctionMetadata<Fun, TReturn, ConstantChecker, TArgs...>;

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
      const std::vector<TypePtr>&,
      const core::QueryConfig&,
      const exec_arg_type<TArgs>*...>::value;

  // TODO Remove
  static constexpr bool udf_has_legacy_initialize = util::has_method<
      Fun,
      initialize_method_resolver,
      void,
      const core::QueryConfig&,
      const exec_arg_type<TArgs>*...>::value;

  static_assert(
      !udf_has_legacy_initialize,
      "Legacy initialize method! Upgrade.");

  static_assert(
      udf_has_call || udf_has_callNullable || udf_has_callNullFree,
      "UDF must implement at least one of `call`, `callNullable`, or `callNullFree` functions.\n"
      "This error happens also if the output and input types of the functions do not match the\n"
      "ones used in registration.");

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

  explicit UDFHolder() : instance_{} {}

  exec::FunctionCanonicalName getCanonicalName() const {
    return udf_canonical_name<Fun>::value;
  }

  bool isDeterministic() const {
    return udf_is_deterministic<Fun>();
  }

  static constexpr bool isVariadic() {
    if constexpr (num_args == 0) {
      return false;
    } else {
      return isVariadicType<type_at<num_args - 1>>::value;
    }
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const typename exec_resolver<TArgs>::in_type*... constantArgs) {
    if constexpr (udf_has_initialize) {
      return instance_.initialize(inputTypes, config, constantArgs...);
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
