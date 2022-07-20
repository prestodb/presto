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

#include <vector>
#include "velox/expression/EvalCtx.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/SimpleVector.h"

#include <folly/Synchronized.h>
namespace facebook::velox::exec {

class Expr;
class ExprSet;
class EvalCtx;

// Superclass for functions which are written to run on whole vectors.
class VectorFunction {
 public:
  virtual ~VectorFunction() = default;

  /// Evaluate the function on the specified rows.
  ///
  /// The rows may or may not be contiguous.
  ///
  /// Single-argument deterministic functions may receive their only argument
  /// vector as flat or constant, but not dictionary encoded.
  ///
  /// Single-argument functions that specify null-in-null-out behavior, e.g.
  /// isDefaultNullBehavior returns true, will never see a null row in 'rows'.
  /// Hence, they can safely assume that args[0] vector is flat or constant and
  /// has no nulls in specified positions.
  ///
  /// Multi-argument functions that specify null-in-null-out behavior will never
  /// see a null row in any of the arguments. They can safely assume that there
  /// are no nulls in any of the arguments in specified positions.
  ///
  /// If context->isFinalSelection() is false, the result may have been
  /// partially populated for the positions not specified in rows. The function
  /// must take care not to overwrite these values. This happens when evaluating
  /// conditional expressions. Consider if(a = 1, f(b), g(c)) expression. The
  /// engine first evaluates a = 1, then invokes f(b) for passing rows, then
  /// invokes g(c) for the rest of the rows. f(b) is invoked with null result,
  /// but g(c) is called with partially populated result which has f(b) values
  /// in some positions. Function g must preserve these values.
  ///
  /// Use context->isFinalSelection() to determine whether partially populated
  /// results must be preserved or not.
  ///
  /// If 'result' is not null, it can be dictionary, constant or sequence
  /// encoded and therefore may be read-only. Call BaseVector::ensureWritable
  /// before writing into the "result", e.g.
  ///
  ///    BaseVector::ensureWritable(&rows, caller->type(), context->pool(),
  ///        result);
  ///
  /// If 'result' is null, the function can allocate a new vector using
  /// BaseVector::create or reuse memory of its argument(s). Memory reuse is
  /// safe if the function returns its argument unmodified or the argument and
  /// its contents are uniquely referenced.
  virtual void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args, // Not using const ref so we can reuse args
      const TypePtr& outputType,
      EvalCtx* context,
      VectorPtr* result) const = 0;

  virtual bool isDeterministic() const {
    return true;
  }

  // Returns true if null in any argument always produces null result.
  // In this case, "rows" in "apply" will point only to positions for
  // which all arguments are not null.
  virtual bool isDefaultNullBehavior() const {
    return true;
  }

  // The evaluation engine will scan and set the string encoding of the
  // specified input arguments when presented if their type is VARCHAR before
  // applying the function
  virtual std::vector<size_t> ensureStringEncodingSetAt() const {
    return {};
  }

  // Same as ensureStringEncodingSetAt but for all string inputs
  virtual bool ensureStringEncodingSetAtAllInputs() const {
    return false;
  }

  // If set, the string encoding of the results will be set by propagating
  // the string encoding from all provided string inputs.
  virtual bool propagateStringEncodingFromAllInputs() const {
    return false;
  }

  // If set, the string encoding of the results will be set by propagating
  // the specified inputs string encodings if presented.
  // If one of the specified inputs have its encoding not determined, the
  // encoding of the result is not determined.
  virtual std::optional<std::vector<size_t>> propagateStringEncodingFrom()
      const {
    return std::nullopt;
  }
};

// Factory for functions which are template generated from simple functions.
class SimpleFunctionAdapterFactory {
 public:
  virtual std::unique_ptr<VectorFunction> createVectorFunction(
      const core::QueryConfig& config,
      const std::vector<VectorPtr>& constantInputs) const = 0;
  virtual ~SimpleFunctionAdapterFactory() = default;
};

/// Returns a list of signatures supported by VectorFunction with the specified
/// name. Returns std::nullopt if there is no function with the specified name.
std::optional<std::vector<FunctionSignaturePtr>> getVectorFunctionSignatures(
    const std::string& name);

/// Returns an instance of VectorFunction for the given name, input types and
/// optionally constant input values.
/// constantInputs should be empty if there are no constant inputs.
/// constantInputs should be aligned with inputTypes if there is at least one
/// constant input; non-constant inputs should be represented as nullptr;
/// constant inputs must be instances of ConstantVector.
std::shared_ptr<VectorFunction> getVectorFunction(
    const std::string& name,
    const std::vector<TypePtr>& inputTypes,
    const std::vector<VectorPtr>& constantInputs);

/// Registers stateless VectorFunction. The same instance will be used for all
/// expressions.
/// Returns true iff an new function is inserted
bool registerVectorFunction(
    const std::string& name,
    std::vector<FunctionSignaturePtr> signatures,
    std::unique_ptr<VectorFunction> func,
    bool overwrite = true);

// Represents arguments for stateful vector functions. Stores element type, and
// the constant value (if supplied).
struct VectorFunctionArg {
  const TypePtr type;
  const VectorPtr constantValue;
};

using VectorFunctionFactory = std::function<std::shared_ptr<VectorFunction>(
    const std::string& name,
    const std::vector<VectorFunctionArg>& inputArgs)>;

struct VectorFunctionEntry {
  std::vector<FunctionSignaturePtr> signatures;
  VectorFunctionFactory factory;
};

// TODO: Use folly::Singleton here
using VectorFunctionMap =
    folly::Synchronized<std::unordered_map<std::string, VectorFunctionEntry>>;

VectorFunctionMap& vectorFunctionFactories();

// A template to simplify making VectorFunctionFactory for a function that has a
// constructor that takes inputTypes and constantInputs
//
// Example of usage:
//
// VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
//    udf_my_function, exec::makeVectorFunctionFactory<MyFunction>());
template <typename T>
VectorFunctionFactory makeVectorFunctionFactory() {
  return [](const std::string& name,
            const std::vector<VectorFunctionArg>& inputArgs) {
    return std::make_shared<T>(name, inputArgs);
  };
}

// Registers stateful VectorFunction. New instance is created for each
// expression using input types and optionally constant values for some inputs.
// When overwrite is true, any previously registered VectorFunction with the
// name is replaced.
// Returns true iff the function was inserted
bool registerStatefulVectorFunction(
    const std::string& name,
    std::vector<FunctionSignaturePtr> signatures,
    VectorFunctionFactory factory,
    bool overwrite = true);

} // namespace facebook::velox::exec

// Private. Return the external function name given a UDF tag.
#define _VELOX_REGISTER_FUNC_NAME(tag) registerVectorFunction_##tag

// Declares a vectorized UDF function given a tag. Goes into the UDF .cpp file.
#define VELOX_DECLARE_VECTOR_FUNCTION(tag, signatures, function) \
  void _VELOX_REGISTER_FUNC_NAME(tag)(const std::string& name) { \
    facebook::velox::exec::registerVectorFunction(               \
        (name), (signatures), (function));                       \
  }

// Declares a stateful vectorized UDF.
#define VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(tag, signatures, function) \
  void _VELOX_REGISTER_FUNC_NAME(tag)(const std::string& name) {          \
    facebook::velox::exec::registerStatefulVectorFunction(                \
        (name), (signatures), (function));                                \
  }

// Registers a vectorized UDF associated with a given tag.
// This should be used in the same namespace the declare macro is used in.
#define VELOX_REGISTER_VECTOR_FUNCTION(tag, name)                   \
  {                                                                 \
    extern void _VELOX_REGISTER_FUNC_NAME(tag)(const std::string&); \
    _VELOX_REGISTER_FUNC_NAME(tag)(name);                           \
  }
