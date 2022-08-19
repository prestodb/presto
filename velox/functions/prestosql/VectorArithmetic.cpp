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

#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::exec {

/// The goal of this file is to provide a guide on how to develop efficient
/// vector functions. Vector functions are generally used in one of the
/// following cases:
/// 1) When dealing with complex vectors such as RowVector, MapVector and
/// ArrayVector it is usually recommended to use vector functions to take
/// advantage of the meta data locality in the complex vectors.
/// 2) If the function needs to keep around state from each element to the
/// next it's preferred to use meta data. For example if the data is bitmap
/// (boolean) for operations like Coalese etc.
/// 3) When for some constant input values major optimization is possible such
/// as the case for regular expressions.
/// 4) When reusing one of the input vectors for creating the output can lead
/// to major gains. Examples are most read only string function such as substr!
///
/// For other use cases, it is much preferred to use scalar functions by
/// following these steps:
///
/// (1) define the function body between VELOX_UDF_BEGIN and VELOX_UDF_END
/// macros.
/// (2) declare it using VELOX_DECLARE_VECTOR_FUNCTION macros.
/// (3) register it using VELOX_DECLARE_VECTOR_FUNCTION macro.
///
/// A VectorFunction can be broken into the following template pieces
///
/// 1) apply function: in charge of performing dynamic dispatch and passing the
/// input to applyTyped which is template-ized vector function
/// 2) applyTyped function: usually in charge of managing vector encodings and
/// ruse before implementing dense or sparse vector loops which performs calls
/// into the scalar level kernels for each data element in valid row range
/// using applyToSelect functions. This function can be implemented in a fast
/// and a slow path depending on the the decoding costs.
/// 3) Implement the type inference function
/// 4) registering your functions using registerVectorFunction

template <typename Operation>
class VectorArithmetic : public VectorFunction {
 public:
  /// apply acts as the main function for a VectorFunction. The apply function
  /// usually performs dynamic dispatch based on the input types and forwards
  /// the inputs to a typed version of the function.
  ///
  /// \param rows: range of row indexes. For example if the function is null
  /// safe which is the case for most SQL function, the row inputs will not have
  /// the input indexes associated with null inputs on any of the values in args
  /// vectors.
  /// \param args: the input vectors to the function
  /// \param caller: the output type
  /// \param context: the eval context that can be used for memory allocation or
  /// accessing config flags
  /// \param result: the output vector that must be created or reuse one of the
  /// inputs that will not be used in the future
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      EvalCtx* context,
      VectorPtr* result) const override {
    // Same basic checks on the number of inputs and their kinds
    VELOX_CHECK(args.size() == 2);
    VELOX_CHECK(args[0]->typeKind() == args[1]->typeKind());
    // Dispatch dynamically to applyTyped
    switch (args[0]->typeKind()) {
      case TypeKind::INTEGER:
        applyTyped<int32_t>(rows, args, outputType, context, result);
        return;
      case TypeKind::BIGINT:
        applyTyped<int64_t>(rows, args, outputType, context, result);
        return;
      case TypeKind::REAL:
        applyTyped<float>(rows, args, outputType, context, result);
        return;
      case TypeKind::DOUBLE:
        applyTyped<double>(rows, args, outputType, context, result);
        return;
      default:
        VELOX_CHECK(false, "Bad type for arithmetic");
    }
  }

 private:
  /// The main body of the function which
  /// (1) decodes the inputs or reuse them
  /// (2) performs special initializations if needed
  /// (3) creates output vector (or reuse one of the inputs)
  /// (4) makes proper calls to the scalar kernels
  ///
  /// \tparam T function argument cpp type
  /// \param rows the valid row indexes
  /// \param args the input argument vectors
  /// \param outputType the output type
  /// \param context the eval context
  /// \param result the generated or reused result vector
  template <typename T>
  void applyTyped(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      EvalCtx* context,
      VectorPtr* result) const {
    BaseVector* left = args[0].get();
    BaseVector* right = args[1].get();
    auto leftEncoding = left->encoding();
    auto rightEncoding = right->encoding();

    // Step 1: Create the output vector
    //
    // Check if the output vector is not initialized, we either use one of
    // re-usable input or create it.
    // To check if an input is re-usable:
    // (1) Its unique() property must be true!
    // (2) the input type must match the output vector type
    // (3) usually we try to reuse inputs with flat encoding
    if (!*result) {
      if (args[0].unique() && leftEncoding == VectorEncoding::Simple::FLAT) {
        *result = std::move(args[0]);
      } else if (
          args[1].unique() && rightEncoding == VectorEncoding::Simple::FLAT) {
        *result = std::move(args[1]);
      } else {
        *result = BaseVector::create(outputType, rows.size(), context->pool());
      }
    } else {
      // if the output is previously initialized, we prepare it for writing
      // here using ensureWritable
      context->ensureWritable(rows, outputType, *result);
    }
    // Here we provide a pointer to the raw flat results.
    BufferPtr resultValues =
        (*result)->as<FlatVector<T>>()->mutableValues(rows.end());
    T* __restrict rawResult = resultValues->asMutable<T>();

    // Step 2: handle input encodings and call the inner kernels
    // Step 2.1 (the fast path): handling flat and constant encodings
    // If both inputs are flat, we can use the input raw input and output data
    // as direct arrays!
    if (leftEncoding == VectorEncoding::Simple::FLAT) {
      const T* __restrict rawLeft = left->as<FlatVector<T>>()->rawValues();
      if (rightEncoding == VectorEncoding::Simple::FLAT) {
        const T* __restrict rawRight = right->as<FlatVector<T>>()->rawValues();
        // Additional optimization if the data is dense (all rows are valid
        // or the cost is worth it! Please check the description for
        // processAsDense heuristic function)
        // use a very tight loop
        if (processAsDense<T>(rows, context)) {
          auto end = rows.end();
          for (auto i = rows.begin(); i < end; ++i) {
            Operation::apply(rawResult[i], rawLeft[i], rawRight[i]);
          }
        } else {
          // If the inputs are not dense we use applyToSelected on rows which
          // only applies the kernels on the valid rows
          rows.applyToSelected([&](int row) {
            Operation::apply(rawResult[row], rawLeft[row], rawRight[row]);
          });
        }
        return;
      }

      // If one of the encodings is constant (which usually happens when the
      // input is a literal) we can use some customized kernels or perform
      // some initialization before calling the kernels. Here in this example
      // before calling into the kernel, we extract the constant value using
      // as<ConstantVector<T>>()->valueAt(0)!
      if (rightEncoding == VectorEncoding::Simple::CONSTANT) {
        T constant = right->as<ConstantVector<T>>()->valueAt(0);
        if (processAsDense<T>(rows, context)) {
          auto end = rows.end();
          for (auto i = rows.begin(); i < end; ++i) {
            Operation::apply(rawResult[i], rawLeft[i], constant);
          }
        } else {
          rows.applyToSelected([&](int row) {
            Operation::apply(rawResult[row], rawLeft[row], constant);
          });
        }
        return;
      }
    }

    // Handle other important encoding depending on your case. For example here,
    // we candle constant for the left and flat encoding for right arguments
    if (leftEncoding == VectorEncoding::Simple::CONSTANT &&
        rightEncoding == VectorEncoding::Simple::FLAT) {
      T constant = left->as<ConstantVector<T>>()->valueAt(0);
      const T* __restrict rawRight = right->as<FlatVector<T>>()->rawValues();
      if (processAsDense<T>(rows, context)) {
        auto end = rows.end();
        for (auto i = rows.begin(); i < end; ++i) {
          Operation::apply(rawResult[i], constant, rawRight[i]);
        }
      } else {
        rows.applyToSelected([&](int row) {
          Operation::apply(rawResult[row], constant, rawRight[row]);
        });
      }
      return;
    }

    // Step 2.2 (the slow path): handling the rest of the encodings using
    // decoded vectors! Decoded vector is the developer API that lets us decode
    // and access any vector with any decoding using the following interface:
    // - decode: decode the input vector
    // - valueAt: get the value after decoding in particular index
    // - nullAt: check if the value is null in a particular index (not used if
    // we are using applyToSelected)
    // Given the cost of decoding here we usually do not optimize for the dense
    // cases
    LocalDecodedVector leftHolder(context);
    LocalDecodedVector rightHolder(context);
    auto leftDecoded = leftHolder.get();
    auto rightDecoded = rightHolder.get();
    leftDecoded->decode(*left, rows);
    rightDecoded->decode(*right, rows);
    rows.applyToSelected([&](int row) {
      Operation::apply(
          rawResult[row],
          leftDecoded->valueAt<T>(row),
          rightDecoded->valueAt<T>(row));
    });
  }

  /// A utility function used to determine if the function can be applied as a
  /// dense type loop (usually important for builtin less complex types)
  /// You can reuse this and add your heuristic to your function
  /// \tparam T template type of the input vector
  /// \param rows the valid rows
  /// \param context the input context
  /// \return the function can be implemented in a dense loop
  template <typename T>
  inline static bool processAsDense(
      const SelectivityVector& rows,
      EvalCtx* context) {
    // Consider it dense if one of these:
    // 1) If the rows are all valid
    // 2) type is integral and is safe to leave the values uninitialized
    // and the number of valid rows is larger than half of the row (heuristic)
    return rows.isAllSelected() ||
        (std::is_integral_v<T> && Operation::isSafeForUninitialized &&
         context->isFinalSelection() && rows.countSelected() > rows.size() / 2);
  }
};

/// The body of your inner kernels (type independent). For String function you
/// have the choice of using StringWriter APIs but they are designed to be
/// usually used by scalar string functions
class Addition {
 public:
  static constexpr bool isSafeForUninitialized = true;

  template <
      typename T,
      typename std::enable_if<!std::is_integral_v<T>, int>::type = 0>
  static void apply(T& result, T left, T right) {
    result = left + right;
  }

  template <
      typename T,
      typename std::enable_if<std::is_integral_v<T>, int>::type = 0>
  static void apply(T& result, T left, T right) {
    bool overflow = __builtin_add_overflow(left, right, &result);
    if (UNLIKELY(overflow)) {
      throw std::overflow_error{
          "integer overflow: " + std::to_string(left) + " + " +
          std::to_string(right)};
    }
  }
};

class Subtraction {
 public:
  static constexpr bool isSafeForUninitialized = true;

  template <
      typename T,
      typename std::enable_if<!std::is_integral_v<T>, int>::type = 0>
  static void apply(T& result, T left, T right) {
    result = left - right;
  }

  template <
      typename T,
      typename std::enable_if<std::is_integral_v<T>, int>::type = 0>
  static void apply(T& result, T left, T right) {
    bool overflow = __builtin_sub_overflow(left, right, &result);
    if (UNLIKELY(overflow)) {
      throw std::overflow_error{
          "integer overflow: " + std::to_string(left) + " - " +
          std::to_string(right)};
    }
  }
};

class Multiplication {
 public:
  static constexpr bool isSafeForUninitialized = true;

  template <
      typename T,
      typename std::enable_if<!std::is_integral_v<T>, int>::type = 0>

  static void apply(T& result, T left, T right) {
    result = left * right;
  }

  template <
      typename T,
      typename std::enable_if<std::is_integral_v<T>, int>::type = 0>
  static void apply(T& result, T left, T right) {
    bool overflow = __builtin_mul_overflow(left, right, &result);
    if (UNLIKELY(overflow)) {
      throw std::overflow_error{
          "integer overflow: " + std::to_string(left) + " - " +
          std::to_string(right)};
    }
  }
};

class Division {
 public:
  static constexpr bool isSafeForUninitialized = false;

  template <typename T>
  static void apply(T& result, T left, T right) {
    if (right == 0) {
      throw std::runtime_error("Cannot divide by zero");
    }
    result = left / right;
  }
};

/// Registering your functions using registerVectorFunction
namespace {
class ArithmeticRegistration {
 public:
  ArithmeticRegistration() {
    std::vector<std::string> types = {
        "tinyint", "smallint", "integer", "bigint", "real", "double"};
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;
    for (const auto& type : types) {
      signatures.emplace_back(exec::FunctionSignatureBuilder()
                                  .returnType(type)
                                  .argumentType(type)
                                  .argumentType(type)
                                  .build());
    }

    registerVectorFunction(
        "plus", signatures, std::make_unique<VectorArithmetic<Addition>>());
    registerVectorFunction(
        "minus", signatures, std::make_unique<VectorArithmetic<Subtraction>>());
    registerVectorFunction(
        "multiply",
        signatures,
        std::make_unique<VectorArithmetic<Multiplication>>());
    registerVectorFunction(
        "divide", signatures, std::make_unique<VectorArithmetic<Division>>());
  }
};

// The main registered object is intentionally commented out as this file
// is intended for training only
// static ArithmeticRegistration registered;
} // namespace

} // namespace facebook::velox::exec
