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

#include "velox/parse/IExpr.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::exec::test {
/// Defines a transform for an intermediate type.
class IntermediateTypeTransform {
 public:
  virtual ~IntermediateTypeTransform() = default;

  /// The type of the value returned by transform().
  virtual TypePtr transformedType() const = 0;

  /// Converts the value in vector at position row into a value that can be
  /// converted back to the original type by the result of projectExpr. Will
  /// only be called with a ConstantVector or a flat-like Vector (depending on
  /// the intermediate type's parent type) where the value is non-null.
  virtual variant transform(const BaseVector* const vector, vector_size_t row)
      const = 0;

  /// An expression tree that can convert the value returned by transform() back
  /// into its original value. It is assumed this has default-null behavior,
  /// i.e. a null input produces a null output.
  virtual core::ExprPtr projectionExpr(
      const core::ExprPtr& inputExpr,
      const std::string& columnAlias) const = 0;
};

/// Returns true if this types is an intermediate only type or contains an
/// intermediate only type.
bool isIntermediateOnlyType(const TypePtr& type);

/// Converts a Vector of an intermediate only type, or containing one, to a
/// Vector of value(s) that can be input to a projection to produce those values
/// of that type but are of types supported as input. Preserves nulls and
/// encodings.
VectorPtr transformIntermediateOnlyType(const VectorPtr& vector);

/// Converts an expression that takes in a value of an intermediate only type so
/// that it applies a transformation to convert valid input types into values
/// of the intermediate only type.
/// @param type The expected output type of the expression, either an
/// intermediate only type, or a complex type containing one.
/// @param inputExpr The expression that will be the input to the returned
/// expression, the output of this expression will be of a type not containing
/// intermediate only types.
/// @param columnAlias The alias to give the returned expression.
core::ExprPtr getIntermediateOnlyTypeProjectionExpr(
    const TypePtr& type,
    const core::ExprPtr& inputExpr,
    const std::string& columnAlias);
} // namespace facebook::velox::exec::test
