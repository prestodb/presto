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

namespace facebook::velox::exec {

struct VectorFunctionMetadata {
  /// Boolean indicating whether this function supports flattening, i.e.
  /// converting a set of nested calls into a single call.
  ///
  ///     f(a, f(b, f(c, d))) => f(a, b, c, d).
  ///
  /// For example, concat(string,...), concat(array,...), map_concat(map,...)
  /// Presto functions support flattening. Similarly, built-in special format
  /// AND and OR also support flattening.
  ///
  /// A function that supports flattening must have a signature with variadic
  /// arguments of the same type. The result type must be the same as input
  /// type.
  bool supportsFlattening{false};

  /// True if the function is deterministic, e.g given same inputs always
  /// returns same result.
  bool deterministic{true};

  /// True if null in any argument always produces null result.
  /// In this case, 'rows' in VectorFunction::apply will point only to positions
  /// for which all arguments are not null.
  bool defaultNullBehavior{true};
};

class VectorFunctionMetadataBuilder {
 public:
  VectorFunctionMetadataBuilder& supportsFlattening(bool supportsFlattening) {
    metadata_.supportsFlattening = supportsFlattening;
    return *this;
  }

  VectorFunctionMetadataBuilder& deterministic(bool deterministic) {
    metadata_.deterministic = deterministic;
    return *this;
  }

  VectorFunctionMetadataBuilder& defaultNullBehavior(bool defaultNullBehavior) {
    metadata_.defaultNullBehavior = defaultNullBehavior;
    return *this;
  }

  const VectorFunctionMetadata& build() const {
    return metadata_;
  }

 private:
  VectorFunctionMetadata metadata_;
};

} // namespace facebook::velox::exec
