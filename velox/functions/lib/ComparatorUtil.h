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
#include "velox/expression/EvalCtx.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions::lib {

// Sort on an indices array by corresponding values array
template <typename T>
struct Index2ValueNullableLess {
  explicit Index2ValueNullableLess(
      const exec::LocalDecodedVector& decodedVector)
      : decodedVector_(decodedVector) {}

  constexpr bool operator()(const vector_size_t& a, const vector_size_t& b)
      const {
    // null should be moved to the head of the array
    if (decodedVector_->isNullAt(a)) {
      return !decodedVector_->isNullAt(b);
    }
    if (decodedVector_->isNullAt(b)) {
      return false;
    }
    return decodedVector_->valueAt<T>(a) < decodedVector_->valueAt<T>(b);
  }

 private:
  const exec::LocalDecodedVector& decodedVector_;
};

template <typename T>
struct Index2ValueNullableGreater : private Index2ValueNullableLess<T> {
  constexpr bool operator()(const vector_size_t& a, const vector_size_t& b)
      const {
    return Index2ValueNullableGreater<T>::operator()(b, a);
  }
};
} // namespace facebook::velox::functions::lib
