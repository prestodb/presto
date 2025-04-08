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

#include <pybind11/embed.h>
#include "velox/python/type/PyType.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::py {

class PyVector {
 public:
  /// PyVector is a thin wrapper around Velox Vector. It wraps the underlying
  /// vector and can optionally hold owernship over the memory pool used by the
  /// vector's allocation, to prevent Python's garbage collection to arbitrarily
  /// decide on their destruction order.
  explicit PyVector(
      const VectorPtr& vector,
      std::shared_ptr<memory::MemoryPool> pool = nullptr)
      : pool_(std::move(pool)), vector_(vector) {}

  /// Returns a wrapper to the Vector type.
  PyType type() const {
    return PyType{vector_->type()};
  }

  // TODO: This is only for printing/debugging for now as it returns the
  // value at idx converted to a string.
  std::string operator[](int32_t idx) const {
    return vector_->toString(idx);
  }

  /// Returns a string summarizing the vector type, encoding and size, e.g:
  ///
  ///   [FLAT BIGINT: 2 elements, no nulls]
  std::string toString() const {
    return vector_->toString(true);
  }

  /// Returns a string containing the values for each record, e.g:
  ///
  ///   0: 1
  ///   1: 2
  ///   ...
  std::string printAll() const {
    return vector_->toString(0, vector_->size());
  }

  /// Prints a long descriptive string of the vector and its values.
  std::string printDetailed() const;

  /// Prints a human-readable summary of the vector.
  std::string summarizeToText() const;

  size_t size() const {
    return vector_->size();
  }

  /// Number of nulls in the vector.
  size_t nullCount() const {
    return BaseVector::countNulls(vector_->nulls(), 0, size());
  }

  /// If vector is null at `idx`.
  bool isNullAt(vector_size_t idx) const {
    return vector_->isNullAt(idx);
  }

  /// Returns the vector's child at position `idx`. Throws if the vector is not
  /// a RowVector.
  PyVector childAt(vector_size_t idx) const;

  /// Compares the current vector at position `index` with `other` at position
  /// `otherIndex`. Return zero if they are equal; non-zero otherwise.
  int32_t compare(
      const PyVector& other,
      vector_size_t index,
      vector_size_t otherIndex) const {
    return vector_->compare(other.vector_.get(), index, otherIndex);
  }

  /// Returns if two PyVectors have the same size and contents.
  bool equals(const velox::py::PyVector& other) const {
    if (size() != other.size()) {
      return false;
    }
    for (vector_size_t i = 0; i < size(); ++i) {
      if (compare(other, i, i) != 0) {
        return false;
      }
    }
    return true;
  }

  VectorPtr vector() const {
    return vector_;
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_;
  VectorPtr vector_;
};

} // namespace facebook::velox::py
