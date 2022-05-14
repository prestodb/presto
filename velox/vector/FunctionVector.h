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

#include "velox/vector/BaseVector.h"

namespace facebook::velox {

namespace exec {
class EvalCtx;
} // namespace exec

// Represents a function with possible captures.
class Callable {
 public:
  virtual ~Callable() = default;

  virtual bool hasCapture() const = 0;

  // Applies 'this' to 'args' for 'rows' and returns the result in
  // '*result'.  'wrapCapture' translates row numbers in 'rows' to the
  // corresponding numbers for captured variables, i.e. is an indices
  // vector that is used for wrapping captured variables in a
  // dictionary before passing these to the function. The typical use
  // case of lambdas applies a function to elements of repeated types,
  // so that the values of the arguments and captures are not
  // aligned. This serves to align these. If nullptr, the captures are
  // passed as is.
  virtual void apply(
      const SelectivityVector& rows,
      BufferPtr wrapCapture,
      exec::EvalCtx* context,
      const std::vector<VectorPtr>& args,
      VectorPtr* result) = 0;
};

// Represents a vector of functions. In most cases all the positions
// in the vector have the same function. We allow for different
// functions at different positions for completeness.
class FunctionVector : public BaseVector {
 public:
  // Iterates over distinct functions that fall inside 'rows'. The
  // Callable and the applicable subset of 'rows' is returned for each
  // distinct function by next(). A vectorized lambda application
  // first loops over the functions, then applies each to the
  // applicable rows. In most situations, there is only one function in
  // the FunctionVector.
  class Iterator {
   public:
    struct Entry {
      /// Callable lambda.
      Callable* callable;

      /// Rows that lambda applies to.
      SelectivityVector* rows;

      operator bool() const {
        return callable != nullptr;
      }
    };

    Iterator(const FunctionVector* vector, const SelectivityVector* rows)
        : rows_(*rows),
          functions_{vector->functions_},
          rowSets_{vector->rowSets_} {}

    Entry next() {
      while (index_ < functions_.size()) {
        effectiveRows_ = rowSets_[index_];
        effectiveRows_.intersect(rows_);
        if (!effectiveRows_.hasSelections()) {
          ++index_;
          continue;
        }
        Entry entry{functions_[index_].get(), &effectiveRows_};
        ++index_;
        return entry;
      }
      return {nullptr, nullptr};
    }

   private:
    const SelectivityVector& rows_;
    const std::vector<std::shared_ptr<Callable>>& functions_;
    const std::vector<SelectivityVector>& rowSets_;
    int32_t index_ = 0;
    SelectivityVector effectiveRows_;
  };

  FunctionVector(velox::memory::MemoryPool* pool, TypePtr type)
      : BaseVector(
            pool,
            type,
            VectorEncoding::Simple::FUNCTION,
            BufferPtr(nullptr),
            0) {}

  // Implements evaluation of the lambda function literal special
  // form. This assigns a function to a specified set of rows. This
  // supports a situation of returning different lambdas from
  // different cases of a conditional. FunctionVectors enforce single
  // assignment. A lambda cannot be for example a loop variable.
  void addFunction(
      std::shared_ptr<Callable> callable,
      const SelectivityVector& rows) {
    for (auto& otherRows : rowSets_) {
      auto begin = std::max(rows.begin(), otherRows.begin());
      auto end = std::min(rows.end(), otherRows.end());
      VELOX_CHECK(
          !bits::hasIntersection(
              otherRows.asRange().bits(), rows.asRange().bits(), begin, end),
          "Functions in a FunctionVector may not have intersecting SelectivityVectors");
    }

    rowSets_.push_back(rows);
    functions_.push_back(callable);
  }

  std::optional<int32_t> compare(
      const BaseVector* /*other*/,
      vector_size_t /*index*/,
      vector_size_t /*otherIndex*/,
      CompareFlags /*flags*/) const override {
    throw std::logic_error("compare not defined for FunctionVector");
  }

  uint64_t hashValueAt(vector_size_t /*index*/) const override {
    throw std::logic_error("not defined for FunctionVector");
  }

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override {
    throw std::logic_error("hashAll not defined for FunctionVector");
  }

  const BaseVector* loadedVector() const override {
    throw std::logic_error("loadedVector not defined for FunctionVector");
  }

  const BaseVector* wrappedVector() const override {
    return this;
  }

  vector_size_t wrappedIndex(vector_size_t index) const override {
    return index;
  }

  // Fast shortcut for determining constancy. The vector will nearly
  // always have a single function. If the non-function argument of a
  // lambda-accepting function is wrapped in a dictionary, the
  // dictionary can be peeled off if the function is constant.
  bool isConstant(const SelectivityVector& /*rows*/) const override {
    return functions_.size() == 1;
  }

  Iterator iterator(const SelectivityVector* rows) const {
    return Iterator(this, rows);
  }

 private:
  std::vector<std::shared_ptr<Callable>> functions_;
  std::vector<SelectivityVector> rowSets_;
};

} // namespace facebook::velox
