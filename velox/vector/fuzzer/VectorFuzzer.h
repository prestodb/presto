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

#include <folly/Random.h>
#include <random>

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox {

// Helper class to generate randomized vectors with random (and potentially
// nested) encodings. Use the constructor seed to make it deterministic.

using FuzzerGenerator = std::mt19937;

enum UTF8CharList {
  ASCII, /* Ascii character set.*/
  UNICODE_CASE_SENSITIVE, /* Unicode scripts that support case.*/
  EXTENDED_UNICODE, /* Extended Unicode: Arabic, Devanagiri etc*/
  MATHEMATICAL_SYMBOLS /* Mathematical Symbols.*/
};

class VectorFuzzer {
 public:
  struct Options {
    size_t vectorSize{100};

    /// Chance of generating a null value in the output vector (`nullRatio` is a
    /// double between 0 and 1).
    double nullRatio{0};

    /// If true, fuzzer will generate top-level nulls for containers
    /// (arrays/maps/rows), i.e, nulls for the containers themselves, not the
    /// elements.
    ///
    /// The amount of nulls are controlled by `nullRatio`.
    bool containerHasNulls{true};

    /// If true, fuzzer will generate top-level nulls for dictionaries.
    bool dictionaryHasNulls{true};

    /// Size of the generated strings. If `stringVariableLength` is true, the
    /// semantic of this option becomes "string maximum length". Here this
    /// represents number of characters and not bytes.
    size_t stringLength{50};

    /// Vector of String charsets to choose from; bias a charset by including it
    /// multiple times.
    std::vector<UTF8CharList> charEncodings{ASCII};

    /// If true, the length of strings are randomly generated and `stringLength`
    /// is treated as maximum length.
    bool stringVariableLength{false};

    /// Size of the generated array/map. If `containerVariableLength` is true,
    /// the semantic of this option becomes "container maximum length".
    size_t containerLength{10};

    /// If true, the length of array/map are randomly generated and
    /// `containerLength` is treated as maximum length.
    bool containerVariableLength{false};

    /// If true, the random generated timestamp value will only be in
    /// microsecond precision (default is nanosecond).
    bool useMicrosecondPrecisionTimestamp{false};
  };

  VectorFuzzer(
      VectorFuzzer::Options options,
      memory::MemoryPool* pool,
      size_t seed = 123456)
      : opts_(options), pool_(pool), rng_(seed) {}

  void setOptions(VectorFuzzer::Options options) {
    opts_ = options;
  }

  // Returns a "fuzzed" vector, containing randomized data, nulls, and indices
  // vector (dictionary).
  VectorPtr fuzz(const TypePtr& type);

  // Returns a flat vector with randomized data and nulls.
  VectorPtr fuzzFlat(const TypePtr& type);

  // Returns a random constant vector (which could be a null constant).
  VectorPtr fuzzConstant(const TypePtr& type);

  // Wraps `vector` using a randomized indices vector, returning a
  // DictionaryVector.
  VectorPtr fuzzDictionary(const VectorPtr& vector);

  // Returns a complex vector with randomized data and nulls.
  VectorPtr fuzzComplex(const TypePtr& type);

  // Returns a "fuzzed" row vector with randomized data and nulls.
  RowVectorPtr fuzzRow(const RowTypePtr& rowType);

  // Same as the function above, but never return nulls for the top-level row
  // elements.
  RowVectorPtr fuzzInputRow(const RowTypePtr& rowType) {
    return fuzzRow(rowType, opts_.vectorSize, false);
  }

  variant randVariant(const TypePtr& arg);

  // Generates a random type, including maps, vectors, and arrays. maxDepth
  // limits the maximum level of nesting for complex types. maxDepth <= 1 means
  // no complex types are allowed.
  //
  // There are no options to control type generation yet; these may be added in
  // the future.
  TypePtr randType(int maxDepth = 5);
  RowTypePtr randRowType(int maxDepth = 5);

  void reSeed(size_t seed) {
    rng_.seed(seed);
  }

  /// Returns true n% of times (`n` is a double between 0 and 1).
  bool coinToss(double n) {
    return folly::Random::randDouble01(rng_) < n;
  }

 private:
  VectorPtr fuzz(const TypePtr& type, vector_size_t size);

  VectorPtr fuzzFlat(const TypePtr& type, vector_size_t size);

  VectorPtr fuzzConstant(const TypePtr& type, vector_size_t size);

  VectorPtr fuzzComplex(const TypePtr& type, vector_size_t size);

  RowVectorPtr
  fuzzRow(const RowTypePtr& rowType, vector_size_t size, bool rowHasNulls);

  // Generate a random null vector.
  BufferPtr fuzzNulls(vector_size_t size);

  VectorFuzzer::Options opts_;

  memory::MemoryPool* pool_;

  FuzzerGenerator rng_;
};

} // namespace facebook::velox
