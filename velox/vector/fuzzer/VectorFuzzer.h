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

using FuzzerGenerator = std::mt19937;

enum UTF8CharList {
  ASCII = 0, // Ascii character set.
  UNICODE_CASE_SENSITIVE = 1, // Unicode scripts that support case.
  EXTENDED_UNICODE = 2, // Extended Unicode: Arabic, Devanagiri etc
  MATHEMATICAL_SYMBOLS = 3 // Mathematical Symbols.
};

/// VectorFuzzer is a helper class that generates randomized vectors and their
/// data for testing, with a high degree of entropy.
///
/// `Options` can be used to customize its behavior, and a seed passed to the
/// constructor can make it deterministic.
///
/// There a three main ways in which VectorFuzzer can be used:
///
/// #1.
///
/// The `fuzz(type, canBeLazy)` method provides the highest degree of entropy.
/// It randomly generates different types of (possibly nested) encodings given
/// the input type, including constants, dictionaries, sliced vectors, and more.
/// It accepts any primitive, complex, or nested types. Additionally,
/// 'canBeLazy' can be set to true to enable a chance of generating a lazy
/// vector:
///
///   auto vector1 = fuzzer.fuzz(INTEGER(), true);
///   auto vector2 =
///       fuzzer.fuzz(MAP(ARRAY(INTEGER()), ROW({REAL(), BIGINT()})), false);
///   auto vector3 = fuzzer.fuzz(ROW({SMALLINT(), DOUBLE()}), true);
///
/// #2.
///
/// The `fuzzFlat(type)` method provides a similar API, except that generated
/// vectors are guaranteed to be flat. It accepts complex types like arrays,
/// maps, and rows. Complex types are guaranteed to have only flat children:
///
///   auto vector1 = fuzzer.fuzzFlat(TINYINT());
///   auto vector2 = fuzzer.fuzzFlat(
///                     MAP(ARRAY(INTEGER()), ROW({REAL(), BIGINT()}));
///
/// #3.
///
/// For more control over the generated encodings, VectorFuzzer also provides
/// composable APIs to let users specify the generated encodings. For instance,
/// to generate a map where the key is a dictionary wrapped around a flat
/// vector, and the value an array with an internal constant vector, you can
/// use:
///
///   auto vector = fuzzer.fuzzMap(
///      fuzzer.fuzzDictionary(
///          fuzzer.fuzzFlat(INTEGER(), 10), 100),
///      fuzzer.fuzzArray(
///          fuzzer.fuzzConstant(DOUBLE(), 40), 100),
///      10);
///
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

    /// If true, fuzz() will randomly generate lazy vectors and fuzzInputRow()
    /// will generate a raw vector with children that can randomly be lazy
    /// vectors. The generated lazy vectors can also have any number of
    /// dictionary layers on top of them.
    bool allowLazyVector{false};
  };

  VectorFuzzer(
      VectorFuzzer::Options options,
      memory::MemoryPool* pool,
      size_t seed = 123456)
      : opts_(options), pool_(pool), rng_(seed) {}

  void setOptions(VectorFuzzer::Options options) {
    opts_ = options;
  }

  const VectorFuzzer::Options& getOptions() {
    return opts_;
  }

  // Returns a "fuzzed" vector, containing randomized data, nulls, and indices
  // vector (dictionary). Returns a vector of `opts_.vectorSize` size.
  VectorPtr fuzz(const TypePtr& type);

  // Returns a flat vector or a complex vector with flat children with
  // randomized data and nulls. Returns a vector of `opts_.vectorSize` size.
  VectorPtr fuzzFlat(const TypePtr& type);

  // Same as above, but returns a vector of `size` size.
  VectorPtr fuzzFlat(const TypePtr& type, vector_size_t size);

  // Returns a random constant vector (which could be a null constant). Returns
  // a vector with size set to `opts_.vectorSize`.
  VectorPtr fuzzConstant(const TypePtr& type);

  // Same as above, but returns a vector of `size` size.
  VectorPtr fuzzConstant(const TypePtr& type, vector_size_t size);

  // Wraps `vector` using a randomized indices vector, returning a
  // DictionaryVector which has same number of indices as the underlying
  // `vector` size.
  VectorPtr fuzzDictionary(const VectorPtr& vector);

  // Same as above, but returns a dictionary vector of `size` size.
  VectorPtr fuzzDictionary(const VectorPtr& vector, vector_size_t size);

  // Uses `elements` as the internal elements vector, wrapping them into an
  // ArrayVector of `size` rows.
  //
  // The number of elements per array row is based on the size of the
  // `elements` vector and `size`, and either fixed or variable (depending on
  // `opts.containerVariableLength`).
  ArrayVectorPtr fuzzArray(const VectorPtr& elements, vector_size_t size);

  // Uses `keys` and `values` as the internal elements vectors, wrapping them
  // into a MapVector of `size` rows.
  //
  // The number of elements per map row is based on the size of the `keys` and
  // `values` vectors and `size`, and either fixed or variable (depending on
  // `opts.containerVariableLength`).
  MapVectorPtr
  fuzzMap(const VectorPtr& keys, const VectorPtr& values, vector_size_t size);

  // Returns a "fuzzed" row vector with randomized data and nulls.
  RowVectorPtr fuzzRow(const RowTypePtr& rowType);

  // Returns a RowVector based on the provided vectors, fuzzing its top-level
  // null buffer.
  RowVectorPtr fuzzRow(std::vector<VectorPtr>&& children, vector_size_t size);

  // Same as the function above, but never return nulls for the top-level row
  // elements.
  RowVectorPtr fuzzInputRow(const RowTypePtr& rowType) {
    return fuzzRow(rowType, opts_.vectorSize, false, opts_.allowLazyVector);
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

  // Wraps the given vector in a LazyVector.
  static VectorPtr wrapInLazyVector(VectorPtr baseVector);

  // Randomly applies wrapInLazyVector() to the children of the given input row
  // vector. Must only be used for input row vectors where all children are
  // non-null and non-lazy. Is useful when the input rowVector needs to be
  // re-used between multiple evaluations.
  RowVectorPtr fuzzRowChildrenToLazy(RowVectorPtr rowVector);

 private:
  // Same as above, but returns a vector of `size` size. Additionally, If
  // 'canBeLazy' is true then the returned vector can be a lazy vector.
  VectorPtr fuzz(const TypePtr& type, vector_size_t size, bool canBeLazy);

  // Generates a flat vector for primitive types.
  VectorPtr fuzzFlatPrimitive(const TypePtr& type, vector_size_t size);

  // Returns a complex vector with randomized data and nulls.  The children and
  // all other descendant vectors will randomly use constant, dictionary, or
  // flat encodings if flatEncoding is set to false, otherwise they will all be
  // flat.
  VectorPtr fuzzComplex(const TypePtr& type, vector_size_t size);

  // If 'canChildrenBeLazy' is set to true then the returned vector can have
  // lazy children.
  RowVectorPtr fuzzRow(
      const RowTypePtr& rowType,
      vector_size_t size,
      bool rowHasNulls,
      bool canChildrenBeLazy);

  // Generate a random null buffer.
  BufferPtr fuzzNulls(vector_size_t size);

  void fuzzOffsetsAndSizes(
      BufferPtr& offsets,
      BufferPtr& sizes,
      size_t elementsSize,
      size_t size);

  VectorFuzzer::Options opts_;

  memory::MemoryPool* pool_;

  FuzzerGenerator rng_;
};

} // namespace facebook::velox
