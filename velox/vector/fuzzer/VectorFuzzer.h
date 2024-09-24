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

#include <boost/random/uniform_01.hpp>
#include <random>

#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/fuzzer/GeneratorSpec.h"

namespace facebook::velox {

enum UTF8CharList {
  ASCII = 0, // Ascii character set.
  UNICODE_CASE_SENSITIVE = 1, // Unicode scripts that support case.
  EXTENDED_UNICODE = 2, // Extended Unicode: Arabic, Devanagiri etc
  MATHEMATICAL_SYMBOLS = 3 // Mathematical Symbols.
};

struct DataSpec {
  bool includeNaN;
  bool includeInfinity;
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
/// The `fuzz(type)` method provides the highest degree of entropy. It randomly
/// generates different types of (possibly nested) encodings given the input
/// type, including constants, dictionaries, sliced vectors, and more. It
/// accepts any primitive, complex, or nested types:
///
///   auto vector1 = fuzzer.fuzz(INTEGER());
///   auto vector2 =
///       fuzzer.fuzz(MAP(ARRAY(INTEGER()), ROW({REAL(), BIGINT()})));
///   auto vector3 = fuzzer.fuzz(ROW({SMALLINT(), DOUBLE()}));
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

    /// If false, fuzzer will not generate nulls for elements within containers
    /// (arrays/maps/rows). It might still generate null for the container
    /// itself.
    ///
    /// The amount of nulls (if true) is controlled by `nullRatio`.
    ///
    /// If you want to prevent the top-level row containers from being null, see
    /// `fuzzInputRow()` instead.
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
    bool containerVariableLength{true};

    /// Restricts the maximum inner (elements) vector size created when
    /// generating nested vectors (arrays, maps, and rows).
    size_t complexElementsMaxSize{10000};

    /// maximum size of array/map wrapped inside constant.
    std::optional<int32_t> maxConstantContainerSize{std::nullopt};

    /// If true, generated map keys are normalized (unique and not-null).
    bool normalizeMapKeys{true};

    /// Control the precision of timestamps generated. By default generate using
    /// nanoseconds precision.
    enum class TimestampPrecision : int8_t {
      kNanoSeconds = 0,
      kMicroSeconds = 1,
      kMilliSeconds = 2,
      kSeconds = 3,
    };
    TimestampPrecision timestampPrecision{TimestampPrecision::kNanoSeconds};

    /// If true, fuzz() will randomly generate lazy vectors and fuzzInputRow()
    /// will generate a raw vector with children that can randomly be lazy
    /// vectors. The generated lazy vectors can also have any number of
    /// dictionary layers on top of them.
    bool allowLazyVector{false};

    /// Data spec for randomly generated data.
    DataSpec dataSpec{false, false};
  };

  VectorFuzzer(
      const VectorFuzzer::Options& options,
      memory::MemoryPool* pool,
      size_t seed = 123456)
      : opts_(options), pool_(pool), rng_(seed) {}

  void setOptions(const VectorFuzzer::Options& options) {
    opts_ = options;
  }

  const VectorFuzzer::Options& getOptions() {
    return opts_;
  }

  /// Returns a "fuzzed" vector, containing randomized data, nulls, and indices
  /// vector (dictionary). Returns a vector containing `opts_.vectorSize` or
  /// `size` elements.
  VectorPtr fuzz(const TypePtr& type);
  VectorPtr fuzz(const TypePtr& type, vector_size_t size);

  /// Returns a "fuzzed" vector containing randomized data customized according
  /// to generatorSpec.
  VectorPtr fuzz(const GeneratorSpec& generatorSpec);

  /// Same as above, but returns a vector without nulls (regardless of the value
  /// of opts.nullRatio).
  VectorPtr fuzzNotNull(const TypePtr& type);
  VectorPtr fuzzNotNull(const TypePtr& type, vector_size_t size);

  /// Returns a flat vector or a complex vector with flat children with
  /// randomized data and nulls. Returns a vector containing `opts_.vectorSize`
  /// or `size` elements.
  VectorPtr fuzzFlat(const TypePtr& type);
  VectorPtr fuzzFlat(const TypePtr& type, vector_size_t size);

  /// Same as above, but returns a vector without nulls (regardless of the value
  /// of opts.nullRatio).
  VectorPtr fuzzFlatNotNull(const TypePtr& type);
  VectorPtr fuzzFlatNotNull(const TypePtr& type, vector_size_t size);

  /// Returns a random constant vector (which could be a null constant). Returns
  /// a vector with size set to `opts_.vectorSize` or 'size'.
  VectorPtr fuzzConstant(const TypePtr& type);
  VectorPtr fuzzConstant(const TypePtr& type, vector_size_t size);

  /// Wraps `vector` using a randomized indices vector, returning a
  /// DictionaryVector which has same number of indices as the underlying
  /// `vector` size.
  VectorPtr fuzzDictionary(const VectorPtr& vector);
  VectorPtr fuzzDictionary(const VectorPtr& vector, vector_size_t size);

  /// Uses `elements` as the internal elements vector, wrapping them into an
  /// ArrayVector of `size` rows.
  ///
  /// The number of elements per array row is based on the size of the
  /// `elements` vector and `size`, and either fixed or variable (depending on
  /// `opts.containerVariableLength`).
  ArrayVectorPtr fuzzArray(const VectorPtr& elements, vector_size_t size);

  /// Uses `keys` and `values` as the internal elements vectors, wrapping them
  /// into a MapVector of `size` rows.
  ///
  /// The number of elements per map row is based on the size of the `keys` and
  /// `values` vectors and `size`, and either fixed or variable (depending on
  /// `opts.containerVariableLength`).
  ///
  /// If opt.normalizeMapKeys is true, keys will be normalized - duplicated key
  /// values for a particular element will be removed/skipped. In that case,
  /// this method throws if the keys vector has nulls.
  MapVectorPtr
  fuzzMap(const VectorPtr& keys, const VectorPtr& values, vector_size_t size);

  /// Returns a "fuzzed" row vector with randomized data and nulls.
  RowVectorPtr fuzzRow(const RowTypePtr& rowType);

  /// If allowTopLevelNulls is false, the top level row wont have nulls.
  RowVectorPtr fuzzRow(
      const RowTypePtr& rowType,
      vector_size_t size,
      bool allowTopLevelNulls = true);

  /// Returns a RowVector based on the provided vectors, fuzzing its top-level
  /// null buffer.
  RowVectorPtr fuzzRow(
      std::vector<VectorPtr>&& children,
      std::vector<std::string> childrenNames,
      vector_size_t size);

  /// Returns a RowVector based on the provided vectors, fuzzing its top-level
  /// null buffer.
  RowVectorPtr fuzzRow(std::vector<VectorPtr>&& children, vector_size_t size);

  /// Same as the function above, but never return nulls for the top-level row
  /// elements.
  RowVectorPtr fuzzInputRow(const RowTypePtr& rowType);

  /// Same as the function above, but all generated vectors are flat, i.e. no
  /// constant or dictionary-encoded vectors at any level.
  RowVectorPtr fuzzInputFlatRow(const RowTypePtr& rowType);

  /// Generates a random type, including maps, vectors, and arrays. maxDepth
  /// limits the maximum level of nesting for complex types. maxDepth <= 1 means
  /// no complex types are allowed.
  ///
  /// There are no options to control type generation yet; these may be added in
  /// the future.
  TypePtr randType(int maxDepth = 5);

  TypePtr randType(const std::vector<TypePtr>& scalarTypes, int maxDepth = 5);

  /// Same as the function above, but only generate orderable types.
  /// MAP types are not generated as they are not orderable.
  TypePtr randOrderableType(int maxDepth = 5);

  TypePtr randOrderableType(
      const std::vector<TypePtr>& scalarTypes,
      int maxDepth = 5);

  RowTypePtr randRowType(int maxDepth = 5);
  RowTypePtr randRowType(
      const std::vector<TypePtr>& scalarTypes,
      int maxDepth = 5);

  /// Generates short decimal TypePtr with random precision and scale.
  inline TypePtr randShortDecimalType() {
    auto [precision, scale] =
        randPrecisionScale(ShortDecimalType::kMaxPrecision);
    return DECIMAL(precision, scale);
  }

  /// Generates long decimal TypePtr with random precision and scale.
  inline TypePtr randLongDecimalType() {
    auto [precision, scale] =
        randPrecisionScale(LongDecimalType::kMaxPrecision);
    return DECIMAL(precision, scale);
  }

  void reSeed(size_t seed) {
    rng_.seed(seed);
  }

  /// Returns true n% of times (`n` is a double between 0 and 1).
  bool coinToss(double n) {
    return boost::random::uniform_01<double>()(rng_) < n;
  }

  /// Wraps the given vector in a LazyVector. If there are multiple dictionary
  /// layers then the lazy wrap is applied over the innermost dictionary layer.
  static VectorPtr wrapInLazyVector(VectorPtr baseVector);

  /// Randomly applies wrapInLazyVector() to the children of the given input row
  /// vector. Must only be used for input row vectors where all children are
  /// non-null and non-lazy. Is useful when the input rowVector needs to be
  /// re-used between multiple evaluations.
  RowVectorPtr fuzzRowChildrenToLazy(RowVectorPtr rowVector);

  /// Returns a copy of 'rowVector' but with the columns having indices listed
  /// in 'columnsToWrapInLazy' wrapped in lazy encoding. Must only be used for
  /// input row vectors where all children are non-null and non-lazy.
  /// 'columnsToWrapInLazy' can contain negative column indices that represent
  /// lazy vectors that should be preloaded before being fed to the evaluator.
  /// This list is sorted on the absolute value of the entries.
  static RowVectorPtr fuzzRowChildrenToLazy(
      RowVectorPtr rowVector,
      const std::vector<int>& columnsToWrapInLazy);

  /// Generate a random null buffer.
  BufferPtr fuzzNulls(vector_size_t size);

  /// Generate a random indices buffer of 'size' with maximum possible index
  /// pointing to (baseVectorSize-1).
  BufferPtr fuzzIndices(vector_size_t size, vector_size_t baseVectorSize);

 private:
  // Generates a flat vector for primitive types.
  VectorPtr fuzzFlatPrimitive(const TypePtr& type, vector_size_t size);

  // Generates random precision in range [1, maxPrecision]
  // and scale in range [0, random precision generated].
  // @param maximum precision.
  std::pair<int8_t, int8_t> randPrecisionScale(int8_t maxPrecision);

  // Returns a complex vector with randomized data and nulls.  The children and
  // all other descendant vectors will randomly use constant, dictionary, or
  // flat encodings if flatEncoding is set to false, otherwise they will all be
  // flat.
  VectorPtr fuzzComplex(const TypePtr& type, vector_size_t size);

  void fuzzOffsetsAndSizes(
      BufferPtr& offsets,
      BufferPtr& sizes,
      size_t elementsSize,
      size_t size);

  // Normalize a vector to be used as map key.
  // For each map element, if duplicate key values are found, remove them (and
  // any subsequent key values) by cutting the map short (reducing its size).
  // Throws if the keys vector contains null values.
  VectorPtr normalizeMapKeys(
      const VectorPtr& keys,
      size_t mapSize,
      BufferPtr& offsets,
      BufferPtr& sizes);

  VectorFuzzer::Options opts_;

  memory::MemoryPool* pool_;

  // Be careful not to call any functions that use rng_ inline as arguments to a
  // function.  C++ does not guarantee the order in which arguments are
  // evaluated, which can lead to inconsistent results across platforms.
  FuzzerGenerator rng_;
};

/// Generates a random type, including maps, structs, and arrays. maxDepth
/// limits the maximum level of nesting for complex types. maxDepth <= 1 means
/// no complex types are allowed.
TypePtr randType(FuzzerGenerator& rng, int maxDepth = 5);

TypePtr randType(
    FuzzerGenerator& rng,
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth = 5);

/// Same as the function above, but only generate orderable types.
/// MAP types are not generated as they are not orderable.
TypePtr randOrderableType(FuzzerGenerator& rng, int maxDepth = 5);

TypePtr randOrderableType(
    FuzzerGenerator& rng,
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth = 5);

/// Generates a random ROW type.
RowTypePtr randRowType(FuzzerGenerator& rng, int maxDepth = 5);

RowTypePtr randRowType(
    FuzzerGenerator& rng,
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth = 5);

/// Default set of scalar types to be chosen from when generating random types.
const std::vector<TypePtr>& defaultScalarTypes();

} // namespace facebook::velox
