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

#include <memory>

#include "folly/json.h"

#include "velox/common/fuzzer/Utils.h"
#include "velox/type/Type.h"
#include "velox/type/Variant.h"

namespace facebook::velox::fuzzer {

using facebook::velox::variant;

/// Get a random input generator for the given type.
/// @param seed The seed used by the returned input generator.
/// @param type The type of the input generator.
/// @param nullRatio The ratio of null values to generate by the returned input
/// generator.
/// @param mapKeys The candidate map keys used when generating data for all maps
/// nested in 'type'. If empty, random keys are used.
/// @param maxContainerSize The maximum size of all containers nested in 'type',
/// including arrays and maps.
std::unique_ptr<AbstractInputGenerator> getRandomInputGenerator(
    size_t seed,
    const TypePtr& type,
    double nullRatio,
    const std::vector<variant>& mapKeys = {},
    size_t maxContainerSize = 10);

template <typename T, typename Enabled = void>
class RandomInputGenerator : public AbstractInputGenerator {
 public:
  RandomInputGenerator(size_t seed, const TypePtr& type, double nullRatio)
      : AbstractInputGenerator(seed, type, nullptr, nullRatio) {}

  ~RandomInputGenerator() override = default;

  variant generate() override {
    if (coinToss(rng_, nullRatio_)) {
      return variant::null(type_->kind());
    }

    if (type_->isDate()) {
      return variant(randDate(rng_));
    }
    return variant(rand<T>(rng_));
  }
};

struct RandomStrVariationOptions {
  double controlCharacterProbability = 0.0;
  double escapeStringProbability = 0.0;
  double truncateProbability = 0.0;
};

void makeRandomStrVariation(
    std::string& input,
    FuzzerGenerator& rng,
    const RandomStrVariationOptions& randomStrVariationOptions);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
template <typename T>
class RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, StringView>>>
    : public AbstractInputGenerator {
 public:
  RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, StringView>>>(
      size_t seed,
      const TypePtr& type,
      double nullRatio,
      size_t maxLength = 20,
      const std::vector<UTF8CharList>& encodings =
          {UTF8CharList::ASCII,
           UTF8CharList::UNICODE_CASE_SENSITIVE,
           UTF8CharList::EXTENDED_UNICODE,
           UTF8CharList::MATHEMATICAL_SYMBOLS},
      RandomStrVariationOptions randomStrVariationOptions = {})
      : AbstractInputGenerator(seed, type, nullptr, nullRatio),
        maxLength_{maxLength},
        encodings_{encodings},
        randomStrVariationOptions_{randomStrVariationOptions} {}

  ~RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, StringView>>>()
      override = default;

  variant generate() override {
    if (coinToss(rng_, nullRatio_)) {
      return variant::null(type_->kind());
    }

    const auto length = rand<size_t>(rng_, 0, maxLength_);
    std::wstring_convert<std::codecvt_utf8<char16_t>, char16_t> converter;
    std::string buf;
    auto randomStr = randString(rng_, length, encodings_, buf, converter);
    makeRandomStrVariation(randomStr, rng_, randomStrVariationOptions_);
    return variant(std::move(randomStr));
  }

 private:
  const size_t maxLength_;

  std::vector<UTF8CharList> encodings_;
  RandomStrVariationOptions randomStrVariationOptions_;
};
#pragma GCC diagnostic pop

template <typename T>
class RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, ArrayType>>>
    : public AbstractInputGenerator {
 public:
  RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, ArrayType>>>(
      size_t seed,
      const TypePtr& type,
      double nullRatio,
      size_t maxLength = 10,
      std::unique_ptr<AbstractInputGenerator>&& elementGenerator = nullptr,
      std::optional<size_t> containAtIndex = std::nullopt,
      std::unique_ptr<AbstractInputGenerator>&& containGenerator = nullptr)
      : AbstractInputGenerator(seed, type, nullptr, nullRatio),
        maxLength_{maxLength},
        elementGenerator_{
            elementGenerator
                ? std::move(elementGenerator)
                : getRandomInputGenerator(seed, type->childAt(0), nullRatio)},
        containAtIndex_{containAtIndex},
        containGenerator_{std::move(containGenerator)} {}

  ~RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, ArrayType>>>()
      override = default;

  variant generate() override {
    if (coinToss(rng_, nullRatio_)) {
      return variant::null(TypeKind::ARRAY);
    }

    const auto length = containAtIndex_.has_value()
        ? rand<size_t>(rng_, containAtIndex_.value() + 1, maxLength_)
        : rand<size_t>(rng_, 0, maxLength_);
    std::vector<variant> elements;
    elements.reserve(length);
    for (size_t i = 0; i < length; ++i) {
      if UNLIKELY (containAtIndex_.has_value() && *containAtIndex_ == i) {
        elements.push_back(containGenerator_->generate());
      } else {
        elements.push_back(elementGenerator_->generate());
      }
    }
    return variant::array(elements);
  }

 private:
  const size_t maxLength_;

  std::unique_ptr<AbstractInputGenerator> elementGenerator_;

  std::optional<size_t> containAtIndex_;

  std::unique_ptr<AbstractInputGenerator> containGenerator_;
};

template <typename T>
class RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, MapType>>>
    : public AbstractInputGenerator {
 public:
  RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, MapType>>>(
      size_t seed,
      const TypePtr& type,
      double nullRatio,
      size_t maxLength = 10,
      std::unique_ptr<AbstractInputGenerator>&& keyGenerator = nullptr,
      std::unique_ptr<AbstractInputGenerator>&& valueGenerator = nullptr,
      std::unique_ptr<AbstractInputGenerator>&& containKeyGenerator = nullptr,
      std::unique_ptr<AbstractInputGenerator>&& containValueGenerator = nullptr)
      : AbstractInputGenerator(seed, type, nullptr, nullRatio),
        maxLength_{maxLength},
        keyGenerator_{
            keyGenerator
                ? std::move(keyGenerator)
                : getRandomInputGenerator(seed, type->childAt(0), 0.0)},
        valueGenerator_{
            valueGenerator
                ? std::move(valueGenerator)
                : getRandomInputGenerator(seed, type->childAt(1), nullRatio)},
        containKeyGenerator_{std::move(containKeyGenerator)},
        containValueGenerator_{std::move(containValueGenerator)} {
    if (containKeyGenerator_ || containValueGenerator_) {
      VELOX_CHECK_NOT_NULL(containKeyGenerator_);
      VELOX_CHECK_NOT_NULL(containValueGenerator_);
    }
  }

  ~RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, MapType>>>()
      override = default;

  variant generate() override {
    if (coinToss(rng_, nullRatio_)) {
      return variant::null(TypeKind::MAP);
    }

    const auto length = rand<size_t>(rng_, 0, maxLength_);
    int64_t containAtIndex = (length > 0 && containKeyGenerator_ != nullptr)
        ? rand<size_t>(rng_, 0, length - 1)
        : -1;
    std::map<variant, variant> map;
    for (int64_t i = 0; i < length; ++i) {
      if UNLIKELY (i == containAtIndex) {
        map.emplace(
            containKeyGenerator_->generate(),
            containValueGenerator_->generate());
      } else {
        map.emplace(keyGenerator_->generate(), valueGenerator_->generate());
      }
    }
    return variant::map(map);
  }

 private:
  const size_t maxLength_;

  std::unique_ptr<AbstractInputGenerator> keyGenerator_;

  std::unique_ptr<AbstractInputGenerator> valueGenerator_;

  std::unique_ptr<AbstractInputGenerator> containKeyGenerator_;

  std::unique_ptr<AbstractInputGenerator> containValueGenerator_;
};

template <typename T>
class RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, RowType>>>
    : public AbstractInputGenerator {
 public:
  RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, RowType>>>(
      size_t seed,
      const TypePtr& type,
      std::vector<std::unique_ptr<AbstractInputGenerator>> fieldGenerators,
      double nullRatio)
      : AbstractInputGenerator(seed, type, nullptr, nullRatio) {
    const auto length = type->size();
    fieldGenerators_ = std::move(fieldGenerators);
    for (size_t i = 0; i < length; ++i) {
      if (fieldGenerators_.size() <= i) {
        fieldGenerators_.push_back(
            getRandomInputGenerator(seed, type->childAt(i), nullRatio));
      } else if (fieldGenerators_[i] == nullptr) {
        fieldGenerators_[i] =
            getRandomInputGenerator(seed, type->childAt(i), nullRatio);
      }
    }
  }

  ~RandomInputGenerator<T, std::enable_if_t<std::is_same_v<T, RowType>>>()
      override = default;

  variant generate() override {
    if (coinToss(rng_, nullRatio_)) {
      return variant::null(TypeKind::ROW);
    }

    const auto length = type_->size();
    std::vector<variant> fields;
    fields.reserve(length);
    for (size_t i = 0; i < length; ++i) {
      fields.push_back(fieldGenerators_[i]->generate());
    }
    return variant::row(fields);
  }

 private:
  std::vector<std::unique_ptr<AbstractInputGenerator>> fieldGenerators_;
};

template <typename T, std::enable_if_t<std::is_arithmetic_v<T>, int> = 0>
class RangeConstrainedGenerator : public AbstractInputGenerator {
 public:
  RangeConstrainedGenerator(
      size_t seed,
      const TypePtr& type,
      double nullRatio,
      T min,
      T max)
      : AbstractInputGenerator(seed, type, nullptr, nullRatio),
        min_{min},
        max_{max} {}

  ~RangeConstrainedGenerator() override = default;

  variant generate() override {
    if (coinToss(rng_, nullRatio_)) {
      return variant::null(type_->kind());
    }
    return variant(rand<T>(rng_, min_, max_));
  }

 private:
  T min_;
  T max_;
};

class NotEqualConstrainedGenerator : public AbstractInputGenerator {
 public:
  // nullRatio doesn't affect the data generation because it is 'next' that
  // generates data.
  NotEqualConstrainedGenerator(
      size_t seed,
      const TypePtr& type,
      const variant& excludedValue,
      std::unique_ptr<AbstractInputGenerator>&& next)
      : AbstractInputGenerator(seed, type, std::move(next), 0.0),
        excludedValue_{excludedValue} {}

  ~NotEqualConstrainedGenerator() override = default;

  variant generate() override;

 private:
  variant excludedValue_;
};

class SetConstrainedGenerator : public AbstractInputGenerator {
 public:
  // nullRatio doesn't affect the data generation because only variants in 'set'
  // can be generated.
  SetConstrainedGenerator(
      size_t seed,
      const TypePtr& type,
      const std::vector<variant>& set)
      : AbstractInputGenerator(seed, type, nullptr, 0.0), set_{set} {}

  ~SetConstrainedGenerator() override = default;

  variant generate() override;

 private:
  std::vector<variant> set_;
};

// Generates random JSON strings. This generator first generates a value that
// the expected JSON represents through 'objectGenerator', then converts the
// value to a JSON string via folly::json::serialize() with randomly generated
// serialization options. Additionally, it makes a random variation to the JSON
// string by chance for testing of invalid cases.
class JsonInputGenerator : public AbstractInputGenerator {
 public:
  JsonInputGenerator(
      size_t seed,
      const TypePtr& type,
      double nullRatio,
      std::unique_ptr<AbstractInputGenerator>&& objectGenerator,
      bool makeRandomVariation = false);

  ~JsonInputGenerator() override;

  variant generate() override;

  const folly::json::serialization_opts& serializationOptions() const {
    return opts_;
  }

 private:
  template <TypeKind KIND>
  folly::dynamic convertVariantToDynamicPrimitive(const variant& v) {
    using T = typename TypeTraits<KIND>::DeepCopiedType;
    VELOX_CHECK(v.isSet());
    const T value = v.value<T>();
    return folly::dynamic(value);
  }

  folly::dynamic convertVariantToDynamic(const variant& object);

  void makeRandomVariation(std::string& json);

  std::unique_ptr<AbstractInputGenerator> objectGenerator_;

  bool makeRandomVariation_;

  folly::json::serialization_opts opts_;
};

class PhoneNumberInputGenerator : public AbstractInputGenerator {
 public:
  PhoneNumberInputGenerator(size_t seed, const TypePtr& type, double nullRatio);

  ~PhoneNumberInputGenerator() override;

  variant generate() override;

 private:
  std::string generateImpl();
};

/// Generates a JSON path for JSON of a given type.
/// @param jsonType The type of data represented by the JSON.
/// @param mapKeys Candidate key values of maps in the JSON. All maps in the
/// JSON are assumed to share the same key type and candidate key values.
/// @param maxContainerLength The maximum length of a container (array or map)
/// in the JSON.
/// @param makeRandomVariation If true, the generator will generate JSON path
/// not supported by JsonExtractor.
/// This generator generates the following JSON paths:
///  - On root: $
///  - On arrays: [index], [], [*], .*
///  - On objects: [key], ['key'], ["key"], [], [*], .*, .key, ."key"
/// TODO: support the following JSON paths:
///  - Recusive gathering by key or array index: ..key, ..[1]
/// TODO: support the following JSON paths after Velox JsonExtractor supports
/// them:
///  - On strings, arrays, and objects: .length()
///  - On arrays: [begin:end:step]

class JsonPathGenerator : public AbstractInputGenerator {
 public:
  JsonPathGenerator(
      size_t seed,
      const TypePtr& type,
      double nullRatio,
      const TypePtr& jsonType,
      const std::vector<variant>& mapKeys,
      size_t maxContainerLength,
      bool makeRandomVariation = false)
      : AbstractInputGenerator(seed, type, nullptr, nullRatio),
        jsonType_{jsonType},
        mapKeys_{mapKeys},
        maxContainerLength_{maxContainerLength},
        makeRandomVariation_{makeRandomVariation} {}

  ~JsonPathGenerator() override = default;

  variant generate() override;

 private:
  uint64_t generateRandomIndex();

  void generateRecursiveAccess(std::string& path, const TypePtr& type);

  void generateImpl(std::string& path, const TypePtr& type);

  TypePtr jsonType_;

  std::vector<variant> mapKeys_;

  size_t maxContainerLength_;

  bool makeRandomVariation_;
};

} // namespace facebook::velox::fuzzer
