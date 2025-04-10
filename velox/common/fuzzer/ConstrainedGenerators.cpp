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

#include "velox/common/fuzzer/ConstrainedGenerators.h"

#include <boost/random/uniform_int_distribution.hpp>

#include "velox/common/fuzzer/Utils.h"

namespace facebook::velox::fuzzer {

// NotEqualConstrainedGenerator
variant NotEqualConstrainedGenerator::generate() {
  variant value;
  do {
    value = next_->generate();
  } while (value == excludedValue_);
  return value;
}

// SetConstrainedGenerator
variant SetConstrainedGenerator::generate() {
  const auto index =
      boost::random::uniform_int_distribution<size_t>(0, set_.size() - 1)(rng_);
  return set_[index];
}

// JsonInputGenerator
JsonInputGenerator::~JsonInputGenerator() = default;

folly::json::serialization_opts getSerializationOptions(
    FuzzerGenerator& rng,
    bool makeRandomVariation) {
  folly::json::serialization_opts opts;
  opts.allow_non_string_keys = true;
  opts.allow_nan_inf = true;
  if (makeRandomVariation) {
    opts.convert_int_keys = rand<bool>(rng);
    opts.pretty_formatting = rand<bool>(rng);
    opts.pretty_formatting_indent_width = rand<uint32_t>(rng, 0, 4);
    opts.encode_non_ascii = rand<bool>(rng);
    opts.sort_keys = rand<bool>(rng);
    opts.skip_invalid_utf8 = rand<bool>(rng);
  }
  return opts;
}

JsonInputGenerator::JsonInputGenerator(
    size_t seed,
    const TypePtr& type,
    double nullRatio,
    std::unique_ptr<AbstractInputGenerator>&& objectGenerator,
    bool makeRandomVariation)
    : AbstractInputGenerator(seed, type, nullptr, nullRatio),
      objectGenerator_{std::move(objectGenerator)},
      makeRandomVariation_{makeRandomVariation} {
  opts_ = getSerializationOptions(rng_, makeRandomVariation_);
}

variant JsonInputGenerator::generate() {
  if (coinToss(rng_, nullRatio_)) {
    return variant::null(type_->kind());
  }

  const auto object = objectGenerator_->generate();
  const folly::dynamic jsonObject = convertVariantToDynamic(object);
  auto jsonString = folly::json::serialize(jsonObject, opts_);
  if (makeRandomVariation_) {
    makeRandomVariation(jsonString);
  }
  return variant(jsonString);
}

folly::dynamic JsonInputGenerator::convertVariantToDynamic(
    const variant& object) {
  if (object.isNull()) {
    return folly::dynamic();
  }

  switch (object.kind()) {
    case TypeKind::BOOLEAN:
      return convertVariantToDynamicPrimitive<TypeKind::BOOLEAN>(object);
    case TypeKind::TINYINT:
      return convertVariantToDynamicPrimitive<TypeKind::TINYINT>(object);
    case TypeKind::SMALLINT:
      return convertVariantToDynamicPrimitive<TypeKind::SMALLINT>(object);
    case TypeKind::INTEGER:
      return convertVariantToDynamicPrimitive<TypeKind::INTEGER>(object);
    case TypeKind::BIGINT:
      return convertVariantToDynamicPrimitive<TypeKind::BIGINT>(object);
    case TypeKind::REAL:
      return convertVariantToDynamicPrimitive<TypeKind::REAL>(object);
    case TypeKind::DOUBLE:
      return convertVariantToDynamicPrimitive<TypeKind::DOUBLE>(object);
    case TypeKind::VARCHAR:
      return convertVariantToDynamicPrimitive<TypeKind::VARCHAR>(object);
    case TypeKind::VARBINARY:
      return convertVariantToDynamicPrimitive<TypeKind::VARBINARY>(object);
    case TypeKind::TIMESTAMP:
      return convertVariantToDynamicPrimitive<TypeKind::TIMESTAMP>(object);
    case TypeKind::HUGEINT:
      return convertVariantToDynamicPrimitive<TypeKind::HUGEINT>(object);
    case TypeKind::ARRAY: {
      folly::dynamic array = folly::dynamic::array;
      for (const auto& element : object.value<TypeKind::ARRAY>()) {
        array.push_back(convertVariantToDynamic(element));
      }
      return array;
    }
    case TypeKind::MAP: {
      folly::dynamic map = folly::dynamic::object;
      for (const auto& [key, value] : object.value<TypeKind::MAP>()) {
        map[convertVariantToDynamic(key)] = convertVariantToDynamic(value);
      }
      return map;
    }
    case TypeKind::ROW: {
      folly::dynamic array = folly::dynamic::array;
      for (const auto& element : object.value<TypeKind::ROW>()) {
        array.push_back(convertVariantToDynamic(element));
      }
      return array;
    }
    default:
      VELOX_UNREACHABLE("Unsupported type");
  }
}

const std::vector<std::string>& getControlCharacters() {
  static const std::vector<std::string> controlCharacters = {
      "\x00",   "\x01",   "\x02",   "\x03",   "\x04",   "\x05",   "\x06",
      "\x07",   "\x08",   "\x09",   "\x0A",   "\x0B",   "\x0C",   "\x0D",
      "\x0E",   "\x0F",   "\x10",   "\x11",   "\x12",   "\x13",   "\x14",
      "\x15",   "\x16",   "\x17",   "\x18",   "\x19",   "\x1A",   "\x1B",
      "\x1C",   "\x1D",   "\x1E",   "\x1F",   "\x20",   "\x7F",   "\u0080",
      "\u0081", "\u0082", "\u0083", "\u0084", "\u0085", "\u0086", "\u0087",
      "\u0088", "\u0089", "\u008A", "\u008B", "\u008C", "\u008D", "\u008E",
      "\u008F", "\u0090", "\u0091", "\u0092", "\u0093", "\u0094", "\u0095",
      "\u0096", "\u0097", "\u0098", "\u0099", "\u009A", "\u009B", "\u009C",
      "\u009D", "\u009E", "\u009F"};
  return controlCharacters;
};

namespace {
void insertRandomControlCharacter(std::string& input, FuzzerGenerator& rng) {
  const auto& controlCharacters = getControlCharacters();
  const auto index = rand<uint32_t>(rng, 0, controlCharacters.size() - 1);
  const auto& controlCharacter = controlCharacters[index];
  const auto indexToInsert = rand<uint32_t>(rng, 0, input.size());
  input.insert(indexToInsert, controlCharacter);
}
} // namespace

void makeRandomStrVariation(
    std::string& input,
    FuzzerGenerator& rng,
    const RandomStrVariationOptions& randOpts) {
  if (!input.empty() && coinToss(rng, randOpts.truncateProbability)) {
    // In string truncation, there's a 50% chance to truncate from the
    // beginning and 50% from the end.
    if (coinToss(rng, 0.5)) {
      const auto size = rand<uint32_t>(rng, 0, input.size());
      input.resize(size);
    } else {
      const auto start = rand<uint32_t>(rng, 0, input.size() - 1);
      input = input.substr(start);
    }
  }
  if (coinToss(rng, randOpts.controlCharacterProbability / 2)) {
    insertRandomControlCharacter(input, rng);
  }
  if (coinToss(rng, randOpts.escapeStringProbability)) {
    input = folly::cEscape<std::string>(input);
  }
  // This helps test the system's ability to handle unexpected control
  // characters within escaped strings.
  if (coinToss(rng, randOpts.controlCharacterProbability / 2)) {
    insertRandomControlCharacter(input, rng);
  }
}

void JsonInputGenerator::makeRandomVariation(std::string& json) {
  return makeRandomStrVariation(
      json, rng_, RandomStrVariationOptions{0.1, 0.0, 0.1});
}

PhoneNumberInputGenerator::PhoneNumberInputGenerator(
    size_t seed,
    const TypePtr& type,
    double nullRatio)
    : AbstractInputGenerator(seed, type, nullptr, nullRatio) {}

PhoneNumberInputGenerator::~PhoneNumberInputGenerator() = default;

variant PhoneNumberInputGenerator::generate() {
  if (coinToss(rng_, nullRatio_)) {
    return variant::null(type_->kind());
  }
  std::string phone = generateImpl();
  return variant(phone);
}

std::string PhoneNumberInputGenerator::generateImpl() {
  std::string phoneNumber = "";
  if (coinToss(rng_, 0.5)) {
    phoneNumber += "+";
  }
  uint32_t numDigits = 0;
  // Generate valid number of digits
  if (coinToss(rng_, 0.8)) {
    numDigits = rand<uint32_t>(rng_, 4, 19);
  } else if (coinToss(rng_, 0.5)) {
    numDigits = rand<uint32_t>(rng_, 0, 3);
  } else {
    numDigits = rand<uint32_t>(rng_, 20, 25);
  }
  phoneNumber.reserve(numDigits);
  const std::string digitsString = "0123456789";
  const std::string randomCharacters = "abc!@#$. -()";
  for (int i = 0; i < numDigits; i++) {
    // Add random characters
    if (coinToss(rng_, 0.1)) {
      auto random_character_index =
          rand<uint32_t>(rng_, 0, randomCharacters.length() - 1);
      phoneNumber += randomCharacters[random_character_index];
    }
    auto random_index = rand<uint32_t>(rng_, 0, digitsString.length() - 1);
    phoneNumber += digitsString[random_index];
  }
  // Add more randomness
  makeRandomStrVariation(
      phoneNumber, rng_, RandomStrVariationOptions{0.1, 0.1, 0.1});
  return phoneNumber;
}

// Utility functions
template <bool, TypeKind KIND>
std::unique_ptr<AbstractInputGenerator> getRandomInputGeneratorPrimitive(
    size_t seed,
    const TypePtr& type,
    double nullRatio) {
  using T = typename TypeTraits<KIND>::NativeType;
  std::unique_ptr<AbstractInputGenerator> generator =
      std::make_unique<RandomInputGenerator<T>>(seed, type, nullRatio);
  return generator;
}

std::unique_ptr<AbstractInputGenerator> getRandomInputGenerator(
    size_t seed,
    const TypePtr& type,
    double nullRatio,
    const std::vector<variant>& mapKeys,
    size_t maxContainerSize) {
  std::unique_ptr<AbstractInputGenerator> generator;
  if (type->isPrimitiveType()) {
    return VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(
        getRandomInputGeneratorPrimitive,
        false,
        type->kind(),
        seed,
        type,
        nullRatio);
  } else if (type->isArray()) {
    generator = std::make_unique<RandomInputGenerator<ArrayType>>(
        seed,
        type,
        nullRatio,
        maxContainerSize,
        getRandomInputGenerator(
            seed, type->childAt(0), nullRatio, mapKeys, maxContainerSize));
  } else if (type->isMap()) {
    generator = std::make_unique<RandomInputGenerator<MapType>>(
        seed,
        type,
        nullRatio,
        maxContainerSize,
        mapKeys.empty() ? nullptr
                        : std::make_unique<SetConstrainedGenerator>(
                              seed, type->childAt(0), mapKeys),
        mapKeys.empty() ? nullptr
                        : getRandomInputGenerator(
                              seed,
                              type->childAt(1),
                              nullRatio,
                              mapKeys,
                              maxContainerSize));
  } else if (type->isRow()) {
    std::vector<std::unique_ptr<AbstractInputGenerator>> children;
    for (auto i = 0; i < type->size(); ++i) {
      children.push_back(getRandomInputGenerator(
          seed, type->childAt(i), nullRatio, mapKeys, maxContainerSize));
    }
    generator = std::make_unique<RandomInputGenerator<RowType>>(
        seed, type, std::move(children), nullRatio);
  }
  return generator;
}

// JsonPathGenerator
variant JsonPathGenerator::generate() {
  if (coinToss(rng_, nullRatio_)) {
    return variant::null(type_->kind());
  }

  std::string path = "$";
  generateImpl(path, jsonType_);
  if (makeRandomVariation_) {
    makeRandomStrVariation(
        path, rng_, RandomStrVariationOptions{0.1, 0.0, 0.1});
  }
  return variant(path);
}

uint64_t JsonPathGenerator::generateRandomIndex() {
  // 10% of times generate invalid index.
  if (coinToss(rng_, 0.1)) {
    return rand<uint64_t>(rng_);
  }
  return rand<uint64_t>(rng_, 0, maxContainerLength_);
}

void JsonPathGenerator::generateImpl(std::string& path, const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
    case TypeKind::TIMESTAMP:
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return;
    case TypeKind::ARRAY:
      if (coinToss(rng_, 0.2)) {
        path += fmt::format(".{}", generateRandomIndex());
      } else if (coinToss(rng_, 0.2)) {
        path += fmt::format("[{}]", generateRandomIndex());
        generateImpl(path, type->childAt(0));
      } else if (coinToss(rng_, 0.2)) {
        path += "[*]";
        generateImpl(path, type->childAt(0));
      } else if (coinToss(rng_, 0.2)) {
        path += ".*";
      } else if (makeRandomVariation_ && coinToss(rng_, 0.1)) {
        // Intentionally test invalid json path.
        path += "[]";
        generateImpl(path, type->childAt(0));
      }
      return;
    case TypeKind::ROW: {
      const auto selectedField =
          rand<uint64_t>(rng_, 0, type->asRow().size() - 1);
      if (coinToss(rng_, 0.2)) {
        path += fmt::format("[{}]", selectedField);
        generateImpl(path, type->childAt(selectedField));
      } else if (coinToss(rng_, 0.6)) {
        if (coinToss(rng_, 0.2)) {
          path += ".*";
        } else if (coinToss(rng_, 0.2)) {
          path += "[*]";
        } else if (makeRandomVariation_ && coinToss(rng_, 0.2)) {
          // Intentionally test invalid json path.
          path += "[]";
        }
        // The result of .* or [*] is a collection of fields that can have
        // different type from selectedField. We intentionally make invalid
        // json path here to test corner cases.
        if (makeRandomVariation_ && coinToss(rng_, 0.3)) {
          generateImpl(path, type->childAt(selectedField));
        }
      }
      return;
    }
    case TypeKind::MAP: {
      const auto selectedKey =
          mapKeys_[rand<uint64_t>(rng_, 0, mapKeys_.size() - 1)].toString(
              type->childAt(0));
      if (coinToss(rng_, 0.1)) {
        path += fmt::format("['{}']", selectedKey);
        generateImpl(path, type->childAt(1));
      } else if (coinToss(rng_, 0.1)) {
        path += fmt::format("[\"{}\"]", selectedKey);
        generateImpl(path, type->childAt(1));
      } else if (coinToss(rng_, 0.1)) {
        path += "[*]";
        generateImpl(path, type->childAt(1));
      } else if (coinToss(rng_, 0.1)) {
        path += ".*";
        generateImpl(path, type->childAt(1));
      } else if (coinToss(rng_, 0.1)) {
        path += fmt::format(".{}", selectedKey);
        generateImpl(path, type->childAt(1));
      } else if (makeRandomVariation_ && coinToss(rng_, 0.1)) {
        // Intentionally test invalid json path.
        path += fmt::format(".\"{}\"", selectedKey);
        generateImpl(path, type->childAt(1));
      } else if (makeRandomVariation_ && coinToss(rng_, 0.1)) {
        // Intentionally test invalid json path.
        path += "[]";
        generateImpl(path, type->childAt(1));
      } else if (makeRandomVariation_ && coinToss(rng_, 0.1)) {
        // Intentionally test invalid json path.
        path += fmt::format("[{}]", selectedKey);
        generateImpl(path, type->childAt(1));
      }
      return;
    }
    default:
      VELOX_UNREACHABLE("Unsupported type");
  }
}

} // namespace facebook::velox::fuzzer
