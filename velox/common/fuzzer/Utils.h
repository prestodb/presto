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

#include <codecvt>

#include <folly/Random.h>

#include <boost/random/uniform_01.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <boost/random/uniform_real_distribution.hpp>

#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"

namespace facebook::velox::fuzzer {

using FuzzerGenerator = folly::detail::DefaultGenerator;

enum UTF8CharList {
  ASCII = 0, // Ascii character set.
  UNICODE_CASE_SENSITIVE = 1, // Unicode scripts that support case.
  EXTENDED_UNICODE = 2, // Extended Unicode: Arabic, Devanagiri etc
  MATHEMATICAL_SYMBOLS = 3, // Mathematical Symbols.
  ALPHABETIC = 4, // Alphabetic Symbols.
  NUMERIC = 5 // Numeric Symbols.
};

bool coinToss(FuzzerGenerator& rng, double threshold);

/// Generate a random type with given scalar types. The level of nesting is up
/// to maxDepth. If keyTypes is non-empty, choosing from keyTypes when
/// determining the types of map keys. If keyTypes is empty, choosing from
/// scalarTypes for the types of map keys. Similar for valueTypes.
TypePtr randType(
    FuzzerGenerator& rng,
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth,
    const std::vector<TypePtr>& mapKeyTypes = {},
    const std::vector<TypePtr>& mapValueTypes = {});

/// Similar to randType but generates a random map type.
TypePtr randMapType(
    FuzzerGenerator& rng,
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth,
    const std::vector<TypePtr>& mapKeyTypes = {},
    const std::vector<TypePtr>& mapValueTypes = {});

/// Similar to randType but generates a random row type.
RowTypePtr randRowType(
    FuzzerGenerator& rng,
    const std::vector<TypePtr>& scalarTypes,
    int maxDepth,
    const std::vector<TypePtr>& mapKeyTypes = {},
    const std::vector<TypePtr>& mapValueTypes = {});

struct DataSpec {
  bool includeNaN;
  bool includeInfinity;
};

enum class FuzzerTimestampPrecision : int8_t {
  kNanoSeconds = 0,
  kMicroSeconds = 1,
  kMilliSeconds = 2,
  kSeconds = 3,
};

// Generate random values for the different supported types.
template <typename T>
inline T rand(
    FuzzerGenerator& /*rng*/,
    DataSpec /*dataSpec*/ = {false, false}) {
  VELOX_NYI();
}

template <>
inline int8_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<int8_t>()(rng);
}

template <>
inline int16_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<int16_t>()(rng);
}

template <>
inline int32_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<int32_t>()(rng);
}

template <>
inline int64_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<int64_t>()(rng);
}

template <>
inline double rand(FuzzerGenerator& rng, DataSpec dataSpec) {
  if (dataSpec.includeNaN && coinToss(rng, 0.05)) {
    return std::nan("");
  }

  if (dataSpec.includeInfinity && coinToss(rng, 0.05)) {
    return std::numeric_limits<double>::infinity();
  }

  return boost::random::uniform_01<double>()(rng);
}

template <>
inline float rand(FuzzerGenerator& rng, DataSpec dataSpec) {
  if (dataSpec.includeNaN && coinToss(rng, 0.05)) {
    return std::nanf("");
  }

  if (dataSpec.includeInfinity && coinToss(rng, 0.05)) {
    return std::numeric_limits<float>::infinity();
  }

  return boost::random::uniform_01<float>()(rng);
}

template <>
inline bool rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<uint32_t>(0, 1)(rng);
}

template <>
inline uint32_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<uint32_t>()(rng);
}

template <>
inline uint64_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return boost::random::uniform_int_distribution<uint64_t>()(rng);
}

template <>
inline int128_t rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  return HugeInt::build(rand<int64_t>(rng), rand<uint64_t>(rng));
}

Timestamp randTimestamp(
    FuzzerGenerator& rng,
    FuzzerTimestampPrecision timestampPrecision);

template <>
inline Timestamp rand(FuzzerGenerator& rng, DataSpec /*dataSpec*/) {
  // TODO: support other timestamp precisions.
  return randTimestamp(rng, FuzzerTimestampPrecision::kMicroSeconds);
}

int32_t randDate(FuzzerGenerator& rng);

template <
    typename T,
    typename std::enable_if_t<std::is_arithmetic_v<T>, int> = 0>
inline T rand(FuzzerGenerator& rng, T min, T max) {
  if constexpr (std::is_integral_v<T>) {
    return boost::random::uniform_int_distribution<T>(min, max)(rng);
  } else {
    return boost::random::uniform_real_distribution<T>(min, max)(rng);
  }
}

/// Generates a random string in buf with characters of encodings. Return buf at
/// the end.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
std::string randString(
    FuzzerGenerator& rng,
    size_t length,
    const std::vector<UTF8CharList>& encodings,
    std::string& buf,
    std::wstring_convert<std::codecvt_utf8<char16_t>, char16_t>& converter);
#pragma GCC diagnostic pop

// Beginning of rule or grammar based input generation. The following set of
// classes will allow us to use a rule based approach to generate random inputs.

// Class rule defines abstract function generate which outputs our input string
// dependent on the defined rule instantiation.
class Rule {
 public:
  Rule() = default;

  virtual std::string generate() {
    return "";
  }

  virtual ~Rule() = default;
};

// List of Rules.
class RuleList : public Rule {
 public:
  explicit RuleList(std::vector<std::shared_ptr<Rule>> rules)
      : rules_{std::move(rules)} {}

  std::string generate() override {
    std::string string;
    for (auto& rule : rules_) {
      string += rule->generate();
    }
    return string;
  }

  size_t size() {
    return rules_.size();
  }

  std::shared_ptr<Rule> operator[](size_t index) const {
    if (index >= rules_.size()) {
      throw std::out_of_range("Index out of range");
    }
    return rules_[index];
  }

  virtual ~RuleList() = default;

 private:
  std::vector<std::shared_ptr<Rule>> rules_;
};

// Rules that are randomly included.
class OptionalRule : public Rule {
 public:
  OptionalRule(FuzzerGenerator& rng, std::shared_ptr<Rule> rule)
      : rng_(rng), rule_(std::move(rule)) {}

  std::string generate() override {
    return coinToss(rng_, .5) ? rule_->generate() : "";
  }

  virtual ~OptionalRule() = default;

 private:
  FuzzerGenerator& rng_;
  std::shared_ptr<Rule> rule_;
};

// Rules that can repeat 'count' times.
class RepeatingRule : public Rule {
 public:
  RepeatingRule(
      FuzzerGenerator& rng,
      std::shared_ptr<Rule> rule,
      size_t minCount,
      size_t maxCount)
      : rng_(rng),
        rule_(std::move(rule)),
        minCount_(minCount),
        maxCount_(maxCount) {}

  std::string generate() override {
    auto repetitions = rand<size_t>(rng_, minCount_, maxCount_);
    std::string string;
    for (size_t i = 0; i < repetitions; ++i) {
      string += rule_->generate();
    }
    return string;
  }

  virtual ~RepeatingRule() = default;

 private:
  FuzzerGenerator& rng_;
  std::shared_ptr<Rule> rule_;
  size_t minCount_, maxCount_;
};

// Rule randomly chosen from list.
class ChoiceRule : public Rule {
 public:
  ChoiceRule(FuzzerGenerator& rng, std::shared_ptr<RuleList> rules)
      : rng_(rng), rules_(std::move(rules)) {}

  std::string generate() override {
    auto index = rand<size_t>(rng_, 0, rules_->size() - 1);
    return (*rules_)[index]->generate();
  }

  virtual ~ChoiceRule() = default;

 private:
  FuzzerGenerator& rng_;
  std::shared_ptr<RuleList> rules_;
};

// Simple rule that is just a constant string.
class ConstantRule : public Rule {
 public:
  explicit ConstantRule(std::string constant)
      : constant_(std::move(constant)) {}

  std::string generate() override {
    return constant_;
  }

  virtual ~ConstantRule() = default;

 private:
  std::string constant_;
};

// String of 1 to 20 characters long that generates ASCII characters. Size and
// character list can be modified.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
class StringRule : public Rule {
 public:
  explicit StringRule(
      FuzzerGenerator& rng,
      std::vector<UTF8CharList> encodings = {UTF8CharList::ASCII},
      size_t minLength = 1,
      size_t maxLength = 20,
      bool flexibleLength = true)
      : rng_(rng),
        encodings_(std::move(encodings)),
        minLength_(minLength),
        maxLength_(maxLength),
        flexibleLength_(flexibleLength) {}

  std::string generate() override {
    auto length = maxLength_;
    if (flexibleLength_) {
      length = rand<size_t>(rng_, minLength_, maxLength_);
    }
    return randString(rng_, length, encodings_, buf_, converter_);
  }

  virtual ~StringRule() = default;

 private:
  FuzzerGenerator& rng_;
  std::vector<UTF8CharList> encodings_;
  size_t minLength_;
  size_t maxLength_;
  bool flexibleLength_;

  std::wstring_convert<std::codecvt_utf8<char16_t>, char16_t> converter_;
  std::string buf_;
};
#pragma GCC diagnostic pop

// Extends from StringRule, uses only alphabetic characters
class WordRule : public StringRule {
 public:
  explicit WordRule(FuzzerGenerator& rng)
      : StringRule(rng, {UTF8CharList::ALPHABETIC}) {}

  WordRule(
      FuzzerGenerator& rng,
      size_t minLength,
      size_t maxLength,
      bool flexibleLength)
      : StringRule(
            rng,
            {UTF8CharList::ALPHABETIC},
            minLength,
            maxLength,
            flexibleLength) {}

  virtual ~WordRule() = default;
};

// Extends from StringRule, uses only numeric characters for random number
// generation with a particular size.
class NumRule : public StringRule {
 public:
  explicit NumRule(FuzzerGenerator& rng)
      : StringRule(rng, {UTF8CharList::NUMERIC}) {}

  NumRule(
      FuzzerGenerator& rng,
      size_t minLength,
      size_t maxLength,
      bool flexibleLength)
      : StringRule(
            rng,
            {UTF8CharList::NUMERIC},
            minLength,
            maxLength,
            flexibleLength) {}

  virtual ~NumRule() = default;
};

} // namespace facebook::velox::fuzzer
