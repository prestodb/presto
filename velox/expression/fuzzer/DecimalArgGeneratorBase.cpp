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
#include "velox/expression/fuzzer/DecimalArgGeneratorBase.h"
#include <boost/random/uniform_int_distribution.hpp>

namespace facebook::velox::fuzzer {
namespace {

// Returns all the possible decimal types.
const std::vector<TypePtr>& getAllTypes() {
  const auto generateAllTypes = []() {
    std::vector<TypePtr> allTypes;
    for (auto p = 1; p <= 38; ++p) {
      for (auto s = 0; s <= p; ++s) {
        allTypes.push_back(DECIMAL(p, s));
      }
    }
    return allTypes;
  };

  static const std::vector<TypePtr> allTypes = generateAllTypes();
  return allTypes;
}

uint32_t rand32(uint32_t max, FuzzerGenerator& rng) {
  return boost::random::uniform_int_distribution<uint32_t>()(rng) % max;
}
} // namespace

std::vector<TypePtr> DecimalArgGeneratorBase::generateArgs(
    const exec::FunctionSignature& /*signature*/,
    const TypePtr& returnType,
    FuzzerGenerator& rng) {
  auto inputs = findInputs(returnType, rng);
  for (const auto& input : inputs) {
    if (input == nullptr) {
      return {};
    }
  }
  return inputs;
}

void DecimalArgGeneratorBase::initialize(uint32_t numArgs) {
  switch (numArgs) {
    case 1: {
      for (const auto& t : getAllTypes()) {
        auto [p, s] = getDecimalPrecisionScale(*t);
        if (auto returnType = toReturnType(p, s)) {
          inputs_[returnType.value()].push_back({t});
        }
      }
      break;
    }
    case 2: {
      for (const auto& a : getAllTypes()) {
        for (const auto& b : getAllTypes()) {
          auto [p1, s1] = getDecimalPrecisionScale(*a);
          auto [p2, s2] = getDecimalPrecisionScale(*b);

          if (auto returnType = toReturnType(p1, s1, p2, s2)) {
            inputs_[returnType.value()].push_back({a, b});
          }
        }
      }
      break;
    }
    default:
      VELOX_NYI(
          "Initialization with {} argument types is not supported.", numArgs);
  }
}

std::vector<TypePtr> DecimalArgGeneratorBase::findInputs(
    const TypePtr& returnType,
    FuzzerGenerator& rng) const {
  const auto [p, s] = getDecimalPrecisionScale(*returnType);
  const auto it = inputs_.find({p, s});
  if (it == inputs_.end()) {
    VLOG(1) << "Cannot find input types for " << returnType->toString();
    return {};
  }

  const auto index = rand32(it->second.size(), rng);
  return it->second[index];
}
} // namespace facebook::velox::fuzzer
