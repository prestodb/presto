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

#include "velox/expression/tests/ArgumentTypeFuzzer.h"

#include <boost/algorithm/string.hpp>

#include "velox/expression/ReverseSignatureBinder.h"
#include "velox/expression/SignatureBinder.h"
#include "velox/type/Type.h"

namespace facebook::velox::test {

namespace {

// Return a random type among those in kSupportedTypes determined by seed.
// TODO: Extend this function to return arbitrary random types including nested
// complex types.
TypePtr randomType(std::mt19937& seed) {
  static std::vector<TypePtr> kSupportedTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      TIMESTAMP(),
      DATE(),
      INTERVAL_DAY_TIME(),
      SHORT_DECIMAL(10, 5),
      LONG_DECIMAL(20, 10)};
  auto index = folly::Random::rand32(kSupportedTypes.size(), seed);
  return kSupportedTypes[index];
}

std::string fromTypeToBaseName(const TypePtr& type) {
  return boost::algorithm::to_lower_copy(std::string{type->kindName()});
}

std::optional<TypeKind> fromBaseNameToTypeKind(std::string& typeName) {
  auto kindName = boost::algorithm::to_upper_copy(typeName);
  return tryMapNameToTypeKind(kindName);
}

} // namespace

void ArgumentTypeFuzzer::determineUnboundedTypeVariables() {
  for (auto& binding : bindings_) {
    if (!binding.second) {
      binding.second = randomType(seed_);
    }
  }
}

bool ArgumentTypeFuzzer::fuzzArgumentTypes(uint32_t maxVariadicArgs) {
  const auto& formalArgs = signature_.argumentTypes();
  auto formalArgsCnt = formalArgs.size();

  exec::ReverseSignatureBinder binder{signature_, returnType_};
  if (!binder.tryBind()) {
    return false;
  }
  bindings_ = binder.bindings();

  determineUnboundedTypeVariables();
  for (auto i = 0; i < formalArgsCnt; i++) {
    TypePtr actualArg;
    if (formalArgs[i].baseName() == "any") {
      actualArg = randomType(seed_);
    } else {
      actualArg =
          exec::SignatureBinder::tryResolveType(formalArgs[i], bindings_);
      VELOX_CHECK(actualArg != nullptr);
    }
    argumentTypes_.push_back(actualArg);
  }

  // Generate random repeats of the last argument type if the signature is
  // variadic.
  if (signature_.variableArity()) {
    auto repeat = folly::Random::rand32(maxVariadicArgs + 1, seed_);
    auto last = argumentTypes_[formalArgsCnt - 1];
    for (int i = 0; i < repeat; ++i) {
      argumentTypes_.push_back(last);
    }
  }

  return true;
}

} // namespace facebook::velox::test
