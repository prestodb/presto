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

#include "velox/expression/fuzzer/ArgValuesGenerators.h"

#include "velox/common/fuzzer/ConstrainedGenerators.h"
#include "velox/common/fuzzer/Utils.h"
#include "velox/core/Expressions.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::fuzzer {

std::vector<core::TypedExprPtr> JsonParseArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 1);
  populateInputTypesAndNames(signature, state);

  // Populate state.customInputGenerators_ for the argument that requires custom
  // generation. A nullptr should be added at state.customInputGenerators_[i] if
  // the i-th argument does not require custom input generation.
  const auto representedType = facebook::velox::randType(rng, 3);
  const auto seed = rand<uint32_t>(rng);
  const auto nullRatio = options.nullRatio;
  state.customInputGenerators_.emplace_back(
      std::make_shared<fuzzer::JsonInputGenerator>(
          seed,
          signature.args[0],
          nullRatio,
          fuzzer::getRandomInputGenerator(seed, representedType, nullRatio),
          true));

  // Populate inputExpressions_ for the argument that requires custom
  // generation. A nullptr should be added at inputExpressions[i] if the i-th
  // argument does not require custom input generation.
  std::vector<core::TypedExprPtr> inputExpressions{
      signature.args.size(), nullptr};
  inputExpressions[0] = std::make_shared<core::FieldAccessTypedExpr>(
      signature.args[0], state.inputRowNames_.back());
  return inputExpressions;
};

std::vector<core::TypedExprPtr> StringEscapeArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 1);
  populateInputTypesAndNames(signature, state);

  const auto representedType = facebook::velox::randType(rng, 0);
  const auto seed = static_cast<size_t>(rand<uint32_t>(rng));
  const auto nullRatio = options.nullRatio;

  state.customInputGenerators_.emplace_back(
      std::make_shared<RandomInputGenerator<StringView>>(
          seed,
          signature.args[0],
          nullRatio,
          20,
          std::vector<UTF8CharList>{
              UTF8CharList::ASCII,
              UTF8CharList::UNICODE_CASE_SENSITIVE,
              UTF8CharList::EXTENDED_UNICODE,
              UTF8CharList::MATHEMATICAL_SYMBOLS},
          RandomStrVariationOptions{
              0.5, // controlCharacterProbability
              0.5, // escapeStringProbability
              0.1, // truncateProbability
          }));

  // Populate inputExpressions_ for the argument that requires custom
  // generation. A nullptr should be added at inputExpressions[i] if the i-th
  // argument does not require custom input generation.
  std::vector<core::TypedExprPtr> inputExpressions{
      signature.args.size(), nullptr};
  inputExpressions[0] = std::make_shared<core::FieldAccessTypedExpr>(
      signature.args[0], state.inputRowNames_.back());
  return inputExpressions;
};

std::vector<core::TypedExprPtr> PhoneNumberArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  populateInputTypesAndNames(signature, state);

  const auto representedType = facebook::velox::randType(rng, 0);
  const auto seed = rand<uint32_t>(rng);
  const auto nullRatio = options.nullRatio;
  std::vector<core::TypedExprPtr> inputExpressions;

  if (functionName_ == "fb_phone_region_code") {
    // Populate state.customInputGenerators_ and inputExpressions with custom
    // input generator for required phone number string field
    VELOX_CHECK_GE(signature.args.size(), 1);
    state.customInputGenerators_.emplace_back(
        std::make_shared<fuzzer::PhoneNumberInputGenerator>(
            seed, signature.args[0], nullRatio));

    VELOX_CHECK_GE(state.inputRowNames_.size(), 1);
    inputExpressions.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(
        signature.args[0],
        signature.args.size() == 1
            ? state.inputRowNames_.back()
            : state.inputRowNames_[state.inputRowNames_.size() - 2]));

    //  Populate state.customInputGenerators_ and inputExpressions with nullptr
    //  for inputs that do not require custom input generators
    if (signature.args.size() == 2) {
      state.customInputGenerators_.emplace_back(nullptr);
      inputExpressions.emplace_back(nullptr);
    }
  } else if (functionName_ == "fb_canonicalize_phone_number") {
    VELOX_CHECK_GE(signature.args.size(), 1);
    // Populate state.customInputGenerators_ and inputExpressions with custom
    // input generator for required phone number string field
    state.customInputGenerators_.emplace_back(
        std::make_shared<fuzzer::PhoneNumberInputGenerator>(
            seed, signature.args[0], nullRatio));

    VELOX_CHECK_GE(state.inputRowNames_.size(), signature.args.size());
    inputExpressions.emplace_back(std::make_shared<core::FieldAccessTypedExpr>(
        signature.args[0],
        signature.args.size() == 1
            ? state.inputRowNames_.back()
            : state.inputRowNames_
                  [state.inputRowNames_.size() - signature.args.size()]));

    for (int i = 1; i < signature.args.size(); i++) {
      if (i == 1) {
        // For country code, use a random string generator with ASCII, UTF8
        // specifying max length to be greater than 2 (max length of country
        // code)
        state.customInputGenerators_.emplace_back(
            std::make_shared<RandomInputGenerator<StringView>>(
                seed,
                signature.args[i],
                nullRatio,
                4,
                std::vector<UTF8CharList>{
                    UTF8CharList::ASCII,
                    UTF8CharList::UNICODE_CASE_SENSITIVE}));
      } else {
        // For phoneFormat, use a random string generator with ASCII, UTF8,
        // MATHEMATICAL_SYMBOLS. Specify max length to be greater than 12 to
        // have enough randomization yet not too long
        state.customInputGenerators_.emplace_back(
            std::make_shared<RandomInputGenerator<StringView>>(
                seed,
                signature.args[i],
                nullRatio,
                12,
                std::vector<UTF8CharList>{
                    UTF8CharList::ASCII,
                    UTF8CharList::UNICODE_CASE_SENSITIVE,
                    UTF8CharList::EXTENDED_UNICODE,
                    UTF8CharList::MATHEMATICAL_SYMBOLS}));
      }
      VELOX_CHECK_GE(state.inputRowNames_.size(), signature.args.size() - i);
      inputExpressions.emplace_back(
          std::make_shared<core::FieldAccessTypedExpr>(
              signature.args[i],
              state.inputRowNames_
                  [state.inputRowNames_.size() + i - signature.args.size()]));
    }
  }
  return inputExpressions;
}
} // namespace facebook::velox::fuzzer
