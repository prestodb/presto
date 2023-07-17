/*
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

#include "presto_cpp/main/JsonSignatureParser.h"
#include <folly/json.h>
#include "velox/common/base/Exceptions.h"

namespace facebook::presto {
namespace {

// Parse a single type signature.
velox::exec::TypeSignature parseTypeSignature(const folly::dynamic& input) {
  VELOX_USER_CHECK(
      input.isString(),
      "Function type name should be a string. Got: {}",
      input.typeName());
  return velox::exec::parseTypeSignature(input.asString());
}

// Parses a list of type signatures.
std::vector<velox::exec::TypeSignature> parseTypeSignatures(
    const folly::dynamic& input) {
  VELOX_USER_CHECK(
      input.isArray(),
      "Function paramType should be an array. Got: {}",
      input.typeName());
  std::vector<velox::exec::TypeSignature> typeSignatures;
  typeSignatures.reserve(input.size());

  for (const auto& it : input) {
    typeSignatures.emplace_back(parseTypeSignature(it));
  }
  return typeSignatures;
}

// Parses a single signature.
velox::exec::FunctionSignaturePtr parseSignature(const folly::dynamic& input) {
  VELOX_USER_CHECK(
      input.isObject(),
      "Function signature should be an object. Got: {}",
      input.typeName());

  auto* outputType = input.get_ptr("outputType");
  auto* paramTypes = input.get_ptr("paramTypes");
  VELOX_USER_CHECK(
      (outputType != nullptr) && (paramTypes != nullptr),
      "`outputType` and `paramTypes` are mandatory in a signature.");

  auto paramTypeSignatures = parseTypeSignatures(*paramTypes);
  std::vector<bool> constantArguments(paramTypeSignatures.size(), false);

  return std::make_shared<velox::exec::FunctionSignature>(
      std::unordered_map<std::string, velox::exec::SignatureVariable>{},
      parseTypeSignature(*outputType),
      std::move(paramTypeSignatures),
      std::move(constantArguments),
      /*variableArity=*/false);
}

// Parses a list of signatures for a function.
std::vector<velox::exec::FunctionSignaturePtr> parseSignatures(
    const folly::dynamic& input) {
  VELOX_USER_CHECK(
      input.isArray(),
      "The value for a function item should be an array of signatures. Got: {}",
      input.typeName());
  std::vector<velox::exec::FunctionSignaturePtr> signatures;
  signatures.reserve(input.size());

  for (const auto& signature : input) {
    signatures.emplace_back(parseSignature(signature));
  }
  return signatures;
}

} // namespace

JsonSignatureParser::JsonSignatureParser(const std::string& input) {
  folly::dynamic topLevelJson;
  try {
    topLevelJson = folly::parseJson(input);
  } catch (const std::exception& e) {
    VELOX_USER_FAIL(
        "Unable to parse function signature JSON file: {}", e.what());
  }

  VELOX_USER_CHECK(
      topLevelJson.isObject(),
      "Top level json item needs to be an object. Got: {}",
      topLevelJson.typeName());

  // Search for the top-level key.
  if (auto* found = topLevelJson.get_ptr("udfSignatureMap")) {
    parse(*found);
  } else {
    VELOX_USER_FAIL("Unable to find top level 'udfSignatureMap' key.");
  }
}

void JsonSignatureParser::parse(const folly::dynamic& input) {
  VELOX_USER_CHECK(
      input.isObject(),
      "Input signatures should be an object. Got: {}",
      input.typeName());

  // Iterate over each function.
  for (auto it : input.items()) {
    // Check function name.
    const auto& key = it.first;
    VELOX_USER_CHECK(
        key.isString() && !key.getString().empty(),
        "The key for a function item should be a non-empty string. Got: '{}'",
        key.asString());

    // Check and parse list of signatures.
    signaturesMap_.emplace(key.getString(), parseSignatures(it.second));
  }
}

} // namespace facebook::presto
