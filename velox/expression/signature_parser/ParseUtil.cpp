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

#include "velox/expression/signature_parser/ParseUtil.h"
#include <string>
#include "velox/type/Type.h"

namespace facebook::velox::exec {

TypeSignaturePtr inferTypeWithSpaces(
    const std::vector<std::string>& words,
    bool canHaveFieldName) {
  VELOX_CHECK_GE(words.size(), 2);
  const auto& fieldName = words[0];
  const auto allWords = folly::join(" ", words);
  if (hasType(allWords) || !canHaveFieldName) {
    return std::make_shared<exec::TypeSignature>(
        exec::TypeSignature(allWords, {}));
  }
  return std::make_shared<exec::TypeSignature>(exec::TypeSignature(
      allWords.data() + fieldName.size() + 1, {}, fieldName));
}

} // namespace facebook::velox::exec
