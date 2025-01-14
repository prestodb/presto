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

#include "velox/expression/fuzzer/SpecialFormSignatureGenerator.h"

namespace facebook::velox::fuzzer {

/// Generates signatures of special forms for the Spark expression fuzzer test
/// to use.
class SparkSpecialFormSignatureGenerator
    : public SpecialFormSignatureGenerator {
 protected:
  std::vector<exec::FunctionSignaturePtr> getSignaturesForCast() const override;

  const std::
      unordered_map<std::string, std::vector<exec::FunctionSignaturePtr>>&
      getSignatures() const override;

 private:
  std::vector<exec::FunctionSignaturePtr> getSignaturesForConcatWs() const;
};

} // namespace facebook::velox::fuzzer
