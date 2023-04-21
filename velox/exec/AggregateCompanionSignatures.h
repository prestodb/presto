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

#include "velox/exec/Aggregate.h"
#include "velox/expression/FunctionSignature.h"

namespace facebook::velox::exec {

class CompanionSignatures {
 public:
  // Return signatures of the partial companion function for the original
  // aggregate function of `signatures`.
  static std::vector<AggregateFunctionSignaturePtr> partialFunctionSignatures(
      const std::vector<AggregateFunctionSignaturePtr>& signatures);

  static std::string partialFunctionName(const std::string& name);

  static AggregateFunctionSignaturePtr mergeFunctionSignature(
      const AggregateFunctionSignaturePtr& signature);

  // Return signatures of the merge companion function for the original
  // aggregate function of `signatures`.
  static std::vector<AggregateFunctionSignaturePtr> mergeFunctionSignatures(
      const std::vector<AggregateFunctionSignaturePtr>& signatures);

  static std::string mergeFunctionName(const std::string& name);

  // Return true if there are multiple signatures that have the same
  // intermediate type.
  static bool hasSameIntermediateTypesAcrossSignatures(
      const std::vector<AggregateFunctionSignaturePtr>& signatures);

  static AggregateFunctionSignaturePtr mergeExtractFunctionSignature(
      const AggregateFunctionSignaturePtr& signature);

  static std::vector<AggregateFunctionSignaturePtr>
  mergeExtractFunctionSignatures(
      const std::vector<AggregateFunctionSignaturePtr>& signatures);

  static std::string mergeExtractFunctionNameWithSuffix(
      const std::string& name,
      const TypeSignature& resultType);

  static std::string mergeExtractFunctionName(const std::string& name);

  // Return signature of the extract companion function for the original
  // aggregate function of `signature`.
  static FunctionSignaturePtr extractFunctionSignature(
      const AggregateFunctionSignaturePtr& signature);

  static std::string extractFunctionNameWithSuffix(
      const std::string& name,
      const TypeSignature& resultType);

  // Return signatures of the extract companion function for the original
  // aggregate function of `signatures`.
  static std::vector<FunctionSignaturePtr> extractFunctionSignatures(
      const std::vector<AggregateFunctionSignaturePtr>& signatures);

  static std::string extractFunctionName(const std::string& name);

  static std::
      unordered_map<TypeSignature, std::vector<AggregateFunctionSignaturePtr>>
      groupSignaturesByReturnType(
          const std::vector<AggregateFunctionSignaturePtr>& signatures);

 private:
  static TypeSignature normalizeTypeImpl(
      const TypeSignature& type,
      const std::unordered_map<std::string, SignatureVariable>& allVariables,
      std::unordered_map<std::string, std::string>& renamedVariables);

  // Rename variables used in `type` as T0, T1, ..., Tn, with the ordering of
  // variables being visited in pre-order traversal.
  static TypeSignature normalizeType(
      const TypeSignature& type,
      const std::unordered_map<std::string, SignatureVariable>& allVariables);

  // Process signatures of distinct pairs of intermediate and result types and
  // return a vector of the processed signatures.
  template <class T>
  static std::vector<T> processSignaturesOfDistinctTypes(
      const std::vector<AggregateFunctionSignaturePtr>& signatures,
      const std::function<T(const AggregateFunctionSignaturePtr&)>&
          processSignature) {
    std::unordered_set<std::pair<TypeSignature, TypeSignature>>
        distinctIntermediateAndResultTypes;
    std::vector<T> processedSignatures;

    for (const auto& signature : signatures) {
      auto normalizdIntermediateType =
          normalizeType(signature->intermediateType(), signature->variables());
      auto normalizedReturnType =
          normalizeType(signature->returnType(), signature->variables());
      if (distinctIntermediateAndResultTypes.count(std::make_pair(
              normalizdIntermediateType, normalizedReturnType))) {
        continue;
      }

      auto processedSignature = processSignature(signature);
      if (processedSignature) {
        processedSignatures.push_back(std::move(processedSignature));
        // Only remember the intermediate and result types and skip subsequent
        // signatures of the same types if we have already successfully
        // processed one of them. There may be multiple signatures of the same
        // intermediate type, some can be processed successfully while some
        // cannot. We only need one processed signature for each pair of
        // intermediate and result types.
        distinctIntermediateAndResultTypes.emplace(
            normalizdIntermediateType, normalizedReturnType);
      }
    }
    return processedSignatures;
  }

  // Process signatures of distinct intermediate types and return a vector of
  // the processed signatures.
  static std::vector<AggregateFunctionSignaturePtr>
  processSignaturesOfDistinctIntermediateTypes(
      const std::vector<AggregateFunctionSignaturePtr>& signatures,
      const std::function<AggregateFunctionSignaturePtr(
          const AggregateFunctionSignaturePtr&)>& processSignature) {
    std::unordered_set<TypeSignature> distinctIntermediateTypes;
    std::vector<AggregateFunctionSignaturePtr> processedSignatures;

    for (const auto& signature : signatures) {
      auto normalizdIntermediateType =
          normalizeType(signature->intermediateType(), signature->variables());
      if (distinctIntermediateTypes.count(normalizdIntermediateType)) {
        continue;
      }

      auto processedSignature = processSignature(signature);
      if (processedSignature) {
        processedSignatures.push_back(std::move(processedSignature));
        // Only remember the intermediate type and skip subsequent signatures of
        // the same type if we have already successfully processed one of them.
        // There may be multiple signatures of the same intermediate type, some
        // can be processed successfully while some cannot. We only need one
        // processed signature for each intermediate type.
        distinctIntermediateTypes.emplace(normalizdIntermediateType);
      }
    }
    return processedSignatures;
  }
};

} // namespace facebook::velox::exec
