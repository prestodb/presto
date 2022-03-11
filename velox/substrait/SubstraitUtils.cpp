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

#include "velox/substrait/SubstraitUtils.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::substrait {

std::shared_ptr<SubstraitParser::SubstraitType> SubstraitParser::parseType(
    const ::substrait::Type& sType) {
  // The used type names should be aligned with those in Velox.
  std::string typeName;
  ::substrait::Type_Nullability nullability;
  switch (sType.kind_case()) {
    case ::substrait::Type::KindCase::kBool: {
      typeName = "BOOLEAN";
      nullability = sType.bool_().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kFp64: {
      typeName = "DOUBLE";
      nullability = sType.fp64().nullability();
      break;
    }
    case ::substrait::Type::KindCase::kStruct: {
      // TODO: Support for Struct is not fully added.
      typeName = "STRUCT";
      auto sStruct = sType.struct_();
      auto sTypes = sStruct.types();
      for (const auto& type : sTypes) {
        parseType(type);
      }
      break;
    }
    case ::substrait::Type::KindCase::kString: {
      typeName = "VARCHAR";
      nullability = sType.string().nullability();
      break;
    }
    default:
      VELOX_NYI("Substrait parsing for type {} not supported.", typeName);
  }

  bool nullable;
  switch (nullability) {
    case ::substrait::Type_Nullability::
        Type_Nullability_NULLABILITY_UNSPECIFIED:
      nullable = true;
      break;
    case ::substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE:
      nullable = true;
      break;
    case ::substrait::Type_Nullability::Type_Nullability_NULLABILITY_REQUIRED:
      nullable = false;
      break;
    default:
      VELOX_NYI(
          "Substrait parsing for nullability {} not supported.", nullability);
  }
  SubstraitType subType = {typeName, nullable};
  return std::make_shared<SubstraitType>(subType);
}

std::vector<std::shared_ptr<SubstraitParser::SubstraitType>>
SubstraitParser::parseNamedStruct(const ::substrait::NamedStruct& namedStruct) {
  // Names is not used currently.
  const auto& sNames = namedStruct.names();
  // Parse Struct.
  const auto& sStruct = namedStruct.struct_();
  const auto& sTypes = sStruct.types();
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>>
      substraitTypeList;
  substraitTypeList.reserve(sTypes.size());
  for (const auto& type : sTypes) {
    substraitTypeList.emplace_back(parseType(type));
  }
  return substraitTypeList;
}

int32_t SubstraitParser::parseReferenceSegment(
    const ::substrait::Expression::ReferenceSegment& sRef) {
  auto typeCase = sRef.reference_type_case();
  switch (typeCase) {
    case ::substrait::Expression::ReferenceSegment::ReferenceTypeCase::
        kStructField: {
      return sRef.struct_field().field();
    }
    default:
      VELOX_NYI(
          "Substrait conversion not supported for ReferenceSegment '{}'",
          typeCase);
  }
}

std::vector<std::string> SubstraitParser::makeNames(
    const std::string& prefix,
    int size) {
  std::vector<std::string> names;
  names.reserve(size);
  for (int i = 0; i < size; i++) {
    names.emplace_back(fmt::format("{}_{}", prefix, i));
  }
  return names;
}

std::string SubstraitParser::makeNodeName(int node_id, int col_idx) {
  return fmt::format("n{}_{}", node_id, col_idx);
}

std::string SubstraitParser::findSubstraitFuncSpec(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    uint64_t id) const {
  if (functionMap.find(id) == functionMap.end()) {
    VELOX_FAIL("Could not find function id {} in function map.", id);
  }
  std::unordered_map<uint64_t, std::string>& map =
      const_cast<std::unordered_map<uint64_t, std::string>&>(functionMap);
  return map[id];
}

std::string SubstraitParser::getSubFunctionName(
    const std::string& subFuncSpec) const {
  // Get the position of ":" in the function name.
  std::size_t pos = subFuncSpec.find(":");
  if (pos == std::string::npos) {
    return subFuncSpec;
  }
  return subFuncSpec.substr(0, pos);
}

std::string SubstraitParser::findVeloxFunction(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    uint64_t id) const {
  std::string subFuncSpec = findSubstraitFuncSpec(functionMap, id);
  std::string subFuncName = getSubFunctionName(subFuncSpec);
  return mapToVeloxFunction(subFuncName);
}

std::string SubstraitParser::mapToVeloxFunction(
    const std::string& subFunc) const {
  auto it = substraitVeloxFunctionMap.find(subFunc);
  if (it != substraitVeloxFunctionMap.end()) {
    return it->second;
  }

  // If not finding the mapping from Substrait function name to Velox function
  // name, the original Substrait function name will be used.
  return subFunc;
}

} // namespace facebook::velox::substrait
