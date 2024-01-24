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

#include "velox/substrait/SubstraitParser.h"
#include <string>
#include "velox/common/base/Exceptions.h"
#include "velox/substrait/TypeUtils.h"
#include "velox/substrait/VeloxSubstraitSignature.h"

namespace facebook::velox::substrait {

TypePtr SubstraitParser::parseType(const ::substrait::Type& substraitType) {
  switch (substraitType.kind_case()) {
    case ::substrait::Type::KindCase::kBool:
      return BOOLEAN();
    case ::substrait::Type::KindCase::kI8:
      return TINYINT();
    case ::substrait::Type::KindCase::kI16:
      return SMALLINT();
    case ::substrait::Type::KindCase::kI32:
      return INTEGER();
    case ::substrait::Type::KindCase::kI64:
      return BIGINT();
    case ::substrait::Type::KindCase::kFp32:
      return REAL();
    case ::substrait::Type::KindCase::kFp64:
      return DOUBLE();
    case ::substrait::Type::KindCase::kString:
      return VARCHAR();
    case ::substrait::Type::KindCase::kBinary:
      return VARBINARY();
    case ::substrait::Type::KindCase::kStruct: {
      const auto& substraitStruct = substraitType.struct_();
      const auto& structTypes = substraitStruct.types();
      std::vector<TypePtr> types;
      std::vector<std::string> names;
      for (int i = 0; i < structTypes.size(); i++) {
        types.emplace_back(parseType(structTypes[i]));
        names.emplace_back(fmt::format("col_{}", i));
      }
      return ROW(std::move(names), std::move(types));
    }
    case ::substrait::Type::KindCase::kList: {
      // The type name of list is in the format of: ARRAY<T>.
      const auto& fieldType = substraitType.list().type();
      return ARRAY(parseType(fieldType));
    }
    case ::substrait::Type::KindCase::kMap: {
      // The type name of map is in the format of: MAP<K,V>.
      const auto& sMap = substraitType.map();
      const auto& keyType = sMap.key();
      const auto& valueType = sMap.value();
      return MAP(parseType(keyType), parseType(valueType));
    }
    case ::substrait::Type::KindCase::kUserDefined:
      // We only support UNKNOWN type to handle the null literal whose type is
      // not known.
      return UNKNOWN();
    case ::substrait::Type::KindCase::kDate:
      return DATE();
    default:
      VELOX_NYI(
          "Parsing for Substrait type not supported: {}",
          substraitType.DebugString());
  }
}

std::vector<TypePtr> SubstraitParser::parseNamedStruct(
    const ::substrait::NamedStruct& namedStruct) {
  // Nte that "names" are not used.

  // Parse Struct.
  const auto& substraitStruct = namedStruct.struct_();
  const auto& substraitTypes = substraitStruct.types();
  std::vector<TypePtr> typeList;
  typeList.reserve(substraitTypes.size());
  for (const auto& type : substraitTypes) {
    typeList.emplace_back(parseType(type));
  }
  return typeList;
}

int32_t SubstraitParser::parseReferenceSegment(
    const ::substrait::Expression::ReferenceSegment& refSegment) {
  auto typeCase = refSegment.reference_type_case();
  switch (typeCase) {
    case ::substrait::Expression::ReferenceSegment::ReferenceTypeCase::
        kStructField: {
      return refSegment.struct_field().field();
    }
    default:
      VELOX_NYI(
          "Substrait conversion not supported for ReferenceSegment '{}'",
          std::to_string(typeCase));
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

int SubstraitParser::getIdxFromNodeName(const std::string& nodeName) {
  // Get the position of "_" in the function name.
  std::size_t pos = nodeName.find("_");
  if (pos == std::string::npos) {
    VELOX_FAIL("Invalid node name.");
  }
  if (pos == nodeName.size() - 1) {
    VELOX_FAIL("Invalid node name.");
  }
  // Get the column index.
  std::string colIdx = nodeName.substr(pos + 1);
  try {
    return stoi(colIdx);
  } catch (const std::exception& err) {
    VELOX_FAIL(err.what());
  }
}

const std::string& SubstraitParser::findFunctionSpec(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    uint64_t id) const {
  if (functionMap.find(id) == functionMap.end()) {
    VELOX_FAIL("Could not find function id {} in function map.", id);
  }
  std::unordered_map<uint64_t, std::string>& map =
      const_cast<std::unordered_map<uint64_t, std::string>&>(functionMap);
  return map[id];
}

std::string SubstraitParser::findVeloxFunction(
    const std::unordered_map<uint64_t, std::string>& functionMap,
    uint64_t id) const {
  std::string funcSpec = findFunctionSpec(functionMap, id);
  std::string_view funcName = getNameBeforeDelimiter(funcSpec, ":");
  return mapToVeloxFunction({funcName.begin(), funcName.end()});
}

std::string SubstraitParser::mapToVeloxFunction(
    const std::string& substraitFunction) const {
  auto it = substraitVeloxFunctionMap_.find(substraitFunction);
  if (it != substraitVeloxFunctionMap_.end()) {
    return it->second;
  }

  // If not finding the mapping from Substrait function name to Velox function
  // name, the original Substrait function name will be used.
  return substraitFunction;
}

std::vector<std::string> SubstraitParser::getSubFunctionTypes(
    const std::string& substraitFunction) {
  // Get the position of ":" in the function name.
  size_t pos = substraitFunction.find(":");
  // Get the input types.
  std::vector<std::string> types;
  if (pos == std::string::npos || pos == substraitFunction.size() - 1) {
    return types;
  }
  // Extract input types with delimiter.
  for (;;) {
    const size_t endPos = substraitFunction.find("_", pos + 1);
    if (endPos == std::string::npos) {
      std::string typeName = substraitFunction.substr(pos + 1);
      if (typeName != "opt" && typeName != "req") {
        types.emplace_back(typeName);
      }
      break;
    }

    const std::string typeName =
        substraitFunction.substr(pos + 1, endPos - pos - 1);
    if (typeName != "opt" && typeName != "req") {
      types.emplace_back(typeName);
    }
    pos = endPos;
  }
  return types;
}

std::vector<TypePtr> SubstraitParser::getInputTypes(
    const std::string& signature) {
  std::vector<std::string> typeStrs = getSubFunctionTypes(signature);
  std::vector<TypePtr> types;
  types.reserve(typeStrs.size());
  for (const auto& typeStr : typeStrs) {
    types.emplace_back(
        VeloxSubstraitSignature::fromSubstraitSignature(typeStr));
  }
  return types;
}

} // namespace facebook::velox::substrait
