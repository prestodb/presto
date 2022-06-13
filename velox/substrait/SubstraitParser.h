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

#include "velox/substrait/proto/substrait/algebra.pb.h"
#include "velox/substrait/proto/substrait/capabilities.pb.h"
#include "velox/substrait/proto/substrait/extensions/extensions.pb.h"
#include "velox/substrait/proto/substrait/function.pb.h"
#include "velox/substrait/proto/substrait/parameterized_types.pb.h"
#include "velox/substrait/proto/substrait/plan.pb.h"
#include "velox/substrait/proto/substrait/type.pb.h"
#include "velox/substrait/proto/substrait/type_expressions.pb.h"

namespace facebook::velox::substrait {

/// This class contains some common functions used to parse Substrait
/// components, and convert them into recognizable representations.
class SubstraitParser {
 public:
  /// Stores the type name and nullability.
  struct SubstraitType {
    std::string type;
    bool nullable;
  };

  /// Parse Substrait NamedStruct.
  std::vector<std::shared_ptr<SubstraitParser::SubstraitType>> parseNamedStruct(
      const ::substrait::NamedStruct& namedStruct);

  /// Parse Substrait Type.
  std::shared_ptr<SubstraitType> parseType(
      const ::substrait::Type& substraitType);

  /// Parse Substrait ReferenceSegment.
  int32_t parseReferenceSegment(
      const ::substrait::Expression::ReferenceSegment& refSegment);

  /// Make names in the format of {prefix}_{index}.
  std::vector<std::string> makeNames(const std::string& prefix, int size);

  /// Make node name in the format of n{nodeId}_{colIdx}.
  std::string makeNodeName(int nodeId, int colIdx);

  /// Get the column index from a node name in the format of
  /// n{nodeId}_{colIdx}.
  int getIdxFromNodeName(const std::string& nodeName);

  /// Find the Substrait function name according to the function id
  /// from a pre-constructed function map. The function specification can be
  /// a simple name or a compound name. The compound name format is:
  /// <function name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN>.
  /// Currently, the input types in the function specification are not used. But
  /// in the future, they should be used for the validation according the
  /// specifications in Substrait yaml files.
  const std::string& findFunctionSpec(
      const std::unordered_map<uint64_t, std::string>& functionMap,
      uint64_t id) const;

  /// Extracts the function name for a function from specified compound name.
  /// When the input is a simple name, it will be returned.
  std::string getFunctionName(const std::string& functionSpec) const;

  /// Extracts argument types for a function from specified compound name.
  void getFunctionTypes(
      const std::string& functionSpec,
      std::vector<std::string>& types) const;

  /// Find the Velox function name according to the function id
  /// from a pre-constructed function map.
  std::string findVeloxFunction(
      const std::unordered_map<uint64_t, std::string>& functionMap,
      uint64_t id) const;

  /// Map the Substrait function keyword into Velox function keyword.
  std::string mapToVeloxFunction(const std::string& substraitFunction) const;

 private:
  /// A map used for mapping Substrait function keywords into Velox functions'
  /// keywords. Key: the Substrait function keyword, Value: the Velox function
  /// keyword. For those functions with different names in Substrait and Velox,
  /// a mapping relation should be added here.
  std::unordered_map<std::string, std::string> substraitVeloxFunctionMap_ = {
      {"add", "plus"},
      {"subtract", "minus"},
      {"modulus", "mod"},
      {"not_equal", "neq"},
      {"equal", "eq"}};
};

} // namespace facebook::velox::substrait
