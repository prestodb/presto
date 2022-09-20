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

#include <optional>
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/substrait/VeloxSubstraitSignature.h"
#include "velox/substrait/proto/substrait/algebra.pb.h"
#include "velox/substrait/proto/substrait/plan.pb.h"
#include "velox/type/Type.h"

namespace facebook::velox::substrait {

struct ExtensionFunctionId {
  /// Substrait extension YAML file uri.
  std::string uri;

  /// Substrait signature used in the function extension declaration is a
  /// combination of the name of the function along with a list of input
  /// argument types.The format is as follows : <function
  /// name>:<short_arg_type0>_<short_arg_type1>_..._<short_arg_typeN> for more
  /// detail information about the argument type please refer to link
  /// https://substrait.io/extensions/#function-signature-compound-names.
  std::string signature;

  bool operator==(const ExtensionFunctionId& other) const {
    return (uri == other.uri && signature == other.signature);
  }
};

/// Assigns unique IDs to function signatures using ExtensionFunctionId.
class SubstraitExtensionCollector {
 public:
  SubstraitExtensionCollector();

  /// Given a scalar function name and argument types, return the functionId
  /// using ExtensionFunctionId.
  int getReferenceNumber(
      const std::string& functionName,
      const std::vector<TypePtr>& arguments);

  /// Given an aggregate function name and argument types and aggregation Step,
  /// return the functionId using ExtensionFunctionId.
  int getReferenceNumber(
      const std::string& functionName,
      const std::vector<TypePtr>& arguments,
      core::AggregationNode::Step aggregationStep);

  /// Add extension functions to Substrait plan.
  void addExtensionsToPlan(::substrait::Plan* plan) const;

 private:
  /// A bi-direction hash map to keep the relation between reference number and
  /// either function or type signature.
  template <class T>
  class BiDirectionHashMap {
   public:
    /// Add (key, value) pair if doesn't exist already, i.e. forwardMap doesn't
    /// contain the key and reverseMap doesn't contain the value.
    ///
    /// @return True if the values were added successfully. False, otherwise.
    bool putIfAbsent(const int& key, const T& value);

    const std::unordered_map<int, ExtensionFunctionId> forwardMap() const {
      return forwardMap_;
    }

    const std::unordered_map<T, int>& reverseMap() const {
      return reverseMap_;
    }

   private:
    std::unordered_map<int, T> forwardMap_;
    std::unordered_map<T, int> reverseMap_;
  };

  /// Assigns unique IDs to function signatures using ExtensionFunctionId.
  int getReferenceNumber(const ExtensionFunctionId& extensionFunctionId);

  int functionReferenceNumber = -1;
  std::shared_ptr<BiDirectionHashMap<ExtensionFunctionId>> extensionFunctions_;
};

using SubstraitExtensionCollectorPtr =
    std::shared_ptr<SubstraitExtensionCollector>;

} // namespace facebook::velox::substrait

namespace std {

/// Hash function of facebook::velox::substrait::ExtensionFunctionId.
template <>
struct hash<facebook::velox::substrait::ExtensionFunctionId> {
  size_t operator()(
      const facebook::velox::substrait::ExtensionFunctionId& k) const {
    size_t val = hash<std::string>()(k.uri);
    val = val * 31 + hash<std::string>()(k.signature);
    return val;
  }
};

}; // namespace std
