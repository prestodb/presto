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

#include "velox/substrait/SubstraitExtensionCollector.h"

namespace facebook::velox::substrait {

int SubstraitExtensionCollector::getReferenceNumber(
    const std::string& functionName,
    const std::vector<TypePtr>& arguments) {
  const auto& substraitFunctionSignature =
      VeloxSubstraitSignature::toSubstraitSignature(functionName, arguments);
  // TODO: Currently we treat all velox registry based function signatures as
  // custom substrait extension, so no uri link and leave it as empty.
  return getReferenceNumber({"", substraitFunctionSignature});
}

int SubstraitExtensionCollector::getReferenceNumber(
    const std::string& functionName,
    const std::vector<TypePtr>& arguments,
    const core::AggregationNode::Step /* aggregationStep */) {
  // TODO: Ignore aggregationStep for now, will refactor when introduce velox
  // registry for function signature binding
  return getReferenceNumber(functionName, arguments);
}

template <typename T>
bool SubstraitExtensionCollector::BiDirectionHashMap<T>::putIfAbsent(
    const int& key,
    const T& value) {
  if (forwardMap_.find(key) == forwardMap_.end() &&
      reverseMap_.find(value) == reverseMap_.end()) {
    forwardMap_[key] = value;
    reverseMap_[value] = key;
    return true;
  }
  return false;
}

void SubstraitExtensionCollector::addExtensionsToPlan(
    ::substrait::Plan* plan) const {
  using SimpleExtensionURI = ::substrait::extensions::SimpleExtensionURI;
  // Currently we don't introduce any substrait extension YAML files, so always
  // only have one URI.
  SimpleExtensionURI* extensionUri = plan->add_extension_uris();
  extensionUri->set_extension_uri_anchor(1);

  for (const auto& [referenceNum, functionId] :
       extensionFunctions_->forwardMap()) {
    auto extensionFunction =
        plan->add_extensions()->mutable_extension_function();
    extensionFunction->set_extension_uri_reference(
        extensionUri->extension_uri_anchor());
    extensionFunction->set_function_anchor(referenceNum);
    extensionFunction->set_name(functionId.signature);
  }
}

SubstraitExtensionCollector::SubstraitExtensionCollector() {
  extensionFunctions_ =
      std::make_shared<BiDirectionHashMap<ExtensionFunctionId>>();
}

int SubstraitExtensionCollector::getReferenceNumber(
    const ExtensionFunctionId& extensionFunctionId) {
  const auto& extensionFunctionAnchorIt =
      extensionFunctions_->reverseMap().find(extensionFunctionId);
  if (extensionFunctionAnchorIt != extensionFunctions_->reverseMap().end()) {
    return extensionFunctionAnchorIt->second;
  }
  ++functionReferenceNumber;
  extensionFunctions_->putIfAbsent(
      functionReferenceNumber, extensionFunctionId);
  return functionReferenceNumber;
}

} // namespace facebook::velox::substrait
