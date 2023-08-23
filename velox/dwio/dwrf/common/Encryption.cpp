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

#include "velox/dwio/dwrf/common/Encryption.h"

namespace facebook::velox::dwrf::encryption {

using dwio::common::TypeWithId;
using dwio::common::encryption::EncrypterFactory;
using dwio::common::encryption::EncryptionProperties;
using dwio::common::encryption::EncryptionPropertiesEqual;
using dwio::common::encryption::EncryptionPropertiesHash;

void EncryptionHandler::populateNodeMaps(
    const TypeWithId& root,
    uint32_t groupIndex) {
  populateChildNodeMap(root.id(), root);
  rootNodes_[root.id()] = groupIndex;
}

void EncryptionHandler::populateChildNodeMap(
    uint32_t root,
    const TypeWithId& node) {
  auto pos = childNodes_.find(node.id());
  DWIO_ENSURE(pos == childNodes_.end(), "node already exists");
  childNodes_[node.id()] = root;
  for (size_t i = 0; i < node.size(); ++i) {
    populateChildNodeMap(root, *node.childAt(i));
  }
}

uint32_t EncryptionHandler::addEncrypter(
    EncrypterFactory& factory,
    const EncryptionProperties& props) {
  auto index = providers_.size();
  auto encrypter = factory.create(providerType_, props);
  DWIO_ENSURE(encrypter.get(), "invalid encrypter");
  providers_.push_back(std::move(encrypter));
  return index;
}

std::unique_ptr<EncryptionHandler> EncryptionHandler::create(
    const std::shared_ptr<const TypeWithId>& schema,
    const EncryptionSpecification& spec,
    EncrypterFactory* factory) {
  auto handler = std::make_unique<EncryptionHandler>();
  if (!spec.rootProps() && spec.fieldSpecs().empty()) {
    return handler;
  }

  DWIO_ENSURE_NOT_NULL(factory, "factory is required");
  handler->providerType_ = spec.providerType();
  if (spec.rootProps() != nullptr) {
    // all fields encrypted using same properties
    handler->addEncrypter(*factory, *spec.rootProps());
    handler->populateNodeMaps(*schema, 0);
  } else {
    std::unordered_map<
        const EncryptionProperties*,
        uint32_t,
        EncryptionPropertiesHash,
        EncryptionPropertiesEqual>
        encrypters;
    for (auto& fs : spec.fieldSpecs()) {
      DWIO_ENSURE(fs.index_, "index not set");
      auto propsPtr = fs.props_.get();
      DWIO_ENSURE_NOT_NULL(propsPtr, "encryption properties not set");
      uint32_t index = 0;
      auto pos = encrypters.find(propsPtr);
      if (pos != encrypters.end()) {
        index = pos->second;
      } else {
        index = handler->addEncrypter(*factory, *propsPtr);
        encrypters[propsPtr] = index;
      }

      handler->populateNodeMaps(*schema->childAt(fs.index_.value()), index);
    }
  }

  return handler;
}

} // namespace facebook::velox::dwrf::encryption
