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

#pragma once

#include <unordered_map>
#include <vector>
#include "velox/dwio/dwrf/common/EncryptionSpecification.h"

namespace facebook::velox::dwrf::encryption {

template <typename T>
class EncryptionHandlerBase {
 public:
  EncryptionHandlerBase() = default;

  EncryptionHandlerBase(const EncryptionHandlerBase& handler)
      : providerType_(handler.providerType_),
        childNodes_{handler.childNodes_},
        rootNodes_{handler.rootNodes_} {
    providers_.reserve(handler.providers_.size());
    for (auto& provider : handler.providers_) {
      providers_.push_back(provider->clone());
    }
  }

  virtual ~EncryptionHandlerBase() = default;

  bool isEncrypted() const {
    return rootNodes_.size() > 0;
  }

  bool isEncrypted(uint32_t nodeId) const {
    return childNodes_.find(nodeId) != childNodes_.end();
  }

  dwio::common::encryption::EncryptionProvider getKeyProviderType() const {
    return providerType_;
  }

  uint32_t getEncryptionRoot(uint32_t nodeId) const {
    return childNodes_.at(nodeId);
  }

  uint32_t getEncryptionGroupCount() const {
    return providers_.size();
  }

  uint32_t getEncryptionGroupIndex(uint32_t nodeId) const {
    return rootNodes_.at(getEncryptionRoot(nodeId));
  }

  const T& getEncryptionProvider(uint32_t nodeId) const {
    return getEncryptionProviderByIndex(getEncryptionGroupIndex(nodeId));
  }

  const T& getEncryptionProviderByIndex(uint32_t groupIndex) const {
    return *providers_.at(groupIndex);
  }

 protected:
  dwio::common::encryption::EncryptionProvider providerType_;
  //  child nodes -> root nodes where encryption was applied
  std::unordered_map<uint32_t, uint32_t> childNodes_;
  // root nodes -> decrypter index
  std::unordered_map<uint32_t, uint32_t> rootNodes_;
  std::vector<std::unique_ptr<const T>> providers_;
};

} // namespace facebook::velox::dwrf::encryption
