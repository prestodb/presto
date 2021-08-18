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

#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/dwrf/common/EncryptionCommon.h"

namespace facebook::velox::dwrf::encryption {

class EncryptionHandler
    : public EncryptionHandlerBase<dwio::common::encryption::Encrypter> {
 public:
  static std::unique_ptr<EncryptionHandler> create(
      const std::shared_ptr<const Type>& schema,
      const EncryptionSpecification& spec,
      dwio::common::encryption::EncrypterFactory* factory = nullptr) {
    return create(dwio::common::TypeWithId::create(schema), spec, factory);
  }

  static std::unique_ptr<EncryptionHandler> create(
      const std::shared_ptr<const dwio::common::TypeWithId>& schema,
      const EncryptionSpecification& spec,
      dwio::common::encryption::EncrypterFactory* factory = nullptr);

 private:
  void populateNodeMaps(
      const dwio::common::TypeWithId& root,
      uint32_t groupIndex);

  void populateChildNodeMap(
      uint32_t root,
      const dwio::common::TypeWithId& node);

  uint32_t addEncrypter(
      dwio::common::encryption::EncrypterFactory& factory,
      const dwio::common::encryption::EncryptionProperties& props);
};

} // namespace facebook::velox::dwrf::encryption
