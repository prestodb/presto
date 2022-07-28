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

#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/EncryptionCommon.h"

namespace facebook::velox::dwrf::encryption {

class DecryptionHandler
    : public EncryptionHandlerBase<dwio::common::encryption::Decrypter> {
 public:
  void setKeys(const google::protobuf::RepeatedPtrField<std::string>& keys);

  static std::unique_ptr<DecryptionHandler> create(
      const proto::Footer& footer,
      dwio::common::encryption::DecrypterFactory* factory = nullptr);

  // Temporary function before Footer can totally replace proto::Footer
  static std::unique_ptr<DecryptionHandler> create(
      const Footer& footer,
      dwio::common::encryption::DecrypterFactory* factory = nullptr);

 private:
  void populateNodeMaps(
      const google::protobuf::RepeatedPtrField<proto::Type>& schema,
      uint32_t root,
      uint32_t groupIndex);

  void populateChildNodeMap(
      const google::protobuf::RepeatedPtrField<proto::Type>& schema,
      uint32_t root,
      uint32_t current);
};

} // namespace facebook::velox::dwrf::encryption
