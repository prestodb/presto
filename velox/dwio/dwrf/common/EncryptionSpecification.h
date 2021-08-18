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

#include <vector>
#include "velox/dwio/common/encryption/Encryption.h"
#include "velox/dwio/common/exception/Exception.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"

namespace facebook::velox::dwrf::encryption {

proto::Encryption_KeyProvider toProto(
    dwio::common::encryption::EncryptionProvider provider);

dwio::common::encryption::EncryptionProvider fromProto(
    proto::Encryption_KeyProvider provider);

class FieldEncryptionSpecification {
 public:
  // TODO: support fields other than top level ones
  FieldEncryptionSpecification& withIndex(uint32_t index) {
    index_ = index;
    return *this;
  }

  FieldEncryptionSpecification& withEncryptionProperties(
      const std::shared_ptr<
          const dwio::common::encryption::EncryptionProperties>& props) {
    DWIO_ENSURE(props.get(), "props is required");
    props_ = props;
    return *this;
  }

 private:
  std::optional<uint32_t> index_;
  std::shared_ptr<const dwio::common::encryption::EncryptionProperties> props_;

  friend class EncryptionHandler;
};

class EncryptionSpecification {
 public:
  explicit EncryptionSpecification(
      dwio::common::encryption::EncryptionProvider provider)
      : providerType_{provider} {}

  EncryptionSpecification& withRootEncryptionProperties(
      const std::shared_ptr<
          const dwio::common::encryption::EncryptionProperties>& props) {
    DWIO_ENSURE(props.get(), "props is required");
    DWIO_ENSURE(
        fieldSpecs_.empty(),
        "only one of encryption properties and encrypted fields is allowed");
    rootProps_ = props;
    return *this;
  }

  EncryptionSpecification& withEncryptedField(
      const FieldEncryptionSpecification& spec) {
    DWIO_ENSURE(
        !rootProps_.get(),
        "only one of encryption properties and encrypted fields is allowed");
    fieldSpecs_.push_back(spec);
    return *this;
  }

 private:
  dwio::common::encryption::EncryptionProvider providerType_;
  std::shared_ptr<const dwio::common::encryption::EncryptionProperties>
      rootProps_;
  std::vector<FieldEncryptionSpecification> fieldSpecs_;

  friend class EncryptionHandler;
};

} // namespace facebook::velox::dwrf::encryption
