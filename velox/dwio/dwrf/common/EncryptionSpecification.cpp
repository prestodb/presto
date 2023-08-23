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

#include "velox/dwio/dwrf/common/EncryptionSpecification.h"

namespace facebook::velox::dwrf::encryption {

using dwio::common::encryption::EncryptionProvider;

proto::Encryption_KeyProvider toProto(EncryptionProvider provider) {
  switch (provider) {
    case EncryptionProvider::Unknown:
      return proto::Encryption_KeyProvider_UNKNOWN;
    case EncryptionProvider::CryptoService:
      return proto::Encryption_KeyProvider_CRYPTO_SERVICE;
    default:
      DWIO_RAISE("unknown provider: ", provider);
  }
}

EncryptionProvider fromProto(proto::Encryption_KeyProvider provider) {
  switch (provider) {
    case proto::Encryption_KeyProvider_UNKNOWN:
      return EncryptionProvider::Unknown;
    case proto::Encryption_KeyProvider_CRYPTO_SERVICE:
      return EncryptionProvider::CryptoService;
    default:
      DWIO_RAISE("unknown provider: ", provider);
  }
}

EncryptionSpecification& EncryptionSpecification::withRootEncryptionProperties(
    const std::shared_ptr<const dwio::common::encryption::EncryptionProperties>&
        props) {
  DWIO_ENSURE(props.get(), "props is required");
  DWIO_ENSURE(
      fieldSpecs_.empty(),
      "only one of encryption properties and encrypted fields is allowed");
  rootProps_ = props;
  return *this;
}

EncryptionSpecification& EncryptionSpecification::withEncryptedField(
    const FieldEncryptionSpecification& spec) {
  DWIO_ENSURE(
      !rootProps_.get(),
      "only one of encryption properties and encrypted fields is allowed");
  fieldSpecs_.push_back(spec);
  return *this;
}
} // namespace facebook::velox::dwrf::encryption
