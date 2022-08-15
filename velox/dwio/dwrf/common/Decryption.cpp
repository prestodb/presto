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

#include "velox/dwio/dwrf/common/Decryption.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwrf::encryption {

using dwio::common::encryption::Decrypter;
using dwio::common::encryption::DecrypterFactory;

namespace {
dwio::common::encryption::DummyDecrypterFactory dummyDecrypterFactory;
}

void DecryptionHandler::setKeys(
    const google::protobuf::RepeatedPtrField<std::string>& keys) {
  DWIO_ENSURE_EQ(keys.size(), providers_.size());
  for (size_t i = 0; i < providers_.size(); ++i) {
    const_cast<Decrypter&>(*providers_.at(i)).setKey(keys.Get(i));
  }
}

void DecryptionHandler::populateNodeMaps(
    const google::protobuf::RepeatedPtrField<proto::Type>& schema,
    uint32_t root,
    uint32_t groupIndex) {
  populateChildNodeMap(schema, root, root);
  rootNodes_[root] = groupIndex;
}

void DecryptionHandler::populateChildNodeMap(
    const google::protobuf::RepeatedPtrField<proto::Type>& schema,
    uint32_t root,
    uint32_t current) {
  auto pos = childNodes_.find(current);
  DWIO_ENSURE(pos == childNodes_.end(), "node already exists");
  childNodes_[current] = root;
  auto& n = schema.Get(current);
  for (auto c : n.subtypes()) {
    populateChildNodeMap(schema, root, c);
  }
}

// Temporary function before Footer can totally replace proto::Footer
std::unique_ptr<DecryptionHandler> DecryptionHandler::create(
    const FooterWrapper& footer,
    DecrypterFactory* factory) {
  return footer.format() == DwrfFormat::kDwrf
      ? create(*footer.getDwrfPtr(), factory)
      : std::make_unique<DecryptionHandler>();
}

std::unique_ptr<DecryptionHandler> DecryptionHandler::create(
    const proto::Footer& footer,
    DecrypterFactory* factory) {
  auto handler = std::make_unique<DecryptionHandler>();
  if (!footer.has_encryption()) {
    return handler;
  }

  // create provider according to metadata type
  if (!factory) {
    DWIO_WARN("decrypter factory is not provided for encrypted file");
    factory = &dummyDecrypterFactory;
  }
  auto& encryption = footer.encryption();
  DWIO_ENSURE(encryption.has_keyprovider(), "missing key provider");
  handler->providerType_ = fromProto(encryption.keyprovider());
  DWIO_ENSURE_GT(
      encryption.encryptiongroups_size(), 0, "empty encryption group");

  auto& type = footer.types();
  bool hasFooterKey = false;
  for (auto& group : encryption.encryptiongroups()) {
    DWIO_ENSURE_GT(group.nodes_size(), 0);
    DWIO_ENSURE_EQ(group.nodes_size(), group.statistics_size());

    auto index = handler->providers_.size();
    auto provider = factory->create(handler->providerType_);
    DWIO_ENSURE(provider.get(), "invalid provider");
    handler->providers_.push_back(std::move(provider));

    bool hasKey = group.has_keymetadata();
    if (index > 0) {
      DWIO_ENSURE_EQ(hasFooterKey, hasKey);
    } else {
      hasFooterKey = hasKey;
    }
    // in the case when group doesn't have explicit footer key, reuse that from
    // the stripe metadata
    auto& decrypter = const_cast<Decrypter&>(*handler->providers_.at(index));
    if (hasFooterKey) {
      decrypter.setKey(group.keymetadata());
    } else {
      DWIO_ENSURE_GT(footer.stripes_size(), 0);
      auto& stripe = footer.stripes(0);
      DWIO_ENSURE_EQ(
          stripe.keymetadata_size(), encryption.encryptiongroups_size());
      decrypter.setKey(stripe.keymetadata(index));
    }

    // walk the schema tree and fill nodes mapping. nodes doesn't have to be top
    // level fields.
    for (auto n : group.nodes()) {
      handler->populateNodeMaps(type, n, index);
    }
  }

  return handler;
}

} // namespace facebook::velox::dwrf::encryption
