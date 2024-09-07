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

#include "velox/dwio/dwrf/reader/StripeReaderBase.h"

namespace facebook::velox::dwrf {

using dwio::common::LogType;

// preload is not considered or mutated if stripe has already been fetched. e.g.
// if fetchStripe(0, false) is called, result will be cached and fetchStripe(0,
// true) will reuse the result without considering the new preload directive
std::unique_ptr<const StripeMetadata> StripeReaderBase::fetchStripe(
    uint32_t index,
    bool& preload) const {
  auto& fileFooter = reader_->footer();
  VELOX_CHECK_LT(index, fileFooter.stripesSize(), "invalid stripe index");
  auto stripe = fileFooter.stripes(index);
  auto& cache = reader_->metadataCache();

  uint64_t offset = stripe.offset();
  uint64_t length =
      stripe.indexLength() + stripe.dataLength() + stripe.footerLength();

  std::unique_ptr<dwio::common::BufferedInput> stripeInput;
  if (reader_->bufferedInput().isBuffered(offset, length)) {
    preload = true;
    stripeInput = nullptr;
  } else {
    stripeInput = reader_->bufferedInput().clone();
    if (preload) {
      // If metadata cache exists, adjust read position to avoid re-reading
      // metadata sections
      if (cache != nullptr) {
        if (cache->has(StripeCacheMode::INDEX, index)) {
          offset += stripe.indexLength();
          length -= stripe.indexLength();
        }
        if (cache->has(StripeCacheMode::FOOTER, index)) {
          length -= stripe.footerLength();
        }
      }

      stripeInput->enqueue({offset, length, "stripe"});
      stripeInput->load(LogType::STRIPE);
    }
  }

  // load stripe footer
  std::unique_ptr<dwio::common::SeekableInputStream> footerStream;
  if (cache) {
    footerStream = cache->get(StripeCacheMode::FOOTER, index);
  }

  if (!footerStream) {
    dwio::common::BufferedInput& bufferInput =
        (stripeInput != nullptr) ? *stripeInput : reader_->bufferedInput();
    footerStream = bufferInput.read(
        stripe.offset() + stripe.indexLength() + stripe.dataLength(),
        stripe.footerLength(),
        LogType::STRIPE_FOOTER);
  }

  const auto streamDebugInfo = fmt::format("Stripe {} Footer ", index);

  auto arena = std::make_shared<google::protobuf::Arena>();
  auto* rawFooter =
      google::protobuf::Arena::CreateMessage<proto::StripeFooter>(arena.get());
  ProtoUtils::readProtoInto(
      reader_->createDecompressedStream(
          std::move(footerStream), streamDebugInfo),
      rawFooter);
  std::shared_ptr<proto::StripeFooter> stripeFooter(
      rawFooter, [arena = std::move(arena)](auto*) { arena->Reset(); });

  auto decryptionHandler = std::make_unique<encryption::DecryptionHandler>(
      reader_->decryptionHandler());

  // refresh stripe encryption key if necessary
  loadEncryptionKeys(index, *stripeFooter, stripe, *decryptionHandler);
  return stripeInput == nullptr ? std::make_unique<const StripeMetadata>(
                                      &reader_->bufferedInput(),
                                      std::move(stripeFooter),
                                      std::move(decryptionHandler),
                                      std::move(stripe))
                                : std::make_unique<const StripeMetadata>(
                                      std::move(stripeInput),
                                      std::move(stripeFooter),
                                      std::move(decryptionHandler),
                                      std::move(stripe));
}

void StripeReaderBase::loadEncryptionKeys(
    uint32_t index,
    const proto::StripeFooter& stripeFooter,
    const StripeInformationWrapper& stripeInfo,
    encryption::DecryptionHandler& handler) const {
  if (!handler.isEncrypted()) {
    return;
  }

  VELOX_CHECK_EQ(
      stripeFooter.encryptiongroups_size(), handler.getEncryptionGroupCount());
  const auto& fileFooter = reader_->footer();
  VELOX_CHECK_LT(index, fileFooter.stripesSize(), "invalid stripe index");

  // If current stripe has keys, load these keys.
  if (stripeInfo.keyMetadataSize() > 0) {
    handler.setKeys(stripeInfo.keyMetadata());
  } else {
    // If current stripe doesn't have keys, then:
    //  1. If it's sequential read (ie. we've just finished reading one stripe
    //  and are now trying to read the stripe right after it), we can reuse the
    //  loaded keys.
    //  2. If it's not sequential read (which means we performed a skip/seek
    //  into a random stripe in the file), we need to sequentially lookup
    //  previous stripes, until we find a stripe with keys.
    //
    // But, since we are enabling parallel loads now, let's not rely on
    // sequential order. Let's apply (2).
    VELOX_CHECK_GT(index, 0, "first stripe must have key");

    uint32_t prevIndex = index - 1;
    while (true) {
      const auto prevStripeInfo = fileFooter.stripes(prevIndex);
      if (prevStripeInfo.keyMetadataSize() > 0) {
        handler.setKeys(prevStripeInfo.keyMetadata());
        break;
      }
      --prevIndex;
    }
  }
}

} // namespace facebook::velox::dwrf
