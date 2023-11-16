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
bool StripeReaderBase::fetchStripe(uint32_t index, bool& preload) {
  DWIO_ENSURE(canLoad_);
  if (prefetchedStripes_.rlock()->contains(index)) {
    VLOG(1) << "Stripe data already fetched at index: " << index;
    return false;
  }
  auto& fileFooter = reader_->getFooter();
  DWIO_ENSURE_LT(index, fileFooter.stripesSize(), "invalid stripe index");
  auto stripe = fileFooter.stripes(index);
  auto& cache = reader_->getMetadataCache();

  uint64_t offset = stripe.offset();
  uint64_t length =
      stripe.indexLength() + stripe.dataLength() + stripe.footerLength();

  std::unique_ptr<dwio::common::BufferedInput> prefetchedStripe;
  if (reader_->getBufferedInput().isBuffered(offset, length)) {
    preload = true;
    prefetchedStripe = nullptr;
  } else {
    prefetchedStripe = reader_->getBufferedInput().clone();
    if (preload) {
      // If metadata cache exists, adjust read position to avoid re-reading
      // metadata sections
      if (cache) {
        if (cache->has(StripeCacheMode::INDEX, index)) {
          offset += stripe.indexLength();
          length -= stripe.indexLength();
        }
        if (cache->has(StripeCacheMode::FOOTER, index)) {
          length -= stripe.footerLength();
        }
      }

      prefetchedStripe->enqueue({offset, length, "stripe"});
      prefetchedStripe->load(LogType::STRIPE);
    }
  }

  // load stripe footer
  std::unique_ptr<dwio::common::SeekableInputStream> stream;
  if (cache) {
    stream = cache->get(StripeCacheMode::FOOTER, index);
  }

  if (!stream) {
    dwio::common::BufferedInput& bi =
        prefetchedStripe ? *prefetchedStripe : reader_->getBufferedInput();
    stream = bi.read(
        stripe.offset() + stripe.indexLength() + stripe.dataLength(),
        stripe.footerLength(),
        LogType::STRIPE_FOOTER);
  }

  proto::StripeFooter* stripeFooter;

  // todo: reuse footer memory in cases where prefetch does not occur, or we
  // have finished processing a stripe and can reuse its footer's memory
  stripeFooter = google::protobuf::Arena::CreateMessage<proto::StripeFooter>(
      reader_->arena());

  auto streamDebugInfo = fmt::format("Stripe {} Footer ", index);
  ProtoUtils::readProtoInto<proto::StripeFooter>(
      reader_->createDecompressedStream(std::move(stream), streamDebugInfo),
      stripeFooter);

  auto prefetchedStripeBase = std::make_shared<PrefetchedStripeBase>();

  prefetchedStripeBase->footer = stripeFooter;
  prefetchedStripeBase->stripeInput = std::move(prefetchedStripe);

  prefetchedStripes_.wlock()->operator[](index) = prefetchedStripeBase;

  // refresh stripe encryption key if necessary
  loadEncryptionKeys(index, stripeFooter);
  lastStripeIndex_ = index;

  return true;
}

// Sets stripeInput_ to a new BufferedInput (or null, if data is already
// buffered in ReaderBase), and loads stripe footer and encryption keys
StripeInformationWrapper StripeReaderBase::loadStripe(
    uint32_t index,
    bool& preload /* load the whole stripe if true*/) {
  DWIO_ENSURE(canLoad_);
  auto& fileFooter = reader_->getFooter();
  DWIO_ENSURE_LT(index, fileFooter.stripesSize(), "invalid stripe index");
  auto stripe = fileFooter.stripes(index);

  fetchStripe(index, preload);
  auto prefetchedStripeBase =
      prefetchedStripes_.withRLock([&](auto& prefetchedStripes) {
        auto prefetchedStatesIt = prefetchedStripes.find(index);
        DWIO_ENSURE(prefetchedStatesIt != prefetchedStripes.end());
        return prefetchedStatesIt->second;
      });

  stripeFooter_ = prefetchedStripeBase->footer;
  stripeInput_ = std::move(prefetchedStripeBase->stripeInput);
  return stripe;
}

void StripeReaderBase::loadEncryptionKeys(
    uint32_t index,
    proto::StripeFooter* stripeFooter) {
  if (stripeFooter == nullptr) {
    stripeFooter = stripeFooter_;
  }
  if (!handler_->isEncrypted()) {
    return;
  }

  DWIO_ENSURE_EQ(
      stripeFooter->encryptiongroups_size(),
      handler_->getEncryptionGroupCount());
  auto& fileFooter = reader_->getFooter();
  DWIO_ENSURE_LT(index, fileFooter.stripesSize(), "invalid stripe index");

  auto stripe = fileFooter.stripes(index);
  // If current stripe has keys, load these keys.
  if (stripe.keyMetadataSize() > 0) {
    handler_->setKeys(stripe.keyMetadata());
  } else {
    // If current stripe doesn't have keys, then:
    //  1. If it's sequential read (ie. we've just finished reading one stripe
    //  and are now trying to read the stripe right after it), we can reuse the
    //  loaded keys.
    //  2. If it's not sequential read (which means we performed a skip/seek
    //  into a random stripe in the file), we need to sequentially lookup
    //  previous stripes, until we find a stripe with keys.
    DWIO_ENSURE_GT(index, 0, "first stripe must have key");
    bool isSequentialRead =
        (lastStripeIndex_ && lastStripeIndex_.value() == index - 1);
    if (!isSequentialRead) {
      uint32_t prevIndex = index - 1;
      while (true) {
        auto prev = fileFooter.stripes(prevIndex);
        if (prev.keyMetadataSize() > 0) {
          handler_->setKeys(prev.keyMetadata());
          break;
        }
        DWIO_ENSURE_GE(prevIndex, 0, "key not found");
        --prevIndex;
      }
    }
  }
}

} // namespace facebook::velox::dwrf
