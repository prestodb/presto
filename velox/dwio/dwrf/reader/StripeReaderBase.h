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

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/common/Decryption.h"
#include "velox/dwio/dwrf/reader/ReaderBase.h"

namespace facebook::velox::dwrf {

class StripeReaderBase {
 public:
  explicit StripeReaderBase(const std::shared_ptr<ReaderBase>& reader)
      : reader_{reader},
        handler_{std::make_unique<encryption::DecryptionHandler>(
            reader_->getDecryptionHandler())} {}

  // for testing purpose
  StripeReaderBase(
      const std::shared_ptr<ReaderBase>& reader,
      const proto::StripeFooter* stripeFooter)
      : reader_{reader},
        handler_{std::make_unique<encryption::DecryptionHandler>(
            reader_->getDecryptionHandler())},
        stripeFooter_{const_cast<proto::StripeFooter*>(stripeFooter)},
        canLoad_{false} {
    // The footer is expected to be arena allocated and to stay
    // live for the lifetime of 'this'.
    DWIO_ENSURE(stripeFooter->GetArena());
  }

  virtual ~StripeReaderBase() = default;

  // Set state to be ready for next stripe, loading stripe information if not
  // prefetched
  StripeInformationWrapper loadStripe(uint32_t index, bool& preload);

  // Get footer of stripe currently being read
  const proto::StripeFooter& getStripeFooter() const {
    DWIO_ENSURE_NOT_NULL(
        stripeFooter_, "stripe hasn't been initialized for read");
    return *stripeFooter_;
  }

  // Get footer of stripe at index i, if it's been loaded.
  const proto::StripeFooter& getStripeFooter(uint32_t index) const {
    const proto::StripeFooter* result = nullptr;
    prefetchedStripes_.withRLock([&](auto& prefetchedStripes) {
      auto stateIt = prefetchedStripes.find(index);
      DWIO_ENSURE(stateIt != prefetchedStripes.end());
      result = stateIt->second->footer;
    });
    return *result;
  }

  dwio::common::BufferedInput& getStripeInput() const {
    return stripeInput_ ? *stripeInput_ : reader_->getBufferedInput();
  }

  ReaderBase& getReader() const {
    return *reader_;
  }

  const std::shared_ptr<ReaderBase>& readerBaseShared() const {
    return reader_;
  }

  const encryption::DecryptionHandler& getDecryptionHandler() const {
    return *handler_;
  }

  /**
  Populates prefetchedStripes_ with a BufferedInput for the given stripe index.
  If readerbase should be used instead, set value to null.
  Input value of preload can be mutated in case where readerBase had stripe
  preloaded, but preload flag was not provided
  is preload is false and stripe isn't prebuffered in readerbase, this function
  returns having set prefetchedStripes_[index] to an appropriate value to be
  LATER fully loaded by RowReader, after StripeStreams are built.
  @return true if prefetch was successful
  */
  bool fetchStripe(uint32_t index, bool& preload);

 protected:
  struct PrefetchedStripeBase {
    std::unique_ptr<dwio::common::BufferedInput> stripeInput;
    proto::StripeFooter* footer;
  };

  std::shared_ptr<PrefetchedStripeBase> getStripeBase(uint32_t index) {
    return prefetchedStripes_.rlock()->at(index);
  }

  bool freeStripeAt(uint32_t index) {
    return prefetchedStripes_.wlock()->erase(index) == 1;
  }

 private:
  const std::shared_ptr<ReaderBase> reader_;
  const std::unique_ptr<encryption::DecryptionHandler> handler_;
  std::unique_ptr<dwio::common::BufferedInput> stripeInput_;
  std::optional<uint32_t> lastStripeIndex_;
  proto::StripeFooter* stripeFooter_ = nullptr;
  bool canLoad_{true};

  // Map containing stripe blobs which have already been loaded.
  // A null value represents that the stripe at the key is loaded in
  // reader_'s BufferedInput. No entry for a given key means that
  // the stripe has not been loaded yet, or has already been processed.
  folly::Synchronized<
      folly::F14FastMap<uint32_t, std::shared_ptr<PrefetchedStripeBase>>>
      prefetchedStripes_;

  // stripeFooter default null arg should only be used for testing.
  void loadEncryptionKeys(
      uint32_t index,
      proto::StripeFooter* stripeFooter = nullptr);

  friend class StripeLoadKeysTest;
};

} // namespace facebook::velox::dwrf
