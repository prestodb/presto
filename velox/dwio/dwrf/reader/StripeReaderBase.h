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
      proto::StripeFooter* footer)
      : reader_{reader},
        footer_{footer},
        handler_{std::make_unique<encryption::DecryptionHandler>(
            reader_->getDecryptionHandler())} {
    // The footer is expected to be arena allocated and to stay
    // live for the lifetime of 'this'.
    DWIO_ENSURE(footer->GetArena());
  }

  virtual ~StripeReaderBase() = default;

  StripeInformationWrapper loadStripe(uint32_t index, bool& preload);

  const proto::StripeFooter& getStripeFooter() const {
    DWIO_ENSURE_NOT_NULL(footer_, "stripe not loaded");
    return *footer_;
  }

  dwio::common::BufferedInput& getStripeInput() const {
    return stripeInput_ ? *stripeInput_ : reader_->getBufferedInput();
  }

  ReaderBase& getReader() const {
    return *reader_;
  }

  std::shared_ptr<ReaderBase> readerBaseShared() {
    return reader_;
  }

  const encryption::DecryptionHandler& getDecryptionHandler() const {
    return *handler_;
  }

 private:
  std::shared_ptr<ReaderBase> reader_;
  std::unique_ptr<dwio::common::BufferedInput> stripeInput_;
  proto::StripeFooter* footer_ = nullptr;
  std::unique_ptr<encryption::DecryptionHandler> handler_;
  std::optional<uint32_t> lastStripeIndex_;

  void loadEncryptionKeys(uint32_t index);

  friend class StripeLoadKeysTest;
};

} // namespace facebook::velox::dwrf
