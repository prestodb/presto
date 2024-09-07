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

struct StripeMetadata {
  dwio::common::BufferedInput* stripeInput;
  std::shared_ptr<const proto::StripeFooter> footer;
  std::unique_ptr<encryption::DecryptionHandler> decryptionHandler;
  StripeInformationWrapper stripeInfo;

  StripeMetadata(
      std::shared_ptr<dwio::common::BufferedInput> _stripeInput,
      std::shared_ptr<const proto::StripeFooter> _footer,
      std::unique_ptr<encryption::DecryptionHandler> _decryptionHandler,
      StripeInformationWrapper _stripeInfo)
      : stripeInput{_stripeInput.get()},
        footer{std::move(_footer)},
        decryptionHandler{std::move(_decryptionHandler)},
        stripeInfo{std::move(_stripeInfo)},
        stripeInputOwned_{std::move(_stripeInput)} {}

  StripeMetadata(
      dwio::common::BufferedInput* _stripeInput,
      std::shared_ptr<const proto::StripeFooter> _footer,
      std::unique_ptr<encryption::DecryptionHandler> _decryptionHandler,
      StripeInformationWrapper _stripeInfo)
      : stripeInput{_stripeInput},
        footer{std::move(_footer)},
        decryptionHandler{std::move(_decryptionHandler)},
        stripeInfo{std::move(_stripeInfo)} {}

 private:
  const std::shared_ptr<dwio::common::BufferedInput> stripeInputOwned_;
};

class StripeReaderBase {
 public:
  explicit StripeReaderBase(const std::shared_ptr<ReaderBase>& reader)
      : reader_{reader} {}

  virtual ~StripeReaderBase() = default;

  ReaderBase& getReader() const {
    return *reader_;
  }

  const std::shared_ptr<ReaderBase>& readerBaseShared() const {
    return reader_;
  }

  std::unique_ptr<const StripeMetadata> fetchStripe(
      uint32_t index,
      bool& preload) const;

 private:
  // stripeFooter default null arg should only be used for testing.
  void loadEncryptionKeys(
      uint32_t index,
      const proto::StripeFooter& stripeFooter,
      const StripeInformationWrapper& stripeInfo,
      encryption::DecryptionHandler& handler) const;

  const std::shared_ptr<ReaderBase> reader_;

  friend class StripeLoadKeysTest;
};

} // namespace facebook::velox::dwrf
