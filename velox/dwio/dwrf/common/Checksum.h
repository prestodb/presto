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

#include "velox/dwio/common/exception/Exception.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"

#include <boost/crc.hpp>
#define XXH_INLINE_ALL
#include <xxhash.h>

namespace facebook::velox::dwrf {

class Checksum {
 public:
  explicit Checksum(proto::ChecksumAlgorithm type) : type_{type} {}

  virtual ~Checksum() = default;

  virtual void update(const void* input, size_t len) = 0;

  virtual int64_t getDigest(bool reset = true) = 0;

  proto::ChecksumAlgorithm getType() const {
    return type_;
  }

 private:
  proto::ChecksumAlgorithm type_;
};

class XxHash : public Checksum {
 public:
  XxHash()
      : Checksum{proto::ChecksumAlgorithm::XXHASH},
        state_{XXH64_createState()} {
    DWIO_ENSURE_NOT_NULL(state_, "failed to initialize xxhash");
    reset();
  }

  ~XxHash() override {
    XXH64_freeState(state_);
  }

  void update(const void* input, size_t len) override {
    DWIO_ENSURE_NE(XXH64_update(state_, input, len), XXH_ERROR);
  }

  int64_t getDigest(bool reset) override {
    auto ret = static_cast<int64_t>(XXH64_digest(state_));
    if (reset) {
      this->reset();
    }
    return ret;
  }

 private:
  XXH64_state_t* state_;
  unsigned long long seed_ = 0; // dwrf java uses seed 0

  void reset() {
    DWIO_ENSURE_NE(XXH64_reset(state_, seed_), XXH_ERROR);
  }
};

class Crc32 : public Checksum {
 public:
  Crc32() : Checksum{proto::ChecksumAlgorithm::CRC32} {
    reset();
  }

  ~Crc32() override = default;

  void update(const void* input, size_t len) override {
    crc_->process_bytes(input, len);
  }

  int64_t getDigest(bool reset) override {
    auto ret = crc_->checksum();
    if (reset) {
      this->reset();
    }
    return ret;
  }

 private:
  std::unique_ptr<boost::crc_32_type> crc_;

  void reset() {
    crc_ = std::make_unique<boost::crc_32_type>();
  }
};

class ChecksumFactory {
 public:
  static std::unique_ptr<Checksum> create(proto::ChecksumAlgorithm type) {
    switch (type) {
      case proto::ChecksumAlgorithm::NULL_:
        return {};
      case proto::ChecksumAlgorithm::XXHASH:
        return std::make_unique<XxHash>();
      case proto::ChecksumAlgorithm::CRC32:
        return std::make_unique<Crc32>();
      default:
        DWIO_RAISE("not supported");
    }
  }
};

} // namespace facebook::velox::dwrf
