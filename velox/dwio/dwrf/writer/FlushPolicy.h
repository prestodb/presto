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

#include <cstdint>
#include "velox/dwio/common/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/WriterContext.h"

namespace facebook::velox::dwrf {
enum class FlushDecision {
  SKIP,
  FLUSH_DICTIONARY,
  ABANDON_DICTIONARY,
};

class DWRFFlushPolicy : virtual public dwio::common::FlushPolicy {
 public:
  virtual ~DWRFFlushPolicy() override = default;
  virtual bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override = 0;
  // Check additional flush criteria based on dictioanry encoding.
  // Different actions can also be taken based on the additional checks.
  // e.g. abandon dictionary encodings.
  virtual FlushDecision shouldFlushDictionary(
      bool stripeProgressDecision,
      bool overMemoryBudget,
      const WriterContext& context) = 0;
  virtual void onClose() override = 0;
};

class DefaultFlushPolicy : public DWRFFlushPolicy {
 public:
  explicit DefaultFlushPolicy(
      uint64_t stripeSizeThreshold,
      uint64_t dictionarySizeThreshold);
  virtual ~DefaultFlushPolicy() override = default;

  bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override {
    return stripeProgress.stripeSizeEstimate >= stripeSizeThreshold_;
  }

  FlushDecision shouldFlushDictionary(
      bool stripeProgressDecision,
      bool overMemoryBudget,
      int64_t dictionaryMemoryUsage);

  FlushDecision shouldFlushDictionary(
      bool stripeProgressDecision,
      bool overMemoryBudget,
      const WriterContext& context) override;

  void onClose() override {
    // No-op
  }

 private:
  const uint64_t stripeSizeThreshold_;
  const uint64_t dictionarySizeThreshold_;
};

class StaticBudgetFlushPolicy : public DWRFFlushPolicy {
 public:
  explicit StaticBudgetFlushPolicy(
      uint64_t stripeSizeThreshold,
      uint64_t dictionarySizeThreshold);
  virtual ~StaticBudgetFlushPolicy() override = default;

  bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override {
    return defaultFlushPolicy_.shouldFlush(stripeProgress);
  }

  FlushDecision shouldFlushDictionary(
      bool stripeProgressDecision,
      bool overMemoryBudget,
      int64_t dictionaryMemoryUsage);

  FlushDecision shouldFlushDictionary(
      bool stripeProgressDecision,
      bool overMemoryBudget,
      const WriterContext& context) override;

  void onClose() override {
    // No-op
  }

 private:
  DefaultFlushPolicy defaultFlushPolicy_;
};

class RowsPerStripeFlushPolicy : public DWRFFlushPolicy {
 public:
  explicit RowsPerStripeFlushPolicy(std::vector<uint64_t> rowsPerStripe);
  virtual ~RowsPerStripeFlushPolicy() override = default;

  bool shouldFlush(const dwio::common::StripeProgress& stripeProgress) override;

  FlushDecision shouldFlushDictionary(
      bool /* stripeProgressDecision */,
      bool /* overMemoryBudget */,
      const WriterContext& /* context */) override {
    return FlushDecision::SKIP;
  }

  void onClose() override {
    // No-op
  }

 private:
  std::vector<uint64_t> rowsPerStripe_;
};

class RowThresholdFlushPolicy : public DWRFFlushPolicy {
 public:
  explicit RowThresholdFlushPolicy(uint64_t rowCountThreshold)
      : rowCountThreshold_{rowCountThreshold} {}
  virtual ~RowThresholdFlushPolicy() override = default;

  bool shouldFlush(
      const dwio::common::StripeProgress& stripeProgress) override {
    return stripeProgress.stripeRowCount >= rowCountThreshold_;
  }

  FlushDecision shouldFlushDictionary(
      bool /* stripeProgressDecision */,
      bool /* overMemoryBudget */,
      const WriterContext& /* context */) override {
    return FlushDecision::SKIP;
  }

  void onClose() override {
    // No-op
  }

 private:
  uint64_t rowCountThreshold_;
};

class LambdaFlushPolicy : public DWRFFlushPolicy {
 public:
  explicit LambdaFlushPolicy(std::function<bool()> lambda) : lambda_{lambda} {}
  virtual ~LambdaFlushPolicy() override = default;

  bool shouldFlush(const dwio::common::StripeProgress& /* ununsed */) override {
    return lambda_();
  }

  FlushDecision shouldFlushDictionary(
      bool /* unused */,
      bool /* unused */,
      const WriterContext& /* unused */) override {
    return FlushDecision::SKIP;
  }

  void onClose() override {
    // No-op
  }

 private:
  std::function<bool()> lambda_;
};

} // namespace facebook::velox::dwrf
