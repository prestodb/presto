/*
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

#include "velox/core/PlanNode.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/Operator.h"

namespace facebook::presto::operators {

/// Plan node for reading shuffle data written by MaterializedOutput.
/// Paired with MaterializedOutputNode for symmetric A/B switching.
class MaterializedExchangeNode : public velox::core::PlanNode {
 public:
  MaterializedExchangeNode(
      const velox::core::PlanNodeId& id,
      velox::RowTypePtr outputType)
      : PlanNode(id), outputType_(std::move(outputType)) {}

  const velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  /// Leaf node — no child plan nodes. Data comes from ExchangeClient via
  /// splits (RemoteConnectorSplit), not from upstream operators.
  const std::vector<velox::core::PlanNodePtr>& sources() const override {
    static const std::vector<velox::core::PlanNodePtr> kEmptySources;
    return kEmptySources;
  }

  bool requiresExchangeClient() const override {
    return true;
  }

  bool requiresSplits() const override {
    return true;
  }

  std::string_view name() const override {
    return "MaterializedExchange";
  }

  folly::dynamic serialize() const override;

  static velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void addDetails(std::stringstream& /* stream */) const override {}

  const velox::RowTypePtr outputType_;
};

/// Operator for reading shuffle data written by MaterializedOutput.
///
/// Reads pages from ExchangeClient (via ShuffleExchangeSource), strips the
/// kFormatBatched prefix, and parses RowGroupHeader + TRowSize-framed
/// CompactRow data into RowVectors. Only handles batched format — no
/// kFormatRaw/legacy support (that's in ShuffleRead).
///
/// Deserializes CompactRow data directly via expandBatchedPage() using the
/// fixed RowGroupHeader + TRowSize framing, bypassing the VectorSerde
/// abstraction. This avoids VectorStreamGroup's column-by-column
/// deserialization overhead since the framing format is fixed and known
/// at compile time (written by MaterializedOutput).
class MaterializedExchange : public velox::exec::Exchange {
 public:
  static constexpr std::string_view kInputBatches =
      "materializedExchangeInputBatches";
  static constexpr std::string_view kTotalRows =
      "materializedExchangeTotalRows";

  MaterializedExchange(
      int32_t operatorId,
      velox::exec::DriverCtx* ctx,
      const std::shared_ptr<const MaterializedExchangeNode>&
          materializedExchangeNode,
      std::shared_ptr<velox::exec::ExchangeClient> exchangeClient);

  velox::RowVectorPtr getOutput() override;

  void close() override;

 private:
  // Not used — MaterializedExchange deserializes CompactRow directly via
  // expandBatchedPage(), bypassing the VectorSerde abstraction.
  velox::VectorSerde* getSerde() override {
    VELOX_UNSUPPORTED("MaterializedExchange doesn't use serde");
  }

  // Parse RowGroupHeader + TRowSize-framed CompactRow data written by
  // MaterializedOutput into row string_views. Operates directly on the framed
  // byte layout (RowGroupHeader followed by big-endian TRowSize + row bytes)
  // rather than going through VectorStreamGroup deserialization, since the
  // framing format is fixed and known at compile time.
  void expandBatchedPage(std::string_view pageData);

  // Clear accumulated page and row state after all rows are consumed.
  void resetOutputState();

  // Expand all current pages into row string_views.
  uint64_t parseCurrentPages();

  // Compute output batch size and deserialize rows into a RowVector.
  velox::RowVectorPtr deserializeNextBatch();

  // Cumulative stats.
  int64_t numInputBatches_{0};
  int64_t totalRows_{0};

  // Row parsing state — populated by parseCurrentPages(), consumed by
  // deserializeNextBatch(). Reset when all rows are consumed.
  std::vector<std::string_view> rows_;
  size_t nextRow_{0};
};

/// Translator that creates MaterializedExchange operators from
/// MaterializedExchangeNode.
class MaterializedExchangeTranslator
    : public velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<velox::exec::Operator> toOperator(
      velox::exec::DriverCtx* ctx,
      int32_t id,
      const velox::core::PlanNodePtr& node,
      std::shared_ptr<velox::exec::ExchangeClient> exchangeClient) override;
};

} // namespace facebook::presto::operators
