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
#include "presto_cpp/main/operators/ShuffleRead.h"
#include "velox/exec/Exchange.h"
#include "velox/row/CompactRow.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/RowSerializer.h"

using namespace facebook::velox::exec;
using namespace facebook::velox;
using facebook::velox::serializer::RowIteratorImpl;

namespace facebook::presto::operators {
velox::core::PlanNodeId deserializePlanNodeId(const folly::dynamic& obj) {
  return obj["id"].asString();
}

namespace {
std::unique_ptr<RowIterator> shuffleRowIteratorFactory(
    ByteInputStream* source,
    const VectorSerde::Options* /*unused*/) {
  return std::make_unique<RowIteratorImpl>(source, source->size());
}

class ShuffleVectorSerde : public VectorSerde {
 public:
  ShuffleVectorSerde() : VectorSerde(VectorSerde::Kind::kCompactRow) {}

  void estimateSerializedSize(
      const BaseVector* /* vector */,
      const folly::Range<const IndexRange*>& /* ranges */,
      vector_size_t** /* sizes */) override {
    // Not used.
    VELOX_UNREACHABLE();
  }

  std::unique_ptr<IterativeVectorSerializer> createIterativeSerializer(
      RowTypePtr /* type */,
      int32_t /* numRows */,
      StreamArena* /* streamArena */,
      const Options* /* options */) override {
    // Not used.
    VELOX_UNREACHABLE();
  }

  void deserialize(
      ByteInputStream* source,
      velox::memory::MemoryPool* pool,
      RowTypePtr type,
      RowVectorPtr* result,
      const Options* /* options */) override {
    VELOX_UNSUPPORTED("ShuffleVectorSerde::deserialize is not supported");
  }

  void deserialize(
      ByteInputStream* source,
      std::unique_ptr<RowIterator>& sourceRowIterator,
      uint64_t maxRows,
      RowTypePtr type,
      RowVectorPtr* result,
      velox::memory::MemoryPool* pool,
      const Options* options) override {
    std::vector<std::string_view> serializedRows;
    std::vector<std::unique_ptr<std::string>> serializedBuffers;
    velox::serializer::RowDeserializer<std::string_view>::deserialize(
        source,
        maxRows,
        sourceRowIterator,
        serializedRows,
        serializedBuffers,
        shuffleRowIteratorFactory,
        options);

    if (serializedRows.empty()) {
      *result = BaseVector::create<RowVector>(type, 0, pool);
      return;
    }

    *result = row::CompactRow::deserialize(serializedRows, type, pool);
  }
};

class ShuffleReadOperator : public Exchange {
 public:
  ShuffleReadOperator(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const ShuffleReadNode>& shuffleReadNode,
      std::shared_ptr<ExchangeClient> exchangeClient)
      : Exchange(
            operatorId,
            ctx,
            std::make_shared<core::ExchangeNode>(
                shuffleReadNode->id(),
                shuffleReadNode->outputType(),
                velox::VectorSerde::Kind::kCompactRow),
            exchangeClient,
            "ShuffleRead"),
        serde_(std::make_unique<ShuffleVectorSerde>()) {}

 protected:
  VectorSerde* getSerde() override {
    return serde_.get();
  }

 private:
  std::unique_ptr<ShuffleVectorSerde> serde_;
};
} // namespace

folly::dynamic ShuffleReadNode::serialize() const {
  auto obj = PlanNode::serialize();
  obj["outputType"] = outputType_->serialize();
  return obj;
}

velox::core::PlanNodePtr ShuffleReadNode::create(
    const folly::dynamic& obj,
    void* context) {
  return std::make_shared<ShuffleReadNode>(
      deserializePlanNodeId(obj),
      ISerializable::deserialize<RowType>(obj["outputType"], context));
}

std::unique_ptr<Operator> ShuffleReadTranslator::toOperator(
    DriverCtx* ctx,
    int32_t id,
    const core::PlanNodePtr& node,
    std::shared_ptr<ExchangeClient> exchangeClient) {
  if (auto shuffleReadNode =
          std::dynamic_pointer_cast<const ShuffleReadNode>(node)) {
    return std::make_unique<ShuffleReadOperator>(
        id, ctx, shuffleReadNode, exchangeClient);
  }
  return nullptr;
}
} // namespace facebook::presto::operators
