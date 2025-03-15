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

#include "velox/exec/HashTable.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec::test {

inline static const std::string kTestIndexConnectorName("IndexConnector");

/// The index table used for index lookup storage test. The data is stored in an
/// in-memory hash table, and the lookup is performed on the hash table using
/// probe and list join result similar as the hash probe operator does.
struct TestIndexTable {
  RowTypePtr keyType;
  RowTypePtr dataType;
  std::shared_ptr<BaseHashTable> table;

  TestIndexTable(
      RowTypePtr _keyType,
      RowTypePtr _dataType,
      std::shared_ptr<BaseHashTable> _table)
      : keyType(std::move(_keyType)),
        dataType(std::move(_dataType)),
        table(std::move(_table)) {}
};

/// The index table handle which provides the index table for index lookup.
class TestIndexTableHandle : public connector::ConnectorTableHandle {
 public:
  explicit TestIndexTableHandle(
      std::string connectorId,
      std::shared_ptr<TestIndexTable> indexTable,
      bool asyncLookup)
      : ConnectorTableHandle(std::move(connectorId)),
        indexTable_(std::move(indexTable)),
        asyncLookup_(asyncLookup) {}

  ~TestIndexTableHandle() override = default;

  std::string toString() const override {
    return fmt::format(
        "IndexTableHandle: num of rows: {}, asyncLookup: {}",
        indexTable_ ? indexTable_->table->rows()->numRows() : 0,
        asyncLookup_);
  }

  const std::string& name() const override {
    static const std::string kTableHandleName{"TestIndexTableHandle"};
    return kTableHandleName;
  }

  bool supportsIndexLookup() const override {
    return true;
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = name();
    obj["connectorId"] = connectorId();
    obj["asyncLookup"] = asyncLookup_;
    return obj;
  }

  static std::shared_ptr<TestIndexTableHandle> create(
      const folly::dynamic& obj,
      void* context) {
    // NOTE: this is only for testing purpose so we don't support to serialize
    // the table.
    return std::make_shared<TestIndexTableHandle>(
        obj["connectorId"].getString(), nullptr, obj["asyncLookup"].asBool());
  }

  static void registerSerDe() {
    auto& registry = DeserializationWithContextRegistryForSharedPtr();
    registry.Register("TestIndexTableHandle", create);
  }

  const std::shared_ptr<TestIndexTable>& indexTable() const {
    return indexTable_;
  }

  /// If true, we returns the lookup result asynchronously for testing purpose.
  bool asyncLookup() const {
    return asyncLookup_;
  }

 private:
  const std::shared_ptr<TestIndexTable> indexTable_;
  const bool asyncLookup_;
};

class TestIndexSource : public connector::IndexSource,
                        public std::enable_shared_from_this<TestIndexSource> {
 public:
  TestIndexSource(
      const RowTypePtr& inputType,
      const RowTypePtr& outputType,
      size_t numEqualJoinKeys,
      const core::TypedExprPtr& joinConditionExpr,
      const std::shared_ptr<TestIndexTableHandle>& tableHandle,
      connector::ConnectorQueryCtx* connectorQueryCtx,
      folly::Executor* executor);

  std::shared_ptr<LookupResultIterator> lookup(
      const LookupRequest& request) override;

  std::unordered_map<std::string, RuntimeMetric> runtimeStats() override;

  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  const std::shared_ptr<TestIndexTable>& indexTable() const {
    return tableHandle_->indexTable();
  }

  const RowTypePtr& outputType() const {
    return outputType_;
  }

  const std::vector<IdentityProjection>& outputProjections() const {
    return lookupOutputProjections_;
  }

  class ResultIterator : public LookupResultIterator {
   public:
    ResultIterator(
        std::shared_ptr<TestIndexSource> source,
        const LookupRequest& request,
        std::unique_ptr<HashLookup> lookupResult,
        folly::Executor* executor);

    std::optional<std::unique_ptr<LookupResult>> next(
        vector_size_t size,
        ContinueFuture& future) override;

   private:
    // Initializes the buffer used to store row pointers or indices for output
    // match result processing.
    template <typename T>
    void initBuffer(vector_size_t size, BufferPtr& buffer, T*& rawBuffer) {
      if (!buffer || !buffer->unique() ||
          buffer->capacity() < sizeof(T) * size) {
        buffer = AlignedBuffer::allocate<T>(size, source_->pool_.get(), T());
      }
      rawBuffer = buffer->asMutable<T>();
    }

    void evalJoinConditions();

    // Check if a given equality matched 'row' has passed join conditions.
    inline bool joinConditionPassed(vector_size_t row) const {
      return source_->conditionFilterInputRows_.isValid(row) &&
          !source_->decodedConditionFilterResult_.isNullAt(row) &&
          source_->decodedConditionFilterResult_.valueAt<bool>(row);
    }

    // Creates input vector for join condition evaluation.
    RowVectorPtr createConditionInput();

    // Extracts the lookup result columns from the index table and return in
    // 'result'.
    void extractLookupColumns(
        folly::Range<char* const*> rows,
        RowVectorPtr& result);

    // Inokved to trigger async lookup using background executor and return the
    // 'future'.
    void asyncLookup(vector_size_t size, ContinueFuture& future);

    // Synchronously lookup the index table and return up to 'size' number of
    // output rows in result.
    std::unique_ptr<LookupResult> syncLookup(vector_size_t size);

    const std::shared_ptr<TestIndexSource> source_;
    const LookupRequest request_;
    const std::unique_ptr<HashLookup> lookupResult_;
    folly::Executor* const executor_{nullptr};

    std::atomic_bool hasPendingRequest_{false};
    std::unique_ptr<BaseHashTable::JoinResultIterator> lookupResultIter_;
    std::optional<std::unique_ptr<LookupResult>> asyncResult_;

    // The reusable buffers for lookup result processing.
    // The input row number in lookup request for each matched result which is
    // paired with 'outputRowMapping_' to indicate if a given input row has
    // match or not. If the corresponding output row pointer in
    // 'outputRowMapping_' is null, then there is no match for the given input
    // row pointed by 'inputRowMapping_'.
    BufferPtr inputRowMapping_;
    vector_size_t* rawInputRowMapping_{nullptr};
    // Points to the matched row pointer in 'indexTable_' for each input row. If
    // there is a miss for a given input, then this is set to null.
    BufferPtr outputRowMapping_;
    char** rawOutputRowMapping_{nullptr};
    // The input row number in request for each output row in the returned
    // lookup result. Any gap in the input row numbers means the corresponding
    // input rows that has no matches in the index table.
    BufferPtr inputHitIndices_;
    vector_size_t* rawInputHitIndices_{nullptr};

    RowVectorPtr lookupOutput_;
  };

 private:
  // Invoked to check if this source has already encountered async lookup error,
  // and throws if it has.
  void checkNotFailed();

  // Initialize the output projections for lookup result processing.
  void initOutputProjections();

  // Initialize the condition filter input type and projections if configured.
  void initConditionProjections();

  void recordCpuTiming(const CpuWallTiming& timing);

  const std::shared_ptr<TestIndexTableHandle> tableHandle_;
  const RowTypePtr inputType_;
  const RowTypePtr outputType_;
  const RowTypePtr keyType_;
  const RowTypePtr valueType_;
  connector::ConnectorQueryCtx* const connectorQueryCtx_;
  const size_t numEqualJoinKeys_;
  const std::unique_ptr<exec::ExprSet> conditionExprSet_;
  const std::shared_ptr<memory::MemoryPool> pool_;
  folly::Executor* const executor_;

  mutable std::mutex mutex_;

  // Join condition filter input type.
  RowTypePtr conditionInputType_;

  // If not empty, set to the first encountered async error.
  std::string error_;

  // Reusable memory for join condition filter evaluation.
  VectorPtr conditionFilterResult_;
  DecodedVector decodedConditionFilterResult_;
  SelectivityVector conditionFilterInputRows_;
  // Column projections for join condition input and lookup output.
  std::vector<IdentityProjection> conditionInputProjections_;
  std::vector<IdentityProjection> conditionTableProjections_;
  std::vector<IdentityProjection> lookupOutputProjections_;
  std::unordered_map<std::string, RuntimeMetric> runtimeStats_;
};

class TestIndexConnector : public connector::Connector {
 public:
  TestIndexConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> config,
      folly::Executor* executor);

  bool supportsIndexLookup() const override {
    return true;
  }

  std::unique_ptr<connector::DataSource> createDataSource(
      const RowTypePtr&,
      const std::shared_ptr<connector::ConnectorTableHandle>&,
      const std::
          unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>&,
      connector::ConnectorQueryCtx*) override {
    VELOX_UNSUPPORTED("{} not implemented", __FUNCTION__);
  }

  std::shared_ptr<connector::IndexSource> createIndexSource(
      const RowTypePtr& inputType,
      size_t numJoinKeys,
      const std::vector<core::IndexLookupConditionPtr>& joinConditions,
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      connector::ConnectorQueryCtx* connectorQueryCtx) override;

  std::unique_ptr<connector::DataSink> createDataSink(
      RowTypePtr,
      std::shared_ptr<connector::ConnectorInsertTableHandle>,
      connector::ConnectorQueryCtx*,
      connector::CommitStrategy) override {
    VELOX_UNSUPPORTED("{} not implemented", __FUNCTION__);
  }

 private:
  folly::Executor* const executor_;
};

class TestIndexConnectorFactory : public connector::ConnectorFactory {
 public:
  TestIndexConnectorFactory()
      : ConnectorFactory(kTestIndexConnectorName.c_str()) {}

  std::shared_ptr<connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> properties,
      folly::Executor* /*unused*/,
      folly::Executor* cpuExecutor) override {
    return std::make_shared<TestIndexConnector>(id, properties, cpuExecutor);
  }
};
} // namespace facebook::velox::exec::test
