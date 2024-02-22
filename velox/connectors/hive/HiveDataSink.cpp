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

#include "velox/connectors/hive/HiveDataSink.h"

#include "velox/common/base/Counters.h"
#include "velox/common/base/Fs.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/ITypedExpr.h"
#include "velox/dwio/common/SortingWriter.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/SortBuffer.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::hive {

namespace {

// Returns the type of non-partition data columns.
RowTypePtr getNonPartitionTypes(
    const std::vector<column_index_t>& dataCols,
    const RowTypePtr& inputType) {
  std::vector<std::string> childNames;
  std::vector<TypePtr> childTypes;
  const auto& dataSize = dataCols.size();
  childNames.reserve(dataSize);
  childTypes.reserve(dataSize);
  for (int dataCol : dataCols) {
    childNames.push_back(inputType->nameOf(dataCol));
    childTypes.push_back(inputType->childAt(dataCol));
  }

  return ROW(std::move(childNames), std::move(childTypes));
}

// Filters out partition columns if there is any.
RowVectorPtr makeDataInput(
    const std::vector<column_index_t>& dataCols,
    const RowVectorPtr& input) {
  std::vector<VectorPtr> childVectors;
  childVectors.reserve(dataCols.size());
  for (int dataCol : dataCols) {
    childVectors.push_back(input->childAt(dataCol));
  }

  return std::make_shared<RowVector>(
      input->pool(),
      getNonPartitionTypes(dataCols, asRowType(input->type())),
      input->nulls(),
      input->size(),
      std::move(childVectors),
      input->getNullCount());
}

// Returns a subset of column indices corresponding to partition keys.
std::vector<column_index_t> getPartitionChannels(
    const std::shared_ptr<const HiveInsertTableHandle>& insertTableHandle) {
  std::vector<column_index_t> channels;

  for (column_index_t i = 0; i < insertTableHandle->inputColumns().size();
       i++) {
    if (insertTableHandle->inputColumns()[i]->isPartitionKey()) {
      channels.push_back(i);
    }
  }

  return channels;
}

// Returns the column indices of non-partition data columns.
std::vector<column_index_t> getNonPartitionChannels(
    const std::vector<column_index_t>& partitionChannels,
    const column_index_t childrenSize) {
  std::vector<column_index_t> dataChannels;
  dataChannels.reserve(childrenSize - partitionChannels.size());

  for (column_index_t i = 0; i < childrenSize; i++) {
    if (std::find(partitionChannels.cbegin(), partitionChannels.cend(), i) ==
        partitionChannels.cend()) {
      dataChannels.push_back(i);
    }
  }

  return dataChannels;
}

std::string makePartitionDirectory(
    const std::string& tableDirectory,
    const std::optional<std::string>& partitionSubdirectory) {
  if (partitionSubdirectory.has_value()) {
    return fs::path(tableDirectory) / partitionSubdirectory.value();
  }
  return tableDirectory;
}

std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

std::unordered_map<LocationHandle::TableType, std::string> tableTypeNames() {
  return {
      {LocationHandle::TableType::kNew, "kNew"},
      {LocationHandle::TableType::kExisting, "kExisting"},
  };
}

template <typename K, typename V>
std::unordered_map<V, K> invertMap(const std::unordered_map<K, V>& mapping) {
  std::unordered_map<V, K> inverted;
  for (const auto& [key, value] : mapping) {
    inverted.emplace(value, key);
  }
  return inverted;
}

std::unique_ptr<core::PartitionFunction> createBucketFunction(
    const HiveBucketProperty& bucketProperty,
    const RowTypePtr& inputType) {
  const auto& bucketedBy = bucketProperty.bucketedBy();
  const auto& bucketedTypes = bucketProperty.bucketedTypes();
  std::vector<column_index_t> bucketedByChannels;
  bucketedByChannels.reserve(bucketedBy.size());
  for (int32_t i = 0; i < bucketedBy.size(); ++i) {
    const auto& bucketColumn = bucketedBy[i];
    const auto& bucketType = bucketedTypes[i];
    const auto inputChannel = inputType->getChildIdx(bucketColumn);
    if (FOLLY_UNLIKELY(
            !inputType->childAt(inputChannel)->equivalent(*bucketType))) {
      VELOX_USER_FAIL(
          "Input column {} type {} doesn't match bucket type {}",
          inputType->nameOf(inputChannel),
          inputType->childAt(inputChannel)->toString(),
          bucketType->toString());
    }
    bucketedByChannels.push_back(inputChannel);
  }
  return std::make_unique<HivePartitionFunction>(
      bucketProperty.bucketCount(), bucketedByChannels);
}

std::string computeBucketedFileName(
    const std::string& queryId,
    int32_t bucket) {
  static const uint32_t kMaxBucketCountPadding =
      std::to_string(HiveDataSink::maxBucketCount() - 1).size();
  const std::string bucketValueStr = std::to_string(bucket);
  return fmt::format(
      "0{:0>{}}_0_{}", bucketValueStr, kMaxBucketCountPadding, queryId);
}

std::shared_ptr<memory::MemoryPool> createSinkPool(
    const std::shared_ptr<memory::MemoryPool>& writerPool) {
  return writerPool->addLeafChild(fmt::format("{}.sink", writerPool->name()));
}

std::shared_ptr<memory::MemoryPool> createSortPool(
    const std::shared_ptr<memory::MemoryPool>& writerPool) {
  return writerPool->addLeafChild(fmt::format("{}.sort", writerPool->name()));
}

#define WRITER_NON_RECLAIMABLE_SECTION_GUARD(index)     \
  exec::NonReclaimableSectionGuard nonReclaimableGuard( \
      writerInfo_[(index)]->nonReclaimableSectionHolder.get())

} // namespace

const HiveWriterId& HiveWriterId::unpartitionedId() {
  static const HiveWriterId writerId{0};
  return writerId;
}

std::string HiveWriterId::toString() const {
  if (!partitionId.has_value()) {
    return "unpart";
  }
  if (bucketId.has_value()) {
    return fmt::format("part[{}.{}]", partitionId.value(), bucketId.value());
  }
  return fmt::format("part[{}]", partitionId.value());
}

const std::string LocationHandle::tableTypeName(
    LocationHandle::TableType type) {
  static const auto tableTypes = tableTypeNames();
  return tableTypes.at(type);
}

LocationHandle::TableType LocationHandle::tableTypeFromName(
    const std::string& name) {
  static const auto nameTableTypes = invertMap(tableTypeNames());
  return nameTableTypes.at(name);
}

HiveSortingColumn::HiveSortingColumn(
    const std::string& sortColumn,
    const core::SortOrder& sortOrder)
    : sortColumn_(sortColumn), sortOrder_(sortOrder) {
  VELOX_USER_CHECK(!sortColumn_.empty(), "hive sort column must be set");

  if (FOLLY_UNLIKELY(
          (sortOrder_.isAscending() && !sortOrder_.isNullsFirst()) ||
          (!sortOrder_.isAscending() && sortOrder_.isNullsFirst()))) {
    VELOX_USER_FAIL("Bad hive sort order: {}", toString());
  }
}

folly::dynamic HiveSortingColumn::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HiveSortingColumn";
  obj["columnName"] = sortColumn_;
  obj["sortOrder"] = sortOrder_.serialize();
  return obj;
}

std::shared_ptr<HiveSortingColumn> HiveSortingColumn::deserialize(
    const folly::dynamic& obj,
    void* context) {
  const std::string columnName = obj["columnName"].asString();
  const auto sortOrder = core::SortOrder::deserialize(obj["sortOrder"]);
  return std::make_shared<HiveSortingColumn>(columnName, sortOrder);
}

std::string HiveSortingColumn::toString() const {
  return fmt::format(
      "[COLUMN[{}] ORDER[{}]]", sortColumn_, sortOrder_.toString());
}

void HiveSortingColumn::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("HiveSortingColumn", HiveSortingColumn::deserialize);
}

HiveBucketProperty::HiveBucketProperty(
    Kind kind,
    int32_t bucketCount,
    const std::vector<std::string>& bucketedBy,
    const std::vector<TypePtr>& bucketTypes,
    const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortedBy)
    : kind_(kind),
      bucketCount_(bucketCount),
      bucketedBy_(bucketedBy),
      bucketTypes_(bucketTypes),
      sortedBy_(sortedBy) {
  validate();
}

void HiveBucketProperty::validate() const {
  VELOX_USER_CHECK_GT(bucketCount_, 0, "Hive bucket count can't be zero");
  VELOX_USER_CHECK(!bucketedBy_.empty(), "Hive bucket columns must be set");
  VELOX_USER_CHECK_EQ(
      bucketedBy_.size(),
      bucketTypes_.size(),
      "The number of hive bucket columns and types do not match {}",
      toString());
}

std::string HiveBucketProperty::kindString(Kind kind) {
  switch (kind) {
    case Kind::kHiveCompatible:
      return "HIVE_COMPATIBLE";
    case Kind::kPrestoNative:
      return "PRESTO_NATIVE";
    default:
      return fmt::format("UNKNOWN {}", static_cast<int>(kind));
  }
}

folly::dynamic HiveBucketProperty::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HiveBucketProperty";
  obj["kind"] = static_cast<int64_t>(kind_);
  obj["bucketCount"] = bucketCount_;
  obj["bucketedBy"] = ISerializable::serialize(bucketedBy_);
  obj["bucketedTypes"] = ISerializable::serialize(bucketTypes_);
  obj["sortedBy"] = ISerializable::serialize(sortedBy_);
  return obj;
}

std::shared_ptr<HiveBucketProperty> HiveBucketProperty::deserialize(
    const folly::dynamic& obj,
    void* context) {
  const Kind kind = static_cast<Kind>(obj["kind"].asInt());
  const int32_t bucketCount = obj["bucketCount"].asInt();
  const auto buckectedBy =
      ISerializable::deserialize<std::vector<std::string>>(obj["bucketedBy"]);
  const auto bucketedTypes = ISerializable::deserialize<std::vector<Type>>(
      obj["bucketedTypes"], context);
  const auto sortedBy =
      ISerializable::deserialize<std::vector<HiveSortingColumn>>(
          obj["sortedBy"], context);
  return std::make_shared<HiveBucketProperty>(
      kind, bucketCount, buckectedBy, bucketedTypes, sortedBy);
}

void HiveBucketProperty::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("HiveBucketProperty", HiveBucketProperty::deserialize);
}

std::string HiveBucketProperty::toString() const {
  std::stringstream out;
  out << "\nHiveBucketProperty[<" << kind_ << " " << bucketCount_ << ">\n";
  out << "\tBucket Columns:\n";
  for (const auto& column : bucketedBy_) {
    out << "\t\t" << column << "\n";
  }
  out << "\tBucket Types:\n";
  for (const auto& type : bucketTypes_) {
    out << "\t\t" << type->toString() << "\n";
  }
  if (!sortedBy_.empty()) {
    out << "\tSortedBy Columns:\n";
    for (const auto& sortColum : sortedBy_) {
      out << "\t\t" << sortColum->toString() << "\n";
    }
  }
  out << "]\n";
  return out.str();
}

HiveDataSink::HiveDataSink(
    RowTypePtr inputType,
    std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy,
    const std::shared_ptr<const HiveConfig>& hiveConfig)
    : inputType_(std::move(inputType)),
      insertTableHandle_(std::move(insertTableHandle)),
      connectorQueryCtx_(connectorQueryCtx),
      commitStrategy_(commitStrategy),
      hiveConfig_(hiveConfig),
      maxOpenWriters_(hiveConfig_->maxPartitionsPerWriters(
          connectorQueryCtx->sessionProperties())),
      partitionChannels_(getPartitionChannels(insertTableHandle_)),
      partitionIdGenerator_(
          !partitionChannels_.empty()
              ? std::make_unique<PartitionIdGenerator>(
                    inputType_,
                    partitionChannels_,
                    maxOpenWriters_,
                    connectorQueryCtx_->memoryPool(),
                    hiveConfig_->isPartitionPathAsLowerCase(
                        connectorQueryCtx->sessionProperties()))
              : nullptr),
      dataChannels_(
          getNonPartitionChannels(partitionChannels_, inputType_->size())),
      bucketCount_(
          insertTableHandle_->bucketProperty() == nullptr
              ? 0
              : insertTableHandle_->bucketProperty()->bucketCount()),
      bucketFunction_(
          isBucketed() ? createBucketFunction(
                             *insertTableHandle_->bucketProperty(),
                             inputType_)
                       : nullptr),
      writerFactory_(dwio::common::getWriterFactory(
          insertTableHandle_->tableStorageFormat())),
      spillConfig_(connectorQueryCtx->spillConfig()) {
  VELOX_USER_CHECK(
      !isBucketed() || isPartitioned(), "A bucket table must be partitioned");
  if (isBucketed()) {
    VELOX_USER_CHECK_LT(
        bucketCount_, maxBucketCount(), "bucketCount exceeds the limit");
  }
  VELOX_USER_CHECK(
      (commitStrategy_ == CommitStrategy::kNoCommit) ||
          (commitStrategy_ == CommitStrategy::kTaskCommit),
      "Unsupported commit strategy: {}",
      commitStrategyToString(commitStrategy_));

  if (!isBucketed()) {
    return;
  }
  const auto& sortedProperty = insertTableHandle_->bucketProperty()->sortedBy();
  if (!sortedProperty.empty()) {
    sortColumnIndices_.reserve(sortedProperty.size());
    sortCompareFlags_.reserve(sortedProperty.size());
    for (int i = 0; i < sortedProperty.size(); ++i) {
      auto columnIndex =
          getNonPartitionTypes(dataChannels_, inputType_)
              ->getChildIdxIfExists(sortedProperty.at(i)->sortColumn());
      if (columnIndex.has_value()) {
        sortColumnIndices_.push_back(columnIndex.value());
        sortCompareFlags_.push_back(
            {sortedProperty.at(i)->sortOrder().isNullsFirst(),
             sortedProperty.at(i)->sortOrder().isAscending(),
             false,
             CompareFlags::NullHandlingMode::kNullAsValue});
      }
    }
  }
}

bool HiveDataSink::canReclaim() const {
  // Currently, we only support memory reclaim on dwrf file writer.
  return (spillConfig_ != nullptr) &&
      (insertTableHandle_->tableStorageFormat() ==
       dwio::common::FileFormat::DWRF);
}

void HiveDataSink::appendData(RowVectorPtr input) {
  checkRunning();

  // Write to unpartitioned table.
  if (!isPartitioned()) {
    const auto index = ensureWriter(HiveWriterId::unpartitionedId());
    write(index, input);
    return;
  }

  // Write to partitioned table.
  computePartitionAndBucketIds(input);

  // Lazy load all the input columns.
  for (column_index_t i = 0; i < input->childrenSize(); ++i) {
    input->childAt(i)->loadedVector();
  }

  // All inputs belong to a single non-bucketed partition. The partition id
  // must be zero.
  if (!isBucketed() && partitionIdGenerator_->numPartitions() == 1) {
    const auto index = ensureWriter(HiveWriterId{0});
    write(index, input);
    return;
  }

  splitInputRowsAndEnsureWriters();

  for (auto index = 0; index < writers_.size(); ++index) {
    const vector_size_t partitionSize = partitionSizes_[index];
    if (partitionSize == 0) {
      continue;
    }

    RowVectorPtr writerInput = partitionSize == input->size()
        ? input
        : exec::wrap(partitionSize, partitionRows_[index], input);
    write(index, writerInput);
  }
}

void HiveDataSink::write(size_t index, RowVectorPtr input) {
  WRITER_NON_RECLAIMABLE_SECTION_GUARD(index);
  auto dataInput = makeDataInput(dataChannels_, input);

  writers_[index]->write(dataInput);
  writerInfo_[index]->numWrittenRows += dataInput->size();
}

std::string HiveDataSink::stateString(State state) {
  switch (state) {
    case State::kRunning:
      return "RUNNING";
    case State::kClosed:
      return "CLOSED";
    case State::kAborted:
      return "ABORTED";
    default:
      VELOX_UNREACHABLE("BAD STATE: {}", static_cast<int>(state));
  }
}

void HiveDataSink::computePartitionAndBucketIds(const RowVectorPtr& input) {
  VELOX_CHECK(isPartitioned());
  partitionIdGenerator_->run(input, partitionIds_);
  if (isBucketed()) {
    bucketFunction_->partition(*input, bucketIds_);
  }
}

DataSink::Stats HiveDataSink::stats() const {
  Stats stats;
  if (state_ == State::kAborted) {
    return stats;
  }

  int64_t numWrittenBytes{0};
  for (const auto& ioStats : ioStats_) {
    numWrittenBytes += ioStats->rawBytesWritten();
  }
  stats.numWrittenBytes = numWrittenBytes;

  if (state_ != State::kClosed) {
    return stats;
  }

  stats.numWrittenFiles = writers_.size();
  for (int i = 0; i < writerInfo_.size(); ++i) {
    const auto& info = writerInfo_.at(i);
    VELOX_CHECK_NOT_NULL(info);
    if (!info->spillStats->empty()) {
      stats.spillStats += *info->spillStats;
    }
  }
  return stats;
}

std::shared_ptr<memory::MemoryPool> HiveDataSink::createWriterPool(
    const HiveWriterId& writerId) {
  auto* connectorPool = connectorQueryCtx_->connectorMemoryPool();
  return connectorPool->addAggregateChild(
      fmt::format("{}.{}", connectorPool->name(), writerId.toString()));
}

void HiveDataSink::setMemoryReclaimers(HiveWriterInfo* writerInfo) {
  auto* connectorPool = connectorQueryCtx_->connectorMemoryPool();
  if (connectorPool->reclaimer() == nullptr) {
    return;
  }
  writerInfo->writerPool->setReclaimer(
      WriterReclaimer::create(this, writerInfo));
  writerInfo->sinkPool->setReclaimer(exec::MemoryReclaimer::create());
  // NOTE: we set the memory reclaimer for sort pool when we construct the sort
  // writer.
}

void HiveDataSink::setState(State newState) {
  checkStateTransition(state_, newState);
  state_ = newState;
}

/// Validates the state transition from 'oldState' to 'newState'.
void HiveDataSink::checkStateTransition(State oldState, State newState) {
  switch (oldState) {
    case State::kRunning:
      if (newState == State::kAborted || newState == State::kClosed) {
        return;
      }
      break;
    case State::kAborted:
      [[fallthrough]];
    case State::kClosed:
      [[fallthrough]];
    default:
      break;
  }
  VELOX_FAIL("Unexpected state transition from {} to {}", oldState, newState);
}

std::vector<std::string> HiveDataSink::close() {
  checkRunning();
  state_ = State::kClosed;
  closeInternal();

  std::vector<std::string> partitionUpdates;
  partitionUpdates.reserve(writerInfo_.size());
  for (int i = 0; i < writerInfo_.size(); ++i) {
    const auto& info = writerInfo_.at(i);
    VELOX_CHECK_NOT_NULL(info);
    // clang-format off
      auto partitionUpdateJson = folly::toJson(
       folly::dynamic::object
          ("name", info->writerParameters.partitionName().value_or(""))
          ("updateMode",
            HiveWriterParameters::updateModeToString(
              info->writerParameters.updateMode()))
          ("writePath", info->writerParameters.writeDirectory())
          ("targetPath", info->writerParameters.targetDirectory())
          ("fileWriteInfos", folly::dynamic::array(
            folly::dynamic::object
              ("writeFileName", info->writerParameters.writeFileName())
              ("targetFileName", info->writerParameters.targetFileName())
              ("fileSize", ioStats_.at(i)->rawBytesWritten())))
          ("rowCount", info->numWrittenRows)
         // TODO(gaoge): track and send the fields when inMemoryDataSizeInBytes
         // and containsNumberedFileNames are needed at coordinator when file_renaming_enabled are turned on.
          ("inMemoryDataSizeInBytes", 0)
          ("onDiskDataSizeInBytes", ioStats_.at(i)->rawBytesWritten())
          ("containsNumberedFileNames", true));
    // clang-format on
    partitionUpdates.push_back(partitionUpdateJson);
  }
  return partitionUpdates;
}

void HiveDataSink::abort() {
  checkRunning();
  state_ = State::kAborted;
  closeInternal();
}

void HiveDataSink::closeInternal() {
  VELOX_CHECK_NE(state_, State::kRunning);

  TestValue::adjust(
      "facebook::velox::connector::hive::HiveDataSink::closeInternal", this);

  if (state_ == State::kClosed) {
    for (int i = 0; i < writers_.size(); ++i) {
      WRITER_NON_RECLAIMABLE_SECTION_GUARD(i);
      writers_[i]->close();
    }
  } else {
    for (int i = 0; i < writers_.size(); ++i) {
      WRITER_NON_RECLAIMABLE_SECTION_GUARD(i);
      writers_[i]->abort();
    }
  }
}

uint32_t HiveDataSink::ensureWriter(const HiveWriterId& id) {
  auto it = writerIndexMap_.find(id);
  if (it != writerIndexMap_.end()) {
    return it->second;
  }
  return appendWriter(id);
}

uint32_t HiveDataSink::appendWriter(const HiveWriterId& id) {
  // Check max open writers.
  VELOX_USER_CHECK_LE(
      writers_.size(), maxOpenWriters_, "Exceeded open writer limit");
  VELOX_CHECK_EQ(writers_.size(), writerInfo_.size());
  VELOX_CHECK_EQ(writerIndexMap_.size(), writerInfo_.size());

  std::optional<std::string> partitionName;
  if (isPartitioned()) {
    partitionName =
        partitionIdGenerator_->partitionName(id.partitionId.value());
  }

  // Without explicitly setting flush policy, the default memory based flush
  // policy is used.
  auto writerParameters = getWriterParameters(partitionName, id.bucketId);
  const auto writePath = fs::path(writerParameters.writeDirectory()) /
      writerParameters.writeFileName();
  auto writerPool = createWriterPool(id);
  auto sinkPool = createSinkPool(writerPool);
  std::shared_ptr<memory::MemoryPool> sortPool{nullptr};
  if (sortWrite()) {
    sortPool = createSortPool(writerPool);
  }
  writerInfo_.emplace_back(std::make_shared<HiveWriterInfo>(
      std::move(writerParameters),
      std::move(writerPool),
      std::move(sinkPool),
      std::move(sortPool)));
  setMemoryReclaimers(writerInfo_.back().get());

  dwio::common::WriterOptions options;
  const auto* connectorSessionProperties =
      connectorQueryCtx_->sessionProperties();
  options.schema = getNonPartitionTypes(dataChannels_, inputType_);

  options.memoryPool = writerInfo_.back()->writerPool.get();
  options.compressionKind = insertTableHandle_->compressionKind();
  if (canReclaim()) {
    options.spillConfig = spillConfig_;
  }
  options.nonReclaimableSection =
      writerInfo_.back()->nonReclaimableSectionHolder.get();
  options.maxStripeSize = std::optional(
      hiveConfig_->orcWriterMaxStripeSize(connectorSessionProperties));
  options.maxDictionaryMemory = std::optional(
      hiveConfig_->orcWriterMaxDictionaryMemory(connectorSessionProperties));
  options.serdeParameters = std::map<std::string, std::string>(
      insertTableHandle_->serdeParameters().begin(),
      insertTableHandle_->serdeParameters().end());
  ioStats_.emplace_back(std::make_shared<io::IoStatistics>());

  // Prevents the memory allocation during the writer creation.
  WRITER_NON_RECLAIMABLE_SECTION_GUARD(writerInfo_.size() - 1);
  auto writer = writerFactory_->createWriter(
      dwio::common::FileSink::create(
          writePath,
          {.bufferWrite = false,
           .connectorProperties = hiveConfig_->config(),
           .fileCreateConfig = hiveConfig_->writeFileCreateConfig(),
           .pool = writerInfo_.back()->sinkPool.get(),
           .metricLogger = dwio::common::MetricsLog::voidLog(),
           .stats = ioStats_.back().get()}),
      options);
  writer = maybeCreateBucketSortWriter(std::move(writer));
  writers_.emplace_back(std::move(writer));
  // Extends the buffer used for partition rows calculations.
  partitionSizes_.emplace_back(0);
  partitionRows_.emplace_back(nullptr);
  rawPartitionRows_.emplace_back(nullptr);

  writerIndexMap_.emplace(id, writers_.size() - 1);
  return writerIndexMap_[id];
}

std::unique_ptr<facebook::velox::dwio::common::Writer>
HiveDataSink::maybeCreateBucketSortWriter(
    std::unique_ptr<facebook::velox::dwio::common::Writer> writer) {
  if (!sortWrite()) {
    return writer;
  }
  auto* sortPool = writerInfo_.back()->sortPool.get();
  VELOX_CHECK_NOT_NULL(sortPool);
  auto sortBuffer = std::make_unique<exec::SortBuffer>(
      getNonPartitionTypes(dataChannels_, inputType_),
      sortColumnIndices_,
      sortCompareFlags_,
      sortPool,
      writerInfo_.back()->nonReclaimableSectionHolder.get(),
      spillConfig_);
  return std::make_unique<dwio::common::SortingWriter>(
      std::move(writer),
      std::move(sortBuffer),
      hiveConfig_->sortWriterMaxOutputRows(
          connectorQueryCtx_->sessionProperties()),
      hiveConfig_->sortWriterMaxOutputBytes(
          connectorQueryCtx_->sessionProperties()),
      writerInfo_.back()->spillStats.get());
}

void HiveDataSink::splitInputRowsAndEnsureWriters() {
  VELOX_CHECK(isPartitioned());
  if (isBucketed()) {
    VELOX_CHECK_EQ(bucketIds_.size(), partitionIds_.size());
  }
  std::fill(partitionSizes_.begin(), partitionSizes_.end(), 0);

  const auto numRows = partitionIds_.size();
  for (auto row = 0; row < numRows; ++row) {
    VELOX_CHECK_LT(partitionIds_[row], std::numeric_limits<uint32_t>::max());
    const uint32_t partitionId = static_cast<uint32_t>(partitionIds_[row]);
    const auto id = isBucketed() ? HiveWriterId{partitionId, bucketIds_[row]}
                                 : HiveWriterId{partitionId};
    const uint32_t index = ensureWriter(id);
    VELOX_DCHECK_LT(index, partitionSizes_.size());
    VELOX_DCHECK_EQ(partitionSizes_.size(), partitionRows_.size());
    VELOX_DCHECK_EQ(partitionRows_.size(), rawPartitionRows_.size());
    if (FOLLY_UNLIKELY(partitionRows_[index] == nullptr) ||
        (partitionRows_[index]->capacity() < numRows * sizeof(vector_size_t))) {
      partitionRows_[index] =
          allocateIndices(numRows, connectorQueryCtx_->memoryPool());
      rawPartitionRows_[index] =
          partitionRows_[index]->asMutable<vector_size_t>();
    }
    rawPartitionRows_[index][partitionSizes_[index]] = row;
    ++partitionSizes_[index];
  }

  for (uint32_t i = 0; i < partitionSizes_.size(); ++i) {
    if (partitionSizes_[i] != 0) {
      VELOX_CHECK_NOT_NULL(partitionRows_[i]);
      partitionRows_[i]->setSize(partitionSizes_[i] * sizeof(vector_size_t));
    }
  }
}

HiveWriterParameters HiveDataSink::getWriterParameters(
    const std::optional<std::string>& partition,
    std::optional<uint32_t> bucketId) const {
  const auto updateMode = getUpdateMode();

  auto [targetFileName, writeFileName] = getWriterFileNames(bucketId);

  return HiveWriterParameters{
      updateMode,
      partition,
      targetFileName,
      makePartitionDirectory(
          insertTableHandle_->locationHandle()->targetPath(), partition),
      writeFileName,
      makePartitionDirectory(
          insertTableHandle_->locationHandle()->writePath(), partition)};
}

std::pair<std::string, std::string> HiveDataSink::getWriterFileNames(
    std::optional<uint32_t> bucketId) const {
  std::string targetFileName;
  if (bucketId.has_value()) {
    // TODO: add hive.file_renaming_enabled support.
    targetFileName = computeBucketedFileName(
        connectorQueryCtx_->queryId(), bucketId.value());
  } else {
    // targetFileName includes planNodeId and Uuid. As a result, different
    // table writers run by the same task driver or the same table writer
    // run in different task tries would have different targetFileNames.
    targetFileName = fmt::format(
        "{}_{}_{}_{}",
        connectorQueryCtx_->taskId(),
        connectorQueryCtx_->driverId(),
        connectorQueryCtx_->planNodeId(),
        makeUuid());
  }
  const std::string writeFileName = isCommitRequired()
      ? fmt::format(".tmp.velox.{}_{}", targetFileName, makeUuid())
      : targetFileName;
  if (insertTableHandle_->tableStorageFormat() ==
      dwio::common::FileFormat::PARQUET) {
    return {
        fmt::format("{}{}", targetFileName, ".parquet"),
        fmt::format("{}{}", writeFileName, ".parquet")};
  }
  return {targetFileName, writeFileName};
}

HiveWriterParameters::UpdateMode HiveDataSink::getUpdateMode() const {
  if (insertTableHandle_->isInsertTable()) {
    if (insertTableHandle_->isPartitioned()) {
      const auto insertBehavior = hiveConfig_->insertExistingPartitionsBehavior(
          connectorQueryCtx_->sessionProperties());
      switch (insertBehavior) {
        case HiveConfig::InsertExistingPartitionsBehavior::kOverwrite:
          return HiveWriterParameters::UpdateMode::kOverwrite;
        case HiveConfig::InsertExistingPartitionsBehavior::kError:
          return HiveWriterParameters::UpdateMode::kNew;
        default:
          VELOX_UNSUPPORTED(
              "Unsupported insert existing partitions behavior: {}",
              HiveConfig::insertExistingPartitionsBehaviorString(
                  insertBehavior));
      }
    } else {
      if (insertTableHandle_->isBucketed()) {
        VELOX_USER_FAIL("Cannot insert into bucketed unpartitioned Hive table");
      }
      if (hiveConfig_->immutablePartitions()) {
        VELOX_USER_FAIL("Unpartitioned Hive tables are immutable.");
      }
      return HiveWriterParameters::UpdateMode::kAppend;
    }
  } else {
    return HiveWriterParameters::UpdateMode::kNew;
  }
}

bool HiveInsertTableHandle::isPartitioned() const {
  return std::any_of(
      inputColumns_.begin(), inputColumns_.end(), [](auto column) {
        return column->isPartitionKey();
      });
}

const HiveBucketProperty* HiveInsertTableHandle::bucketProperty() const {
  return bucketProperty_.get();
}

bool HiveInsertTableHandle::isBucketed() const {
  return bucketProperty() != nullptr;
}

bool HiveInsertTableHandle::isInsertTable() const {
  return locationHandle_->tableType() == LocationHandle::TableType::kExisting;
}

folly::dynamic HiveInsertTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HiveInsertTableHandle";
  folly::dynamic arr = folly::dynamic::array;
  for (const auto& ic : inputColumns_) {
    arr.push_back(ic->serialize());
  }

  obj["inputColumns"] = arr;
  obj["locationHandle"] = locationHandle_->serialize();
  return obj;
}

HiveInsertTableHandlePtr HiveInsertTableHandle::create(
    const folly::dynamic& obj) {
  auto inputColumns = ISerializable::deserialize<std::vector<HiveColumnHandle>>(
      obj["inputColumns"]);
  auto locationHandle =
      ISerializable::deserialize<LocationHandle>(obj["locationHandle"]);
  return std::make_shared<HiveInsertTableHandle>(inputColumns, locationHandle);
}

void HiveInsertTableHandle::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("HiveInsertTableHandle", HiveInsertTableHandle::create);
}

std::string HiveInsertTableHandle::toString() const {
  std::ostringstream out;
  out << "HiveInsertTableHandle [inputColumns: [";
  for (const auto& i : inputColumns_) {
    out << " " << i->toString();
  }
  out << " ], locationHandle: " << locationHandle_->toString() << "]";
  return out.str();
}

std::string LocationHandle::toString() const {
  return fmt::format(
      "LocationHandle [targetPath: {}, writePath: {}, tableType: {},",
      targetPath_,
      writePath_,
      tableTypeName(tableType_));
}

void LocationHandle::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("LocationHandle", LocationHandle::create);
}

folly::dynamic LocationHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "LocationHandle";
  obj["targetPath"] = targetPath_;
  obj["writePath"] = writePath_;
  obj["tableType"] = tableTypeName(tableType_);
  return obj;
}

LocationHandlePtr LocationHandle::create(const folly::dynamic& obj) {
  auto targetPath = obj["targetPath"].asString();
  auto writePath = obj["writePath"].asString();
  auto tableType = tableTypeFromName(obj["tableType"].asString());
  return std::make_shared<LocationHandle>(targetPath, writePath, tableType);
}

std::unique_ptr<memory::MemoryReclaimer> HiveDataSink::WriterReclaimer::create(
    HiveDataSink* dataSink,
    HiveWriterInfo* writerInfo) {
  return std::unique_ptr<memory::MemoryReclaimer>(
      new HiveDataSink::WriterReclaimer(dataSink, writerInfo));
}

bool HiveDataSink::WriterReclaimer::reclaimableBytes(
    const memory::MemoryPool& pool,
    uint64_t& reclaimableBytes) const {
  VELOX_CHECK_EQ(pool.name(), writerInfo_->writerPool->name());
  reclaimableBytes = 0;
  if (!dataSink_->canReclaim()) {
    return false;
  }
  return exec::MemoryReclaimer::reclaimableBytes(pool, reclaimableBytes);
}

uint64_t HiveDataSink::WriterReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK_EQ(pool->name(), writerInfo_->writerPool->name());
  if (!dataSink_->canReclaim()) {
    return 0;
  }

  if (*writerInfo_->nonReclaimableSectionHolder.get()) {
    RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
    LOG(WARNING) << "Can't reclaim from hive writer pool " << pool->name()
                 << " which is under non-reclaimable section, "
                 << " used memory: " << succinctBytes(pool->currentBytes())
                 << ", reserved memory: "
                 << succinctBytes(pool->reservedBytes());
    ++stats.numNonReclaimableAttempts;
    return 0;
  }

  const uint64_t memoryUsageBeforeReclaim = pool->currentBytes();
  const std::string memoryUsageTreeBeforeReclaim = pool->treeMemoryUsage();
  const auto reclaimedBytes =
      exec::MemoryReclaimer::reclaim(pool, targetBytes, maxWaitMs, stats);
  const uint64_t memoryUsageAfterReclaim = pool->currentBytes();
  if (memoryUsageAfterReclaim > memoryUsageBeforeReclaim) {
    VELOX_FAIL(
        "Unexpected memory growth after memory reclaim from {}, the memory usage before reclaim: {}, after reclaim: {}\nThe memory tree usage before reclaim:\n{}\nThe memory tree usage after reclaim:\n{}",
        pool->name(),
        succinctBytes(memoryUsageBeforeReclaim),
        succinctBytes(memoryUsageAfterReclaim),
        memoryUsageTreeBeforeReclaim,
        pool->treeMemoryUsage());
  }
  return reclaimedBytes;
}
} // namespace facebook::velox::connector::hive
