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

#include "velox/experimental/wave/dwio/nimble/tests/NimbleReaderTestUtil.h"
#include <numeric>
#include "dwio/nimble/common/tests/NimbleFileWriter.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/velox/VeloxWriterOptions.h"
#include "velox/common/file/File.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/experimental/wave/dwio/ColumnReader.h"
#include "velox/experimental/wave/dwio/StructColumnReader.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFileFormat.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFormatData.h"
#include "velox/experimental/wave/dwio/nimble/SelectiveStructColumnReader.h"

namespace facebook::velox::wave {
TestNimbleReader::TestNimbleReader(
    memory::MemoryPool* pool,
    RowVectorPtr input,
    std::vector<std::unique_ptr<StreamLoader>>& streamLoaders,
    const std::vector<FilterSpec>& filters)
    : pool_(pool), input_(input) {
  deviceArena_ = std::make_unique<GpuArena>(
      100000000, getDeviceAllocator(getDevice()), 400000000);

  std::vector<std::unique_ptr<NimbleChunkedStream>> chunkedStreams;
  for (int i = 0; i < streamLoaders.size(); i++) {
    auto& streamLoader = streamLoaders[i];
    if (streamLoader == nullptr) { // this is a null stream
      continue;
    }
    nonNullOutputChildrenIds_.push_back(i);
    auto chunkedStream =
        std::make_unique<NimbleChunkedStream>(*pool, streamLoader->getStream());
    VELOX_CHECK(chunkedStream->hasNext());
    chunkedStreams.emplace_back(std::move(chunkedStream));
  }

  if (chunkedStreams.empty()) { // only contain null streams
    return;
  }

  // Create non-null type and scan spec that only include non-null streams
  std::vector<std::string> nonNullNames;
  std::vector<TypePtr> nonNullTypes;
  const auto& inputRowType = input->type()->asRow();
  for (int32_t childId : nonNullOutputChildrenIds_) {
    nonNullNames.push_back(inputRowType.nameOf(childId));
    nonNullTypes.push_back(inputRowType.childAt(childId));
  }
  auto nonNullRowType = ROW(std::move(nonNullNames), std::move(nonNullTypes));

  // Set up scan spec and format parameters
  scanSpec_ = std::make_shared<common::ScanSpec>("root");
  scanSpec_->addAllChildFields(*nonNullRowType);

  // Apply filters to specific children if provided
  for (const auto& [childName, filter, keepValues] : filters) {
    auto* childSpec = scanSpec_->childByName(childName);
    if (childSpec != nullptr) {
      childSpec->setFilter(filter);
      childSpec->setProjectOut(keepValues);
    } else if (!filter->nullAllowed()) {
      earlyStop_ = true;
      return;
    }
  }
  auto requestedType = nonNullRowType;
  auto fileType = dwio::common::TypeWithId::create(requestedType);
  std::shared_ptr<dwio::common::TypeWithId> fileTypeShared =
      std::move(fileType);

  stripe_ = std::make_unique<NimbleStripe>(
      std::move(chunkedStreams), fileTypeShared, input->size());

  formatParams_ = std::make_unique<NimbleFormatParams>(*pool, stats_, *stripe_);
  reader_ = nimble::NimbleFormatReader::build(
      requestedType, fileTypeShared, *formatParams_, *scanSpec_, nullptr, true);

  // Set up wave stream and read stream
  auto arena = std::make_unique<GpuArena>(100000000, getAllocator(getDevice()));
  InstructionStatus instructionStatus;
  waveStream_ = std::make_unique<WaveStream>(
      std::move(arena),
      deviceArena(),
      reinterpret_cast<nimble::SelectiveStructColumnReader*>(reader_.get())
          ->getOperands(),
      &operandStateMap_,
      instructionStatus,
      0);

  readStream_ = std::make_unique<ReadStream>(
      reinterpret_cast<StructColumnReader*>(reader_.get()),
      *waveStream_,
      &ioStats_,
      fileInfo_);
}

RowVectorPtr TestNimbleReader::read() {
  if (earlyStop_) {
    return RowVector::createEmpty(input_->type(), pool_);
  }
  std::vector<int32_t> rowIds;
  rowIds.resize(input_->size());
  std::iota(rowIds.begin(), rowIds.end(), 0);
  RowSet rows(rowIds.data(), rowIds.size());
  ReadStream::launch(std::move(readStream_), 0, rows);

  std::vector<OperandId> operandIds;
  operandIds.resize(nonNullOutputChildrenIds_.size());
  std::iota(operandIds.begin(), operandIds.end(), 1);
  folly::Range<const int32_t*> operandIdRange(
      operandIds.data(), operandIds.size());
  std::vector<VectorPtr> nonNullOutputChildren;
  nonNullOutputChildren.resize(nonNullOutputChildrenIds_.size());
  auto numOutputValues = waveStream_->getOutput(
      0, *pool_, operandIdRange, nonNullOutputChildren.data());

  auto output = std::make_shared<RowVector>(
      input_->pool(),
      input_->type(),
      BufferPtr(nullptr),
      numOutputValues,
      std::vector<VectorPtr>(input_->childrenSize()));

  for (int i = 0; i < nonNullOutputChildrenIds_.size(); i++) {
    output->childAt(nonNullOutputChildrenIds_[i]) =
        std::move(nonNullOutputChildren[i]);
  }

  for (int i = 0; i < input_->childrenSize(); i++) {
    if (output->childAt(i) == nullptr) { // populate the null children
      output->childAt(i) = BaseVector::createNullConstant(
          input_->childAt(i)->type(), numOutputValues, pool_);
    }
  }

  return output;
}

void NimbleReaderVerifier::verifyWithQueryRunner(
    RowVectorPtr input,
    const std::vector<FilterSpec>& filters,
    RowVectorPtr actual) {
  std::string whereClause;
  for (int32_t i = 0; i < filters.size(); i++) {
    whereClause += filters[i].toString();
    if (i < filters.size() - 1) {
      whereClause += " AND ";
    }
  }

  auto plan =
      exec::test::PlanBuilder().values({input}).filter(whereClause).planNode();
  auto result = referenceQueryRunner_->execute(plan);
  VELOX_CHECK_NE(
      result.second, exec::test::ReferenceQueryErrorCode::kReferenceQueryFail);
  VELOX_CHECK(
      exec::test::assertEqualResults(
          result.first.value(), plan->outputType(), {actual}),
      "Velox and Reference results don't match");
}

void NimbleReaderVerifier::verify(
    RowVectorPtr input,
    const std::vector<FilterSpec>& filters,
    RowVectorPtr actual) {
  if (referenceQueryRunner_ != nullptr) {
    verifyWithQueryRunner(input, filters, actual);
  }

  if (filters.empty()) {
    VELOX_CHECK(
        exec::test::assertEqualResults({input}, {actual}),
        "Input and actual results don't match when no filters are applied");
    return;
  }

  // Create a ScanSpec for the root
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*input->type());

  // Apply filters to the appropriate columns
  for (const auto& filterSpec : filters) {
    auto* childSpec = scanSpec->childByName(filterSpec.name);
    if (childSpec != nullptr) {
      childSpec->setFilter(filterSpec.filter);
      childSpec->setProjectOut(filterSpec.keepValues);
    } else {
      VELOX_FAIL("Column '{}' not found in input schema", filterSpec.name);
    }
  }
  // Apply filters to get a bitmask of passing rows
  auto size = input->size();
  // Initialize result buffer with enough bits for all rows
  auto resultSize = bits::nwords(size);
  std::vector<uint64_t> resultBuffer(resultSize, ~0ULL);
  uint64_t* result = resultBuffer.data();
  scanSpec->applyFilter(*input, size, result);

  // Count selected rows
  vector_size_t selectedCount = 0;
  for (int i = 0; i < size; i++) {
    if (bits::isBitSet(result, i)) {
      selectedCount++;
    }
  }

  // Create expected output based on filter results
  RowVectorPtr expected;
  if (selectedCount == 0) {
    // No rows passed the filter, so create empty vector with same schema
    std::vector<VectorPtr> emptyChildren;
    emptyChildren.reserve(input->childrenSize());
    for (int i = 0; i < input->childrenSize(); i++) {
      emptyChildren.push_back(
          BaseVector::create(input->childAt(i)->type(), 0, input->pool()));
    }
    expected = std::make_shared<RowVector>(
        input->pool(),
        input->type(),
        BufferPtr(nullptr),
        0,
        std::move(emptyChildren));
  } else if (selectedCount == size) {
    // All rows passed the filter
    expected = input;
  } else {
    BufferPtr indices = allocateIndices(selectedCount, input->pool());
    auto* rawIndices = indices->asMutable<vector_size_t>();
    int idx = 0;
    for (int i = 0; i < size; i++) {
      if (bits::isBitSet(result, i)) {
        rawIndices[idx++] = i;
      }
    }

    // Create filtered children vectors
    std::vector<VectorPtr> filteredChildren;
    for (int i = 0; i < input->childrenSize(); i++) {
      auto* childSpec = scanSpec->childByName(input->type()->asRow().nameOf(i));

      if (childSpec && childSpec->projectOut()) {
        // This column should be projected out - wrap with dictionary
        filteredChildren.push_back(BaseVector::wrapInDictionary(
            nullptr, indices, selectedCount, input->childAt(i)));
      } else {
        // This column is not projected out - create null constant
        filteredChildren.push_back(BaseVector::createNullConstant(
            input->childAt(i)->type(), selectedCount, input->pool()));
      }
    }

    expected = std::make_shared<RowVector>(
        input->pool(),
        input->type(),
        BufferPtr(nullptr),
        selectedCount,
        std::move(filteredChildren));
  }

  VELOX_CHECK(
      exec::test::assertEqualResults({expected}, {actual}),
      "Expected and actual results don't match after applying filters");
}

RowVectorPtr createInputFromChunkVectorGroups(
    const std::vector<std::vector<VectorPtr>>& chunkVectorGroups) {
  std::vector<RowVectorPtr> groupVectors;
  for (const auto& chunkVectors : chunkVectorGroups) {
    if (chunkVectors.empty()) {
      continue;
    }

    auto groupVector = std::dynamic_pointer_cast<RowVector>(chunkVectors[0]);
    for (int i = 1; i < chunkVectors.size(); i++) {
      groupVector->append(chunkVectors[i].get());
    }
    groupVectors.push_back(groupVector);
  }

  if (groupVectors.empty()) {
    return nullptr; // No valid groups
  }

  if (groupVectors.size() == 1) {
    return groupVectors[0];
  } else {
    std::vector<std::string> allNames;
    std::vector<TypePtr> allTypes;
    std::vector<VectorPtr> allChildren;

    int32_t childId = 0;
    for (const auto& groupVector : groupVectors) {
      const auto& rowType = groupVector->type()->asRow();
      for (int i = 0; i < groupVector->childrenSize(); i++, childId++) {
        allNames.push_back("c" + std::to_string(childId));
        allTypes.push_back(rowType.childAt(i));
        allChildren.push_back(groupVector->childAt(i));
      }
    }

    auto combinedType = ROW(std::move(allNames), std::move(allTypes));
    return std::make_shared<RowVector>(
        groupVectors[0]->pool(),
        combinedType,
        BufferPtr(nullptr),
        groupVectors[0]->size(),
        std::move(allChildren));
  }
}

std::vector<std::unique_ptr<StreamLoader>> writeToNimbleAndGetStreamLoaders(
    memory::MemoryPool* pool,
    const std::vector<std::vector<VectorPtr>>& chunkVectorGroups,
    const std::vector<std::pair<EncodingType, float>>& readFactors,
    const CompressionOptions& compressionOptions) {
  using namespace facebook::nimble;

  // Configure writer options with the specified read factors and compression
  // options
  VeloxWriterOptions writerOptions;
  ManualEncodingSelectionPolicyFactory encodingFactory(
      readFactors, compressionOptions);
  writerOptions.encodingSelectionPolicyFactory = [&](DataType dataType) {
    return encodingFactory.createPolicy(dataType);
  };
  writerOptions.enableChunking = true;
  writerOptions.minStreamChunkRawSize = 0;
  writerOptions.flushPolicyFactory = [] {
    return std::make_unique<LambdaFlushPolicy>(
        [](const StripeProgress&) { return FlushDecision::Chunk; });
  };

  std::vector<std::unique_ptr<StreamLoader>> allStreamLoaders;

  for (const auto& chunkVectors : chunkVectorGroups) {
    if (chunkVectors.empty()) {
      continue;
    }

    auto file = facebook::nimble::test::createNimbleFile(
        *pool, chunkVectors, writerOptions, false);

    auto numChildren = chunkVectors[0]->as<RowVector>()->childrenSize();

    auto readFile = std::make_unique<InMemoryReadFile>(file);
    TabletReader tablet(*pool, std::move(readFile));
    auto stripeIdentifier = tablet.getStripeIdentifier(0);
    // VELOX_CHECK_EQ(numChildren + 1, tablet.streamCount(stripeIdentifier));

    std::vector<uint32_t> streamIds;
    streamIds.resize(numChildren);
    std::iota(streamIds.begin(), streamIds.end(), 1);
    auto streamLoaders =
        tablet.load(stripeIdentifier, std::span<const uint32_t>(streamIds));

    for (auto& loader : streamLoaders) {
      allStreamLoaders.push_back(std::move(loader));
    }
  }

  return allStreamLoaders;
}
} // namespace facebook::velox::wave
