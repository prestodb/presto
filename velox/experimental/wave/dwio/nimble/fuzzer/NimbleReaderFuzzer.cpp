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

#include "velox/experimental/wave/dwio/nimble/fuzzer/NimbleReaderFuzzer.h"
#include <cstddef>
#include <numeric>
#include "dwio/nimble/common/tests/NimbleFileWriter.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "dwio/nimble/velox/VeloxWriterOptions.h"
#include "velox/common/file/File.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/experimental/wave/dwio/ColumnReader.h"
#include "velox/experimental/wave/dwio/StructColumnReader.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFileFormat.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFormatData.h"
#include "velox/experimental/wave/dwio/nimble/SelectiveStructColumnReader.h"
#include "velox/type/Filter.h"

DEFINE_int32(steps, 10, "Number of plans to generate and test.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

namespace facebook::velox::wave {
namespace {
using namespace facebook::nimble;

struct FilterSpec {
  std::string name;
  std::shared_ptr<common::Filter> filter;
  bool keepValues;

  std::string toString() const {
    switch (filter->kind()) {
      case common::FilterKind::kBigintRange: {
        auto filter =
            std::dynamic_pointer_cast<common::BigintRange>(this->filter);
        return fmt::format(
            "{} BETWEEN {} AND {}", name, filter->lower(), filter->upper());
      }
      default:
        VELOX_NYI("Not supported filter type");
    }
  }
};

struct ChunkSpec {
  TypePtr type;
  int32_t numValues;
  double nullRatio;
};

struct StreamSpec {
  std::string name;
  std::vector<ChunkSpec> chunks;
  double nullRatio;

  vector_size_t numValues() const {
    vector_size_t numValues = 0;
    for (const auto& chunk : chunks) {
      numValues += chunk.numValues;
    }
    return numValues;
  }

  TypePtr type() const {
    return chunks.empty() ? nullptr : chunks[0].type;
  }
};

struct StripeSpec {
  std::vector<StreamSpec> streams;
  std::vector<FilterSpec> filters;

  void check() const {
    for (const auto& stream : streams) {
      VELOX_CHECK_EQ(stream.numValues(), numValues());
    }
  }

  vector_size_t numValues() const {
    return streams.empty() ? 0 : streams[0].numValues();
  }
};

class TestNimbleReader {
 public:
  TestNimbleReader(
      memory::MemoryPool* pool,
      RowVectorPtr input,
      std::vector<std::unique_ptr<StreamLoader>>& streamLoaders,
      const std::vector<FilterSpec>& filters);
  RowVectorPtr read();

 private:
  memory::MemoryPool* pool_{nullptr};
  RowVectorPtr input_{nullptr};
  std::unique_ptr<GpuArena> deviceArena_{nullptr};
  std::shared_ptr<common::ScanSpec> scanSpec_{nullptr};
  std::unique_ptr<NimbleStripe> stripe_{nullptr};
  std::unique_ptr<NimbleFormatParams> formatParams_{nullptr};
  std::unique_ptr<ColumnReader> reader_{nullptr};
  dwio::common::ColumnReaderStatistics stats_;
  io::IoStatistics ioStats_;
  FileInfo fileInfo_;
  OperatorStateMap operandStateMap_;
  std::unique_ptr<WaveStream> waveStream_{nullptr};
  std::unique_ptr<ReadStream> readStream_{nullptr};
  std::vector<int32_t> nonNullOutputChildrenIds_;
  bool earlyStop_{false};

  GpuArena& deviceArena() {
    return *deviceArena_;
  }
};

class NimbleReaderVerifier {
 public:
  explicit NimbleReaderVerifier(
      std::unique_ptr<exec::test::ReferenceQueryRunner> referenceQueryRunner)
      : referenceQueryRunner_{std::move(referenceQueryRunner)} {};

  void verify(
      RowVectorPtr input,
      const std::vector<FilterSpec>& filters,
      RowVectorPtr actual);

 private:
  std::unique_ptr<exec::test::ReferenceQueryRunner> referenceQueryRunner_;
  void verifyWithQueryRunner(
      RowVectorPtr input,
      const std::vector<FilterSpec>& filters,
      RowVectorPtr actual);
};

class NimbleReaderFuzzer {
 public:
  NimbleReaderFuzzer(
      size_t initialSeed,
      NimbleReaderFuzzerOptions opts,
      std::unique_ptr<exec::test::ReferenceQueryRunner> referenceQueryRunner);

  void go();

  void setOptions(const NimbleReaderFuzzerOptions& options) {
    opts_ = options;
  }

  const NimbleReaderFuzzerOptions& getOptions() {
    return opts_;
  }

  NimbleReaderFuzzerOptions& getMutableOptions() {
    return opts_;
  }

 private:
  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.allowSlice = false;
    opts.allowConstantVector = false;
    opts.allowDictionaryVector = false;
    return opts;
  }

  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(rng_());
  }

  void run();

  VectorPtr makeChunk(const ChunkSpec& spec);
  std::vector<VectorPtr> makeStream(const StreamSpec& spec);
  std::vector<std::vector<VectorPtr>> makeStripe(const StripeSpec& spec);
  StripeSpec generateStripeSpec();
  template <typename T>
  std::shared_ptr<common::Filter> generateOneFilter(VectorPtr input);
  std::vector<FilterSpec> generateFilters(RowVectorPtr input);
  std::vector<std::pair<EncodingType, float>> generateReadFactors();

  int32_t randInt(int32_t min, int32_t max) {
    return boost::random::uniform_int_distribution<int32_t>(min, max)(rng_);
  }

  double randDouble01() {
    return boost::random::uniform_01<double>()(rng_);
  }

  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool(
          "nimbleReaderFuzzer",
          memory::kMaxMemory,
          memory::MemoryReclaimer::create())};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild(
      "nimbleReaderFuzzerLeaf",
      true,
      memory::MemoryReclaimer::create())};

  VectorFuzzer vectorFuzzer_;
  NimbleReaderVerifier verifier_;

  struct Stats {
    // The total number of iterations tested.
    size_t numIterations{0};

    size_t numStreams{0};
    size_t numNullables{0};
    size_t numChunks{0};
    size_t numFilters{0};
    size_t numFiltersKeepValues{0};
    size_t numFiltersWithNullsAllowed{0};
    double selectivity{0.0};

    std::string toString() const {
      return fmt::format(
          "\nTotal iterations tested: {}\n"
          "Avg. number of streams: {}\n"
          "Avg. number of nullables: {}\n"
          "Avg. number of chunks: {}\n"
          "Avg. number of filters: {}\n"
          "Avg. number of filters with keep values: {}\n"
          "Avg. number of filters with nulls allowed: {}\n"
          "Avg. selectivity: {}\n",
          numIterations,
          numStreams / (double)numIterations,
          numNullables / (double)numIterations,
          numChunks / (double)numIterations,
          numFilters / (double)numIterations,
          numFiltersKeepValues / (double)numIterations,
          numFiltersWithNullsAllowed / (double)numIterations,
          selectivity / numIterations);
    }

    void add(
        const StripeSpec& stripe,
        const std::vector<FilterSpec>& filters,
        RowVectorPtr output) {
      numStreams += stripe.streams.size();
      std::for_each(
          stripe.streams.begin(),
          stripe.streams.end(),
          [&](const auto& stream) {
            numNullables += stream.nullRatio > 0 ? 1 : 0;
            numChunks += stream.chunks.size();
          });

      numFilters += filters.size();
      std::for_each(filters.begin(), filters.end(), [&](const auto& filter) {
        numFiltersKeepValues += filter.keepValues ? 1 : 0;
        numFiltersWithNullsAllowed += filter.filter->nullAllowed() ? 1 : 0;
      });

      if (output) {
        selectivity += output->size() / (double)stripe.numValues();
      }
    }
  };

  Stats stats_;
  NimbleReaderFuzzerOptions opts_;
};

bool isDone(
    size_t i,
    std::chrono::time_point<std::chrono::system_clock> startTime) {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

// Helper function to create input by combining chunk vector groups
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

// Helper function to write multiple vectors to nimble files and get back all
// streamLoaders. A chunk vector group contains several chunked vectors
// (mapping to chunked streams in Nimble) that share the same chunk
// boundaries.
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

NimbleReaderFuzzer::NimbleReaderFuzzer(
    size_t initialSeed,
    NimbleReaderFuzzerOptions opts,
    std::unique_ptr<exec::test::ReferenceQueryRunner> referenceQueryRunner)
    : vectorFuzzer_{getFuzzerOptions(), pool_.get()},
      verifier_{std::move(referenceQueryRunner)},
      opts_(opts) {
  seed(initialSeed);
}

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

void NimbleReaderFuzzer::go() {
  VELOX_USER_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.");

  const auto startTime = std::chrono::system_clock::now();

  while (!isDone(stats_.numIterations, startTime)) {
    LOG(INFO) << "==============================> Started iteration "
              << stats_.numIterations << " (seed: " << currentSeed_ << ")";

    run();

    LOG(INFO) << "==============================> Done with iteration "
              << stats_.numIterations;

    reSeed();
    ++stats_.numIterations;
  }
  LOG(INFO) << stats_.toString();
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

VectorPtr NimbleReaderFuzzer::makeChunk(const ChunkSpec& spec) {
  auto& option = vectorFuzzer_.getMutableOptions();
  option.vectorSize = spec.numValues;
  option.nullRatio = spec.nullRatio;
  auto vector = vectorFuzzer_.fuzzFlat(spec.type);
  auto tempType = vector->type();
  return std::make_shared<RowVector>(
      vector->pool(),
      ROW({""}, {tempType}),
      BufferPtr{nullptr},
      vector->size(),
      std::vector<VectorPtr>{vector});
}

std::vector<VectorPtr> NimbleReaderFuzzer::makeStream(const StreamSpec& spec) {
  std::vector<VectorPtr> chunkVectors;
  for (const auto& chunkSpec : spec.chunks) {
    chunkVectors.push_back(makeChunk(chunkSpec));
  }
  return chunkVectors;
}

std::vector<std::vector<VectorPtr>> NimbleReaderFuzzer::makeStripe(
    const StripeSpec& spec) {
  std::vector<std::vector<VectorPtr>> chunkVectorGroups;
  for (const auto& streamSpec : spec.streams) {
    chunkVectorGroups.push_back(makeStream(streamSpec));
  }
  return chunkVectorGroups;
}

template <typename T>
std::shared_ptr<common::Filter> NimbleReaderFuzzer::generateOneFilter(
    VectorPtr input) {
  vector_size_t endPointIdx1 = randInt(0, input->size() - 1);
  vector_size_t endPointIdx2 = randInt(0, input->size() - 1);
  bool nullAllowed = vectorFuzzer_.coinToss(opts_.nullAllowedProbability);
  auto endPoint1 = input->as<SimpleVector<T>>()->valueAt(endPointIdx1);
  auto endPoint2 = input->as<SimpleVector<T>>()->valueAt(endPointIdx2);
  auto lower = std::min(endPoint1, endPoint2);
  auto upper = std::max(endPoint1, endPoint2);
  return std::make_shared<common::BigintRange>(lower, upper, nullAllowed);
}

std::vector<FilterSpec> NimbleReaderFuzzer::generateFilters(
    RowVectorPtr input) {
  std::vector<FilterSpec> filters;
  bool hasFilter = vectorFuzzer_.coinToss(opts_.hasFilterProbability);
  if (!hasFilter) {
    return filters;
  }
  for (int i = 0; i < input->childrenSize(); i++) {
    if (!vectorFuzzer_.coinToss(opts_.isFilterProbability)) {
      continue;
    }
    auto kind = input->childAt(i)->typeKind();
    if (kind != TypeKind::BIGINT) {
      continue;
    }
    filters.emplace_back();
    auto& filterSpec = filters.back();
    filterSpec.name = input->type()->asRow().nameOf(i);
    filterSpec.keepValues =
        vectorFuzzer_.coinToss(opts_.filterKeepValuesProbability);
    switch (kind) {
      case TypeKind::BIGINT:
        filterSpec.filter = generateOneFilter<int64_t>(input->childAt(i));
        break;
      case TypeKind::INTEGER:
        filterSpec.filter = generateOneFilter<int32_t>(input->childAt(i));
        break;
      default:
        VELOX_UNREACHABLE();
    }
  }

  return filters;
}

StripeSpec NimbleReaderFuzzer::generateStripeSpec() {
  StripeSpec spec;
  const auto numStreams = randInt(opts_.minNumStreams, opts_.maxNumStreams);
  const auto numValues = randInt(opts_.minNumValues, opts_.maxNumValues);
  spec.streams.resize(numStreams);
  for (size_t i = 0; i < numStreams; i++) {
    auto& stream = spec.streams[i];
    stream.name = "c" + std::to_string(i);
    auto type = vectorFuzzer_.randType(opts_.types, 1);
    auto numChunks = randInt(opts_.minNumChunks, opts_.maxNumChunks);
    stream.chunks.resize(numChunks);
    bool hasNulls = vectorFuzzer_.coinToss(opts_.hasNullProbability);
    stream.nullRatio = hasNulls ? randDouble01() : 0;

    size_t numValuesLeft = numValues;
    for (size_t nthChunk = 0; nthChunk < numChunks; nthChunk++) {
      auto& chunk = stream.chunks[nthChunk];
      chunk.type = type;
      chunk.nullRatio = stream.nullRatio;

      if (nthChunk == numChunks - 1) {
        chunk.numValues = numValuesLeft;
      } else {
        chunk.numValues = randInt(
            opts_.minChunkSize,
            numValuesLeft - (numChunks - nthChunk - 1) * opts_.minChunkSize);
      }
      numValuesLeft -= chunk.numValues;
    }
  }

  spec.check();
  return spec;
}

std::vector<std::pair<EncodingType, float>>
NimbleReaderFuzzer::generateReadFactors() {
  return {
      {EncodingType::Nullable, 1.0}, // This enables both nullable and trivial
  };
}

void NimbleReaderFuzzer::run() {
  auto stripeSpec = generateStripeSpec();
  auto stripe = makeStripe(stripeSpec);

  auto streamLoaders = writeToNimbleAndGetStreamLoaders(
      pool_.get(),
      stripe,
      generateReadFactors(),
      CompressionOptions{.compressionAcceptRatio = 0.0});

  auto input = createInputFromChunkVectorGroups(stripe);
  auto filterSpecs = generateFilters(input);

  TestNimbleReader reader(pool_.get(), input, streamLoaders, filterSpecs);
  auto testResult = reader.read();

  stats_.add(stripeSpec, filterSpecs, testResult);
  verifier_.verify(input, filterSpecs, testResult);
}
} // namespace

void nimbleReaderFuzzer(
    size_t seed,
    NimbleReaderFuzzerOptions opts,
    std::unique_ptr<exec::test::ReferenceQueryRunner> referenceQueryRunner) {
  NimbleReaderFuzzer(seed, opts, std::move(referenceQueryRunner)).go();
}
} // namespace facebook::velox::wave
