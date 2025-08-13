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
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/experimental/wave/dwio/nimble/tests/NimbleReaderTestUtil.h"
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

NimbleReaderFuzzer::NimbleReaderFuzzer(
    size_t initialSeed,
    NimbleReaderFuzzerOptions opts,
    std::unique_ptr<exec::test::ReferenceQueryRunner> referenceQueryRunner)
    : vectorFuzzer_{getFuzzerOptions(), pool_.get()},
      verifier_{std::move(referenceQueryRunner)},
      opts_(opts) {
  seed(initialSeed);
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
