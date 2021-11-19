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

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "velox/common/time/Timer.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/type/Filter.h"
#include "velox/type/Subfield.h"
#include "velox/vector/FlatVector.h"

#include "velox/dwio/common/MemoryInputStream.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/reader/SelectiveColumnReader.h"
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/dwio/dwrf/test/utils/MapBuilder.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"

DEFINE_int32(timing_repeats, 0, "Count of repeats for timing filter tests");

namespace facebook::velox::dwio::dwrf {
using namespace facebook::velox::test;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::dwio::type::fbhive;
using namespace facebook::velox;
using namespace facebook::velox::common;

using dwio::common::MemoryInputStream;
using dwio::common::MemorySink;
using velox::common::Subfield;

using SubfieldFilters = std::unordered_map<
    velox::common::Subfield,
    std::unique_ptr<velox::common::Filter>>;

// Encodes a batch number and an index into the batch into an int32_t
uint32_t batchPosition(uint32_t batchNumber, vector_size_t batchRow) {
  return batchNumber << 16 | batchRow;
}

static uint32_t batchNumber(uint32_t position) {
  return position >> 16;
}

static vector_size_t batchRow(uint32_t position) {
  return position & 0xffff;
}

VectorPtr getChildBySubfield(RowVector* rowVector, const Subfield& subfield) {
  auto& path = subfield.path();
  auto container = rowVector;
  for (int i = 0; i < path.size(); ++i) {
    auto nestedField =
        dynamic_cast<const Subfield::NestedField*>(path[i].get());
    VELOX_CHECK(nestedField, "Path does not consist of nested fields");

    auto rowType = container->type()->as<TypeKind::ROW>();
    auto child = rowVector->childAt(rowType.getChildIdx(nestedField->name()));

    if (i == path.size() - 1) {
      return child;
    }
    VELOX_CHECK(child->typeKind() == TypeKind::ROW);
    container = child->as<RowVector>();
  }
  // Never reached.
  VELOX_CHECK(false);
  return nullptr;
}

struct FilterSpec {
  std::string field;
  float startPct = 50;
  float selectPct = 20;
  FilterKind filterKind = FilterKind::kBigintRange;
  // If true, makes a filter that matches max value in the column so as to skip
  // row groups on min/max.
  bool isForRowGroupSkip{false};
};

class AbstractColumnStats {
 public:
  // ASCII string greater than test data values. Used for row group skipping
  // tests.
  static constexpr const char* kMaxString = "~~~~~";
  explicit AbstractColumnStats(TypePtr type) : type_(type) {}

  virtual ~AbstractColumnStats() = default;

  virtual void sample(
      const std::vector<RowVectorPtr>& batches,
      const Subfield& subfield,
      std::vector<uint32_t>& rows) = 0;

  virtual std::unique_ptr<velox::common::Filter> filter(
      float startPct,
      float selectPct,
      FilterKind filterKind,
      const std::vector<RowVectorPtr>& batches,
      const Subfield& subfield,
      std::vector<uint32_t>& hits) = 0;

  virtual std::unique_ptr<velox::common::Filter> rowGroupSkipFilter(
      const std::vector<RowVectorPtr>& /*batches*/,
      const Subfield& /*subfield*/,
      std::vector<uint32_t>& /*hits*/) {
    VELOX_NYI();
  }

 protected:
  const TypePtr type_;
  int32_t numDistinct_ = 0;
  int32_t numNulls_ = 0;
  int32_t numSamples_ = 0;
  std::unordered_set<size_t> uniques_;
  static uint32_t counter_;
};

uint32_t AbstractColumnStats::counter_ = 0;

template <typename T>
class ColumnStats : public AbstractColumnStats {
 public:
  explicit ColumnStats(TypePtr type) : AbstractColumnStats(type) {}

  void sample(
      const std::vector<RowVectorPtr>& batches,
      const Subfield& subfield,
      std::vector<uint32_t>& rows) override {
    int32_t previousBatch = -1;
    SimpleVector<T>* values = nullptr;
    for (auto row : rows) {
      auto batch = batchNumber(row);
      if (batch != previousBatch) {
        previousBatch = batch;
        auto vector = batches[batch];
        values = getChildBySubfield(vector.get(), subfield)
                     ->asUnchecked<SimpleVector<T>>();
      }

      addSample(values, batchRow(row));
    }
    std::sort(values_.begin(), values_.end());
  }

  std::unique_ptr<velox::common::Filter> filter(
      float startPct,
      float selectPct,
      FilterKind filterKind,
      const std::vector<RowVectorPtr>& batches,
      const Subfield& subfield,
      std::vector<uint32_t>& hits) override {
    std::unique_ptr<velox::common::Filter> filter;
    switch (filterKind) {
      case FilterKind::kIsNull:
        filter = std::make_unique<velox::common::IsNull>();
        break;
      case FilterKind::kIsNotNull:
        filter = std::make_unique<velox::common::IsNotNull>();
        break;
      default:
        filter = makeRangeFilter(startPct, selectPct);
        break;
    }

    size_t numHits = 0;
    SimpleVector<T>* values = nullptr;
    int32_t previousBatch = -1;
    for (auto hit : hits) {
      auto batch = batchNumber(hit);
      if (batch != previousBatch) {
        previousBatch = batch;
        auto vector = batches[batch];
        values = getChildBySubfield(batches[batch].get(), subfield)
                     ->as<SimpleVector<T>>();
      }
      auto row = batchRow(hit);
      if (values->isNullAt(row)) {
        if (filter->testNull()) {
          hits[numHits++] = hit;
        }
        continue;
      }
      if (velox::common::applyFilter(*filter, values->valueAt(row))) {
        hits[numHits++] = hit;
      }
    }
    hits.resize(numHits);
    return filter;
  }

  std::unique_ptr<velox::common::Filter> rowGroupSkipFilter(
      const std::vector<RowVectorPtr>& batches,
      const Subfield& subfield,
      std::vector<uint32_t>& hits) override {
    std::unique_ptr<velox::common::Filter> filter;
    filter = makeRowGroupSkipRangeFilter(batches, subfield);
    size_t numHits = 0;
    SimpleVector<T>* values = nullptr;
    int32_t previousBatch = -1;
    for (auto hit : hits) {
      auto batch = batchNumber(hit);
      if (batch != previousBatch) {
        previousBatch = batch;
        auto vector = batches[batch];
        values = getChildBySubfield(batches[batch].get(), subfield)
                     ->as<SimpleVector<T>>();
      }
      auto row = batchRow(hit);
      if (values->isNullAt(row)) {
        if (filter->testNull()) {
          hits[numHits++] = hit;
        }
        continue;
      }
      if (velox::common::applyFilter(*filter, values->valueAt(row))) {
        hits[numHits++] = hit;
      }
    }
    hits.resize(numHits);
    return filter;
  }

 private:
  void addSample(SimpleVector<T>* vector, vector_size_t index) {
    ++numSamples_;
    if (vector->isNullAt(index)) {
      ++numNulls_;
      return;
    }
    T value = vector->valueAt(index);
    size_t hash = folly::hasher<T>()(value) & kUniquesMask;
    if (uniques_.find(hash) != uniques_.end()) {
      return;
    }
    uniques_.insert(hash);
    ++numDistinct_;
    values_.push_back(value);
  }

  T valueAtPct(float pct, int32_t* indexOut = nullptr) {
    int32_t index = values_.size() * (pct / 100);
    int32_t boundedIndex =
        std::min<int32_t>(values_.size() - 1, std::max<int32_t>(0, index));
    if (indexOut) {
      *indexOut = boundedIndex;
    }
    return values_[boundedIndex];
  }

  std::unique_ptr<velox::common::Filter> makeRangeFilter(
      float startPct,
      float selectPct) {
    if (values_.empty()) {
      return std::make_unique<velox::common::IsNull>();
    }
    int32_t lowerIndex;
    int32_t upperIndex;
    T lower = valueAtPct(startPct, &lowerIndex);
    T upper = valueAtPct(startPct + selectPct, &upperIndex);
    if (upperIndex - lowerIndex < 1000 && ++counter_ % 10 < 3) {
      std::vector<int64_t> in;
      for (auto i = lowerIndex; i <= upperIndex; ++i) {
        in.push_back(values_[i]);
      }
      return velox::common::createBigintValues(in, true);
    }
    return std::make_unique<velox::common::BigintRange>(
        lower, upper, selectPct > 25);
  }

  std::unique_ptr<velox::common::Filter> makeRowGroupSkipRangeFilter(
      const std::vector<RowVectorPtr>& batches,
      const Subfield& subfield) {
    T max;
    bool hasMax = false;
    for (auto batch : batches) {
      auto values =
          getChildBySubfield(batch.get(), subfield)->as<SimpleVector<T>>();

      for (auto i = 0; i < values->size(); ++i) {
        if (values->isNullAt(i)) {
          continue;
        }
        if (hasMax && max < values->valueAt(i)) {
          max = values->valueAt(i);
        } else if (!hasMax) {
          max = values->valueAt(i);
          hasMax = true;
        }
      }
    }

    return std::make_unique<velox::common::BigintRange>(max, max, false);
  }

  static constexpr size_t kUniquesMask = 0xfff;
  std::vector<T> values_;
};

template <>
std::unique_ptr<Filter> ColumnStats<bool>::makeRangeFilter(
    float startPct,
    float selectPct) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  bool value = valueAtPct(startPct + selectPct);
  return std::make_unique<velox::common::BoolValue>(value, selectPct > 50);
}

template <>
std::unique_ptr<Filter> ColumnStats<float>::makeRangeFilter(
    float startPct,
    float selectPct) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  float lower = valueAtPct(startPct);
  float upper = valueAtPct(startPct + selectPct);
  return std::make_unique<velox::common::FloatRange>(
      lower, false, false, upper, false, false, selectPct > 25);
}

template <>
std::unique_ptr<Filter> ColumnStats<double>::makeRangeFilter(
    float startPct,
    float selectPct) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  double lower = valueAtPct(startPct);
  double upper = valueAtPct(startPct + selectPct);
  return std::make_unique<velox::common::DoubleRange>(
      lower, false, false, upper, false, false, selectPct > 25);
}

template <>
std::unique_ptr<Filter> ColumnStats<StringView>::makeRangeFilter(
    float startPct,
    float selectPct) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  StringView lower = valueAtPct(startPct);
  StringView upper = valueAtPct(startPct + selectPct);
  return std::make_unique<velox::common::BytesRange>(
      std::string(lower),
      false,
      false,
      std::string(upper),
      false,
      false,
      selectPct > 25);
}
template <>
std::unique_ptr<velox::common::Filter>
ColumnStats<StringView>::makeRowGroupSkipRangeFilter(
    const std::vector<RowVectorPtr>& /*batches*/,
    const Subfield& /*subfield*/) {
  static std::string max = kMaxString;
  return std::make_unique<velox::common::BytesRange>(
      max, false, false, max, false, false, false);
}

template <TypeKind Kind>
std::unique_ptr<AbstractColumnStats> makeStats(TypePtr type) {
  using T = typename TypeTraits<Kind>::NativeType;
  return std::make_unique<ColumnStats<T>>(type);
}

template <typename T>
class TestingHook : public ValueHook {
 public:
  explicit TestingHook(FlatVector<T>* result) : result_(result) {}

  void addValue(vector_size_t row, const void* value) override {
    result_->set(row, *reinterpret_cast<const T*>(value));
  }

 private:
  FlatVector<T>* result_;
};

template <>
void TestingHook<StringView>::addValue(vector_size_t row, const void* value) {
  result_->set(
      row, StringView(*reinterpret_cast<const folly::StringPiece*>(value)));
}

class E2EFilterTest : public testing::Test {
 protected:
  static constexpr int32_t kRowsInGroup = 10'000;

  void SetUp() override {
    pool_ = memory::getDefaultScopedMemoryPool();
    rng_.seed(1);
  }

  void makeDataset(
      const std::string& columns,
      std::function<void()> customizeData,
      bool wrapInStruct,
      bool forRowGroupSkip = false) {
    const size_t batchCount = 4;
    const size_t size = 25'000;
    std::string schema = wrapInStruct
        ? fmt::format("struct<{},struct_val:struct<{}>>", columns, columns)
        : fmt::format("struct<{}>", columns);
    dwio::type::fbhive::HiveTypeParser parser;
    rowType_ = std::dynamic_pointer_cast<const RowType>(parser.parse(schema));

    batches_.clear();
    for (size_t i = 0; i < batchCount; ++i) {
      batches_.push_back(std::static_pointer_cast<RowVector>(
          BatchMaker::createBatch(rowType_, size, *pool_)));
    }
    if (customizeData) {
      customizeData();
    }
    if (forRowGroupSkip) {
      addRowGroupSpecificData();
    }
    writeToMemory(rowType_, batches_, forRowGroupSkip);

    auto spec = makeScanSpec(SubfieldFilters{});

    uint64_t timeWithNoFilter = 0;
    readWithoutFilter(spec.get(), batches_, timeWithNoFilter);
    std::cout << " Time without filter: " << timeWithNoFilter << " us"
              << std::endl;
  }

  // Adds high values to 'batches_' so that these values occur only in some row
  // groups. Tests skipping row groups based on row group stats.
  void addRowGroupSpecificData() {
    auto type = batches_[0]->type();
    for (auto i = 0; i < type->size(); ++i) {
      if (type->childAt(i)->kind() == TypeKind::BIGINT) {
        setRowGroupMarkers<int64_t>(
            batches_, i, std::numeric_limits<int64_t>::max());
        return;
      }
      if (type->childAt(i)->kind() == TypeKind::VARCHAR) {
        static StringView marker(
            AbstractColumnStats::kMaxString,
            strlen(AbstractColumnStats::kMaxString));
        setRowGroupMarkers<StringView>(batches_, i, marker);
        return;
      }
    }
  }

  // Adds 'marker' to random places in selectable  row groups for 'i'th child in
  // 'batches' If 'marker' occurs in skippable row groups, sets the element to
  // T(). Row group numbers that are multiples of 3 are skippable.
  template <typename T>
  void setRowGroupMarkers(
      const std::vector<RowVectorPtr>& batches,
      int32_t child,
      T marker) {
    int32_t row = 0;
    for (auto& batch : batches) {
      auto values = batch->childAt(child)->as<FlatVector<T>>();
      for (auto i = 0; i < values->size(); ++i) {
        auto rowGroup = row++ / kRowsInGroup;
        bool isIn = (rowGroup % 3) != 0;
        if (isIn) {
          if (folly::Random::rand32(rng_) % 100 == 0) {
            values->set(i, marker);
          }
        } else {
          if (!values->isNullAt(i) && values->valueAt(i) == marker) {
            values->set(i, T());
          }
        }
      }
    }
  }

  SubfieldFilters makeFilters(
      const std::vector<FilterSpec>& filterSpecs,
      const std::vector<RowVectorPtr>& batches,
      std::vector<uint32_t>& hitRows) {
    vector_size_t totalSize = 0;
    for (auto& batch : batches) {
      totalSize += batch->size();
    }
    hitRows.reserve(totalSize);
    for (auto i = 0; i < batches.size(); ++i) {
      auto batch = batches[i];
      for (auto j = 0; j < batch->size(); ++j) {
        hitRows.push_back(batchPosition(i, j));
      }
    }

    RowVector* first = batches[0].get();

    SubfieldFilters filters;
    for (auto& filterSpec : filterSpecs) {
      Subfield subfield(filterSpec.field);
      auto vector = getChildBySubfield(first, subfield);
      std::unique_ptr<AbstractColumnStats> stats;
      switch (vector->typeKind()) {
        case TypeKind::BOOLEAN:
          stats = makeStats<TypeKind::BOOLEAN>(vector->type());
          break;
        case TypeKind::TINYINT:
          stats = makeStats<TypeKind::TINYINT>(vector->type());
          break;
        case TypeKind::SMALLINT:
          stats = makeStats<TypeKind::SMALLINT>(vector->type());
          break;
        case TypeKind::INTEGER:
          stats = makeStats<TypeKind::INTEGER>(vector->type());
          break;
        case TypeKind::BIGINT:
          stats = makeStats<TypeKind::BIGINT>(vector->type());
          break;
        case TypeKind::VARCHAR:
          stats = makeStats<TypeKind::VARCHAR>(vector->type());
          break;

        case TypeKind::REAL:
          stats = makeStats<TypeKind::REAL>(vector->type());
          break;
        case TypeKind::DOUBLE:
          stats = makeStats<TypeKind::DOUBLE>(vector->type());
          break;
        default:
          VELOX_CHECK(false, "Type not supported");
      }

      stats->sample(batches, subfield, hitRows);
      std::unique_ptr<Filter> filter;
      if (filterSpec.isForRowGroupSkip) {
        filter = stats->rowGroupSkipFilter(batches, subfield, hitRows);
      } else {
        filter = stats->filter(
            filterSpec.startPct,
            filterSpec.selectPct,
            filterSpec.filterKind,
            batches,
            subfield,
            hitRows);
      }
      filters[Subfield(filterSpec.field)] = std::move(filter);
    }

    return filters;
  }

  void makeAllNulls(const std::string& name) {
    Subfield subfield(name);
    for (RowVectorPtr batch : batches_) {
      auto values = getChildBySubfield(batch.get(), subfield);
      SelectivityVector rows(values->size());
      values->addNulls(nullptr, rows);
    }
  }

  template <typename T>
  void makeIntDistribution(
      const Subfield& field,
      int64_t min,
      int64_t max,
      int32_t repeats,
      int32_t rareFrequency,
      int64_t rareMin,
      int64_t rareMax,
      bool keepNulls) {
    int counter = 0;
    for (RowVectorPtr batch : batches_) {
      auto numbers =
          getChildBySubfield(batch.get(), field)->as<FlatVector<T>>();
      for (auto row = 0; row < numbers->size(); ++row) {
        if (keepNulls && numbers->isNullAt(row)) {
          continue;
        }
        int64_t value;
        if (counter % 100 < repeats) {
          value = counter % repeats;
          numbers->set(row, value);
        } else if (counter % 100 > 90 && row > 0) {
          numbers->copy(numbers, row - 1, row, 1);
        } else {
          if (rareFrequency && counter % rareFrequency == 0) {
            value =
                rareMin + (folly::Random::rand32(rng_) % (rareMax - rareMin));
          } else {
            value = min + (folly::Random::rand32(rng_) % (max - min));
          }
          numbers->set(row, value);
        }
        ++counter;
      }
    }
  }

  // Makes strings with an ascending sequence of S<n>, followed by
  // random values with the given cardinality, then repeated
  // values. This is intended to hit different RLE encodings,
  // e.g. repeat, repeat with delta and random values within a
  // range. These patterns repeat every 100 values so as to trigger
  // dictionary encoding.
  void makeStringDistribution(
      const Subfield& field,
      int cardinality,
      bool keepNulls,
      bool addOneOffs) {
    int counter = 0;
    for (RowVectorPtr batch : batches_) {
      auto strings =
          getChildBySubfield(batch.get(), field)->as<FlatVector<StringView>>();
      for (auto row = 0; row < strings->size(); ++row) {
        if (keepNulls && strings->isNullAt(row)) {
          continue;
        }
        std::string value;
        if (counter % 100 < cardinality) {
          value = fmt::format("s{}", counter % cardinality);
          strings->set(row, StringView(value));
        } else if (counter % 100 > 90 && row > 0) {
          strings->copy(strings, row - 1, row, 1);
        } else if (addOneOffs && counter % 234 == 0) {
          value = fmt::format(
              "s{}", folly::Random::rand32(rng_) % (111 * cardinality));

        } else {
          value = fmt::format("s{}", folly::Random::rand32(rng_) % cardinality);
          strings->set(row, StringView(value));
        }
        ++counter;
      }
    }
  }

  // Makes non-null strings unique by appending a row number.
  void makeStringUnique(const Subfield& field) {
    for (RowVectorPtr batch : batches_) {
      auto strings =
          getChildBySubfield(batch.get(), field)->as<FlatVector<StringView>>();
      for (auto row = 0; row < strings->size(); ++row) {
        if (strings->isNullAt(row)) {
          continue;
        }
        std::string value = strings->valueAt(row);
        value += fmt::format("{}", row);
        strings->set(row, StringView(value));
      }
    }
  }

  // Makes all data in 'batches_' non-null. This finds a sampling of
  // non-null values from each column and replaces nulls in the column
  // in question with one of these. A column where only nulls are
  // found in sampling is not changed.
  void makeNotNull(int32_t firstRow = 0) {
    for (RowVectorPtr batch : batches_) {
      for (auto& data : batch->children()) {
        std::vector<vector_size_t> nonNulls;
        vector_size_t probe = 0;
        for (auto counter = 0; counter < 23; ++counter) {
          // Sample with a prime stride for a handful of non-null  values.
          probe = (probe + 47) % data->size();
          if (!data->isNullAt(probe)) {
            nonNulls.push_back(probe);
          }
        }
        if (nonNulls.empty()) {
          continue;
        }
        int32_t nonNullCounter = 0;
        for (auto row = firstRow; row < data->size(); ++row) {
          if (data->isNullAt(row)) {
            data->copy(
                data.get(), row, nonNulls[nonNullCounter % nonNulls.size()], 1);
            ++nonNullCounter;
          }
        }
      }
    }
  }

  void writeToMemory(
      const TypePtr& type,
      const std::vector<RowVectorPtr>& batches,
      bool forRowGroupSkip) {
    auto config = std::make_shared<dwrf::Config>();
    config->set(dwrf::Config::COMPRESSION, dwrf::CompressionKind_NONE);
    config->set(dwrf::Config::USE_VINTS, useVInts_);
    WriterOptions options;
    options.config = config;
    options.schema = type;
    int32_t flushCounter = 0;
    // If we test row group skip, we have all the data in one stripe. For scan,
    // we start  a stripe every 'flushEveryNBatches_' batches.
    options.flushPolicy = [&](auto /* unused */, auto& /* unused */) {
      return forRowGroupSkip ? false
                             : (++flushCounter % flushEveryNBatches_ == 0);
    };
    sink_ = std::make_unique<MemorySink>(*pool_, 200 * 1024 * 1024);
    sinkPtr_ = sink_.get();
    writer_ = std::make_unique<Writer>(options, std::move(sink_), *pool_);
    for (auto& batch : batches) {
      writer_->write(batch);
    }
    writer_->close();
  }

  void readWithoutFilter(
      ScanSpec* spec,
      const std::vector<RowVectorPtr>& batches,
      uint64_t& time) {
    auto input = std::make_unique<MemoryInputStream>(
        sinkPtr_->getData(), sinkPtr_->size());

    dwio::common::ReaderOptions readerOpts;
    dwio::common::RowReaderOptions rowReaderOpts;
    auto reader = std::make_unique<DwrfReader>(readerOpts, std::move(input));
    // The spec must stay live over the lifetime of the reader.
    rowReaderOpts.setScanSpec(spec);
    auto rowReader = reader->createRowReader(rowReaderOpts);

    auto batchIndex = 0;
    auto rowIndex = 0;
    auto batch = BaseVector::create(rowType_, 1, pool_.get());
    while (true) {
      bool hasData;
      {
        MicrosecondTimer timer(&time);
        hasData = rowReader->next(1000, batch);
      }
      if (!hasData) {
        break;
      }

      for (int32_t i = 0; i < batch->size(); ++i) {
        ASSERT_TRUE(batch->equalValueAt(batches[batchIndex].get(), i, rowIndex))
            << "Content mismatch at batch " << batchIndex << " at index "
            << rowIndex
            << ": expected: " << batches[batchIndex]->toString(rowIndex)
            << " actual: " << batch->toString(i);

        if (++rowIndex == batches[batchIndex]->size()) {
          rowIndex = 0;
          ++batchIndex;
        }
      }
    }
    ASSERT_EQ(batchIndex, batches.size());
    ASSERT_EQ(rowIndex, 0);
  }

  void readWithFilter(
      ScanSpec* spec,
      const std::vector<RowVectorPtr>& batches,
      const std::vector<uint32_t>& hitRows,
      uint64_t& time,
      bool useValueHook,
      bool skipCheck = false) {
    auto input = std::make_unique<MemoryInputStream>(
        sinkPtr_->getData(), sinkPtr_->size());

    dwio::common::ReaderOptions readerOpts;
    dwio::common::RowReaderOptions rowReaderOpts;
    auto reader = std::make_unique<DwrfReader>(readerOpts, std::move(input));
    auto factory = std::make_unique<SelectiveColumnReaderFactory>(spec);
    // The  spec must stay live over the lifetime of the reader.
    rowReaderOpts.setScanSpec(spec);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    runtimeStats_ = dwio::common::RuntimeStatistics();
    auto rowIndex = 0;
    auto batch = BaseVector::create(rowType_, 1, pool_.get());
    resetReadBatchSizes();
    while (true) {
      {
        MicrosecondTimer timer(&time);
        bool hasData = rowReader->next(nextReadBatchSize(), batch);
        if (!hasData) {
          break;
        }
        if (batch->size() == 0) {
          // No hits in the last batch of rows.
          continue;
        }
        if (useValueHook) {
          auto rowVector = reinterpret_cast<RowVector*>(batch.get());
          for (int32_t i = 0; i < rowVector->childrenSize(); ++i) {
            auto child = rowVector->childAt(i);
            if (child->encoding() == VectorEncoding::Simple::LAZY) {
              ASSERT_TRUE(loadWithHook(rowVector, i, child, hitRows, rowIndex));
            }
          }
          rowIndex += batch->size();
          continue;
        }
        // Load eventual LazyVectors inside the timed section.
        auto rowVector = batch->asUnchecked<RowVector>();
        for (auto i = 0; i < rowVector->childrenSize(); ++i) {
          rowVector->loadedChildAt(i);
        }
        if (skipCheck) {
          // Fetch next batch inside timed section.
          continue;
        }
      }
      // Outside of timed section.
      for (int32_t i = 0; i < batch->size(); ++i) {
        uint32_t hit = hitRows[rowIndex++];
        ASSERT_TRUE(batch->equalValueAt(
            batches[batchNumber(hit)].get(), i, batchRow(hit)))
            << "Content mismatch at " << rowIndex - 1 << ": expected: "
            << batches[batchNumber(hit)]->toString(batchRow(hit))
            << " actual: " << batch->toString(i);
      }
    }
    if (!skipCheck) {
      ASSERT_EQ(rowIndex, hitRows.size());
    }
    rowReader->updateRuntimeStats(runtimeStats_);
  }

  template <TypeKind Kind>
  bool checkLoadWithHook(
      RowVector* batch,
      int32_t columnIndex,
      VectorPtr child,
      const std::vector<uint32_t>& hitRows,
      int32_t rowIndex) {
    using T = typename TypeTraits<Kind>::NativeType;
    std::vector<vector_size_t> rows;
    // The 5 first values are densely read.
    for (int32_t i = 0; i < 5 && i < batch->size(); ++i) {
      rows.push_back(i);
    }
    for (int32_t i = 5; i < 5 && i < batch->size(); i += 2) {
      rows.push_back(i);
    }
    auto result = std::static_pointer_cast<FlatVector<T>>(
        BaseVector::create(child->type(), batch->size(), pool_.get()));
    TestingHook<T> hook(result.get());
    child->as<LazyVector>()->load(rows, &hook);
    for (auto i = 0; i < rows.size(); ++i) {
      auto row = rows[i] + rowIndex;
      auto reference = batches_[batchNumber(hitRows[row])]
                           ->childAt(columnIndex)
                           ->as<FlatVector<T>>();
      auto referenceIndex = batchRow(hitRows[row]);
      if (reference->isNullAt(referenceIndex)) {
        continue; // The hook is ot called on nulls.
      }
      if (reference->valueAt(referenceIndex) != result->valueAt(i)) {
        return false;
      }
    }
    return true;
  }

  bool loadWithHook(
      RowVector* batch,
      int32_t columnIndex,
      VectorPtr child,
      const std::vector<uint32_t>& hitRows,
      int32_t rowIndex) {
    auto kind = child->typeKind();
    if (kind == TypeKind::ROW || kind == TypeKind::ARRAY ||
        kind == TypeKind::MAP) {
      return true;
    }
    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        checkLoadWithHook, kind, batch, columnIndex, child, hitRows, rowIndex);
  }

  static void makeFieldSpecs(
      const std::string& pathPrefix,
      int32_t level,
      const std::shared_ptr<const Type>& type,
      ScanSpec* spec) {
    switch (type->kind()) {
      case TypeKind::ROW: {
        auto rowType = dynamic_cast<const RowType*>(type.get());
        for (auto i = 0; i < type->size(); ++i) {
          std::string path = level == 0 ? rowType->nameOf(i)
                                        : pathPrefix + "." + rowType->nameOf(i);
          Subfield subfield(path);
          ScanSpec* fieldSpec = spec->getOrCreateChild(subfield);
          fieldSpec->setProjectOut(true);
          fieldSpec->setExtractValues(true);
          fieldSpec->setChannel(i);
          makeFieldSpecs(path, level + 1, type->childAt(i), spec);
        }
        break;
      }
      case TypeKind::MAP: {
        auto keySpec = spec->getOrCreateChild(Subfield(pathPrefix + ".keys"));
        keySpec->setProjectOut(true);
        keySpec->setExtractValues(true);
        makeFieldSpecs(pathPrefix + ".keys", level + 1, type->childAt(0), spec);
        auto valueSpec =
            spec->getOrCreateChild(Subfield(pathPrefix + ".elements"));
        valueSpec->setProjectOut(true);
        valueSpec->setExtractValues(true);
        makeFieldSpecs(
            pathPrefix + ".elements", level + 1, type->childAt(1), spec);
        break;
      }
      case TypeKind::ARRAY: {
        auto childSpec =
            spec->getOrCreateChild(Subfield(pathPrefix + ".elements"));
        childSpec->setProjectOut(true);
        childSpec->setExtractValues(true);
        makeFieldSpecs(
            pathPrefix + ".elements", level + 1, type->childAt(0), spec);
        break;
      }

      default:
        break;
    }
  }

  std::unique_ptr<ScanSpec> makeScanSpec(SubfieldFilters filters) {
    auto spec = std::make_unique<ScanSpec>("root");
    makeFieldSpecs("", 0, rowType_, spec.get());

    for (auto& pair : filters) {
      auto fieldSpec = spec->getOrCreateChild(pair.first);
      fieldSpec->setFilter(std::move(pair.second));
    }
    return spec;
  }

  std::string specsToString(const std::vector<FilterSpec>& specs) {
    std::stringstream out;
    bool first = true;
    for (auto& spec : specs) {
      if (!first) {
        out << ", ";
      }
      first = false;
      out << spec.field;
      if (spec.filterKind == FilterKind::kIsNull) {
        out << " is null";
      } else if (spec.filterKind == FilterKind::kIsNotNull) {
        out << " is not null";
      } else {
        out << ":" << spec.selectPct << "," << spec.startPct << " ";
      }
    }
    return out.str();
  }

  std::vector<FilterSpec> makeRandomSpecs(
      const std::vector<std::string>& filterable) {
    std::vector<FilterSpec> specs;
    auto deck = filterable;
    for (int i = 0; i < filterable.size(); ++i) {
      // We aim at 1.5
      if (folly::Random::rand32(rng_) % (100 * filterable.size()) < 125) {
        auto idx = folly::Random::rand32(rng_) % deck.size();
        auto name = deck[idx];
        if (specs.empty()) {
          ++filterCoverage_[name][0];
        } else {
          ++filterCoverage_[name][1];
        }
        deck.erase(deck.begin() + idx);
        specs.emplace_back();
        specs.back().field = name;
        auto category = folly::Random::rand32(rng_) % 13;
        if (category == 0) {
          specs.back().selectPct = 1;
        } else if (category < 4) {
          specs.back().selectPct = category * 10;
        } else if (category == 11) {
          specs.back().filterKind = FilterKind::kIsNull;
        } else if (category == 12) {
          specs.back().filterKind = FilterKind::kIsNotNull;
        } else {
          specs.back().selectPct = 60 + category * 4;
        }
        specs.back().startPct = specs.back().selectPct < 100
            ? folly::Random::rand32(rng_) %
                static_cast<int32_t>(100 - specs.back().selectPct)
            : 0;
      }
    }

    return specs;
  }

  void testFilterSpecs(const std::vector<FilterSpec>& filterSpecs) {
    std::vector<uint32_t> hitRows;
    auto filters = makeFilters(filterSpecs, batches_, hitRows);
    auto spec = makeScanSpec(std::move(filters));
    uint64_t timeWithFilter = 0;
    readWithFilter(spec.get(), batches_, hitRows, timeWithFilter, false);
    std::cout << hitRows.size() << "  in " << timeWithFilter << " us"
              << std::endl;

    if (FLAGS_timing_repeats) {
      for (auto i = 0; i < FLAGS_timing_repeats; ++i) {
        readWithFilter(
            spec.get(), batches_, hitRows, timeWithFilter, false, true);
      }
      std::cout << FLAGS_timing_repeats << " repeats in " << timeWithFilter
                << " us" << std::endl;
    }
    // Redo the test with LazyVectors for non-filtered columns.
    timeWithFilter = 0;
    for (auto& childSpec : spec->children()) {
      childSpec->setExtractValues(false);
    }
    readWithFilter(spec.get(), batches_, hitRows, timeWithFilter, false);
    std::cout << hitRows.size() << "  lazy vectors in " << timeWithFilter
              << " us" << std::endl;
    timeWithFilter = 0;
    readWithFilter(spec.get(), batches_, hitRows, timeWithFilter, true);
    std::cout << hitRows.size() << "  lazy vectors with sparse load pushdown "
              << "in " << timeWithFilter << " us" << std::endl;
  }

  void testRowGroupSkip(const std::vector<std::string>& filterable) {
    std::vector<FilterSpec> specs;
    // Makes a row group skipping filter for the first bigint column.
    for (auto& field : filterable) {
      VectorPtr child = getChildBySubfield(batches_[0].get(), Subfield(field));
      if (child->typeKind() == TypeKind::BIGINT ||
          child->typeKind() == TypeKind::VARCHAR) {
        specs.emplace_back();
        specs.back().field = field;
        specs.back().isForRowGroupSkip = true;
        break;
      }
    }
    if (specs.empty()) {
      // No suitable column.
      return;
    }
    std::cout << ": Testing with row group skip " << specsToString(specs)
              << std::endl;
    testFilterSpecs(specs);
    EXPECT_LT(0, runtimeStats_.skippedStrides);
  }

  void testWithTypes(
      const std::string& columns,
      std::function<void()> customize,
      bool wrapInStruct,
      const std::vector<std::string>& filterable,
      int32_t numCombinations,
      bool tryNoNulls = false,
      bool tryNoVInts = false) {
    for (int32_t noVInts = 0; noVInts < (tryNoVInts ? 2 : 1); ++noVInts) {
      useVInts_ = !noVInts;
      for (int32_t noNulls = 0; noNulls < (tryNoNulls ? 2 : 1); ++noNulls) {
        if (noNulls) {
          makeNotNull();
        }
        std::cout << fmt::format(
                         "Run with {} nulls, {} vints",
                         noNulls ? "no" : "",
                         noVInts ? "no" : "")
                  << std::endl;
        rng_.seed(1);

        makeDataset(columns, customize, wrapInStruct);
        for (auto i = 0; i < numCombinations; ++i) {
          std::vector<FilterSpec> specs = makeRandomSpecs(filterable);
          std::cout << i << ": Testing " << specsToString(specs) << std::endl;
          testFilterSpecs(specs);
        }
        makeDataset(columns, customize, wrapInStruct, true);
        testRowGroupSkip(filterable);
      }
    }
    std::cout << "Coverage:" << std::endl;
    for (auto& pair : filterCoverage_) {
      std::cout << pair.first << " as first filter: " << pair.second[0]
                << " as second: " << pair.second[1] << std::endl;
    }
  }

  // Allows testing reading with different batch sizes.
  void resetReadBatchSizes() {
    nextReadSizeIndex_ = 0;
  }

  int32_t nextReadBatchSize() {
    if (nextReadSizeIndex_ >= readSizes_.size()) {
      return 1000;
    }
    return readSizes_[nextReadSizeIndex_++];
  }

  std::unique_ptr<memory::MemoryPool> pool_;
  std::shared_ptr<const RowType> rowType_;
  std::unique_ptr<MemorySink> sink_;
  MemorySink* sinkPtr_;
  std::unique_ptr<Writer> writer_;
  std::vector<RowVectorPtr> batches_;
  std::unordered_map<std::string, std::array<int32_t, 2>> filterCoverage_;
  folly::Random::DefaultGenerator rng_;
  bool useVInts_ = true;
  dwio::common::RuntimeStatistics runtimeStats_;
  // Number of calls to flush policy between starting new stripes.
  int32_t flushEveryNBatches_{10};
  int32_t nextReadSizeIndex_{0};
  std::vector<int32_t> readSizes_;
};

TEST_F(E2EFilterTest, integerDirect) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint,"
      "long_null:bigint",
      [&]() { makeAllNulls("long_null"); },
      true,
      {"short_val", "int_val", "long_val"},
      20,
      true,
      true);
}

TEST_F(E2EFilterTest, integerDictionary) {
  testWithTypes(
      "short_val:smallint,"
      "int_val:int,"
      "long_val:bigint",
      [&]() {
        makeIntDistribution<int64_t>(
            Subfield("long_val"),
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            10000000000, // rareMax
            true); // keepNulls

        makeIntDistribution<int32_t>(
            Subfield("int_val"),
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -9999, // rareMin
            100000000, // rareMax
            false); // keepNulls

        makeIntDistribution<int16_t>(
            Subfield("short_val"),
            10, // min
            100, // max
            22, // repeats
            19, // rareFrequency
            -999, // rareMin
            30000, // rareMax
            true); // keepNulls
      },
      true,
      {"short_val", "int_val", "long_val"},
      20,
      true,
      true);
}

TEST_F(E2EFilterTest, byteRle) {
  testWithTypes(
      "tiny_val:tinyint,"
      "bool_val:boolean,"
      "long_val:bigint,"
      "tiny_null:bigint",
      [&]() { makeAllNulls("tiny_null"); },
      true,
      {"tiny_val", "bool_val", "tiny_null"},
      20);
}

TEST_F(E2EFilterTest, floatAndDouble) {
  testWithTypes(
      "float_val:float,"
      "double_val:double,"
      "long_val:bigint,"
      "float_null:float",
      [&]() { makeAllNulls("float_null"); },
      true,
      {"float_val", "double_val", "float_null"},
      20,
      true,
      false);
}

TEST_F(E2EFilterTest, stringDirect) {
  flushEveryNBatches_ = 1;
  testWithTypes(
      "string_val:string,"
      "string_val_2:string",
      [&]() {
        makeStringUnique(Subfield("string_val"));
        makeStringUnique(Subfield("string_val_2"));
      },

      true,
      {"string_val", "string_val_2"},
      20,
      true);
}

TEST_F(E2EFilterTest, stringDictionary) {
  testWithTypes(
      "string_val:string,"
      "string_val_2:string",
      [&]() {
        makeStringDistribution(Subfield("string_val"), 100, true, false);
        makeStringDistribution(Subfield("string_val_2"), 170, false, true);
      },
      true,
      {"string_val", "string_val_2"},
      20,
      true,
      true);
}

TEST_F(E2EFilterTest, listAndMap) {
  testWithTypes(
      "long_val:bigint,"
      "long_val_2:bigint,"
      "int_val:int,"
      "array_val:array<struct<array_member: array<int>>>,"
      "map_val:map<bigint,struct<nested_map: map<int, int>>>",
      [&]() {},
      true,
      {"long_val", "long_val_2", "int_val"},
      10);
}

TEST_F(E2EFilterTest, nullCompactRanges) {
  // Makes a dataset with nulls at the beginning. Tries different
  // filter ombinations on progressively larger batches. tests for a
  // bug in null compaction where null bits past end of nulls buffer
  // were compacted while there actually were no nulls.

  readSizes_ = {10, 100, 1000, 10000, 10000, 10000};
  testWithTypes(
      "tiny_val:tinyint,"
      "bool_val:boolean,"
      "long_val:bigint,"
      "tiny_null:bigint",

      [&]() { makeNotNull(500); },

      true,
      {"tiny_val", "bool_val", "long_val", "tiny_null"},
      20,
      false,
      false);
}

} // namespace facebook::velox::dwio::dwrf
