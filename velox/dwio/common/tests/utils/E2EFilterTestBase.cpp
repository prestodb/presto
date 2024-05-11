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

#include "velox/dwio/common/tests/utils/E2EFilterTestBase.h"

#include "velox/dwio/common/tests/utils/DataSetBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/expression/ExprToSubfieldFilter.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorMaker.h"

// Set FLAGS_minloglevel to a value in {1,2,3} to disable logging at the
// INFO(=0) level.
// Set FLAGS_logtostderr = true to log messages to stderr instead of logfiles
// Set FLAGS_timing_repeats = n to run timing filter tests n times
DEFINE_int32(timing_repeats, 0, "Count of repeats for timing filter tests");

namespace facebook::velox::dwio::common {

using namespace facebook::velox::test;
using namespace facebook::velox::type::fbhive;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox;
using namespace facebook::velox::common;

using dwio::common::BufferedInput;
using dwio::common::InMemoryReadFile;
using dwio::common::MemorySink;
using velox::common::Subfield;

std::vector<RowVectorPtr> E2EFilterTestBase::makeDataset(
    std::function<void()> customize,
    bool forRowGroupSkip) {
  if (!dataSetBuilder_) {
    dataSetBuilder_ = std::make_unique<DataSetBuilder>(*leafPool_, 0);
  }

  dataSetBuilder_->makeDataset(rowType_, batchCount_, batchSize_);

  if (forRowGroupSkip) {
    dataSetBuilder_->withRowGroupSpecificData(kRowsInGroup);
  }

  if (customize) {
    customize();
  }

  std::vector<RowVectorPtr> batches = *dataSetBuilder_->build();
  return batches;
}

void E2EFilterTestBase::makeAllNulls(const std::string& fieldName) {
  dataSetBuilder_->withAllNullsForField(Subfield(fieldName));
}

void E2EFilterTestBase::makeStringDistribution(
    const std::string& fieldName,
    int cardinality,
    bool keepNulls,
    bool addOneOffs) {
  auto field = Subfield(fieldName);
  dataSetBuilder_->withStringDistributionForField(
      field, cardinality, keepNulls, addOneOffs);
}

void E2EFilterTestBase::makeStringUnique(const std::string& fieldName) {
  dataSetBuilder_->withUniqueStringsForField(Subfield(fieldName));
}

void E2EFilterTestBase::makeNotNull(int32_t firstRow) {
  dataSetBuilder_->withNoNullsAfter(firstRow);
}

void E2EFilterTestBase::readWithoutFilter(
    std::shared_ptr<ScanSpec> spec,
    const std::vector<RowVectorPtr>& batches,
    uint64_t& time) {
  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  dwio::common::RowReaderOptions rowReaderOpts;
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(sinkData_),
      readerOpts.getMemoryPool());
  auto reader = makeReader(readerOpts, std::move(input));

  // The spec must stay live over the lifetime of the reader.
  setUpRowReaderOptions(rowReaderOpts, spec);
  OwnershipChecker ownershipChecker;
  auto rowReader = reader->createRowReader(rowReaderOpts);

  auto batchIndex = 0;
  auto rowIndex = 0;
  auto resultBatch = BaseVector::create(rowType_, 1, leafPool_.get());
  while (true) {
    bool hasData;
    {
      MicrosecondTimer timer(&time);
      auto rowsScanned = rowReader->next(1000, resultBatch);
      VLOG(1) << "rowsScanned=" << rowsScanned;
      hasData = rowsScanned > 0;
    }
    if (!hasData) {
      break;
    }

    ownershipChecker.check(resultBatch);
    for (int32_t i = 0; i < resultBatch->size(); ++i) {
      ASSERT_TRUE(
          resultBatch->equalValueAt(batches[batchIndex].get(), i, rowIndex))
          << "Content mismatch at resultBatch " << batchIndex << " at index "
          << rowIndex
          << ": expected: " << batches[batchIndex]->toString(rowIndex)
          << " actual: " << resultBatch->toString(i);

      if (++rowIndex == batches[batchIndex]->size()) {
        rowIndex = 0;
        ++batchIndex;
      }
    }
  }
  ASSERT_EQ(batchIndex, batches.size());
  ASSERT_EQ(rowIndex, 0);
}

void E2EFilterTestBase::readWithFilter(
    std::shared_ptr<ScanSpec> spec,
    const MutationSpec& mutationSpec,
    const std::vector<RowVectorPtr>& batches,
    const std::vector<uint64_t>& hitRows,
    uint64_t& time,
    bool useValueHook,
    bool skipCheck) {
  dwio::common::ReaderOptions readerOpts{leafPool_.get()};
  dwio::common::RowReaderOptions rowReaderOpts;
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(sinkData_),
      readerOpts.getMemoryPool());
  auto reader = makeReader(readerOpts, std::move(input));

  // The spec must stay live over the lifetime of the reader.
  setUpRowReaderOptions(rowReaderOpts, spec);
  VLOG(1) << "spec: " << spec->toString();
  OwnershipChecker ownershipChecker;
  auto rowReader = reader->createRowReader(rowReaderOpts);
  runtimeStats_ = dwio::common::RuntimeStatistics();
  auto rowIndex = 0;
  auto resultBatch = BaseVector::create(rowType_, 1, leafPool_.get());
  resetReadBatchSizes();
  int32_t clearCnt = 0;
  auto deletedRowsIter = mutationSpec.deletedRows.begin();
  while (true) {
    {
      MicrosecondTimer timer(&time);
      if (++clearCnt % 17 == 0) {
        rowReader->resetFilterCaches();
      }
      auto nextRowNumber = rowReader->nextRowNumber();
      if (nextRowNumber == RowReader::kAtEnd) {
        break;
      }
      auto readSize = rowReader->nextReadSize(nextReadBatchSize());
      std::vector<uint64_t> isDeleted(bits::nwords(readSize));
      bool haveDelete = false;
      for (; deletedRowsIter != mutationSpec.deletedRows.end();
           ++deletedRowsIter) {
        auto i = *deletedRowsIter;
        if (i < nextRowNumber) {
          continue;
        }
        if (i >= nextRowNumber + readSize) {
          break;
        }
        bits::setBit(isDeleted.data(), i - nextRowNumber);
        haveDelete = true;
      }
      Mutation mutation;
      if (haveDelete) {
        mutation.deletedRows = isDeleted.data();
      }
      auto rowsScanned = rowReader->next(readSize, resultBatch, &mutation);
      VLOG(1) << "rowsScanned=" << rowsScanned;
      ASSERT_EQ(rowsScanned, readSize);
      if (resultBatch->size() == 0) {
        // No hits in the last resultBatch of rows.
        continue;
      }
      if (useValueHook) {
        auto rowVector = reinterpret_cast<RowVector*>(resultBatch.get());
        for (int32_t i = 0; i < rowVector->childrenSize(); ++i) {
          auto child = rowVector->childAt(i);
          if (child->encoding() == VectorEncoding::Simple::LAZY &&
              typeKindSupportsValueHook(child->typeKind())) {
            ASSERT_TRUE(
                loadWithHook(batches, rowVector, i, child, hitRows, rowIndex));
          }
        }
        rowIndex += resultBatch->size();
        continue;
      }
      // Load eventual LazyVectors inside the timed section.
      auto rowVector = resultBatch->asUnchecked<RowVector>();
      for (auto i = 0; i < rowVector->childrenSize(); ++i) {
        rowVector->childAt(i)->loadedVector();
      }
      if (skipCheck) {
        // Fetch next resultBatch inside timed section.
        continue;
      }
    }
    // Outside of timed section.
    for (int32_t i = 0; i < resultBatch->size(); ++i) {
      uint64_t hit = hitRows[rowIndex++];
      auto expectedBatch = batches[batchNumber(hit)].get();
      auto expectedRow = batchRow(hit);
      // We compare column by column, skipping over filter-only columns.
      for (auto childIndex = 0; childIndex < spec->children().size();
           ++childIndex) {
        if (!spec->children()[childIndex]->keepValues()) {
          continue;
        }
        auto column = spec->children()[childIndex]->channel();
        auto result = resultBatch->asUnchecked<RowVector>()->childAt(column);
        auto expectedColumn = expectedBatch->childAt(column).get();
        ASSERT_TRUE(result->equalValueAt(expectedColumn, i, expectedRow))
            << "Content mismatch at " << rowIndex - 1 << " column " << column
            << ": expected: " << expectedColumn->toString(expectedRow)
            << " actual: " << result->toString(i);
      }
    }
    // Check no overwrites after all LazyVectors are loaded.
    ownershipChecker.check(resultBatch);
  }
  if (!skipCheck) {
    ASSERT_EQ(rowIndex, hitRows.size());
  }
  rowReader->updateRuntimeStats(runtimeStats_);
}

bool E2EFilterTestBase::loadWithHook(
    const std::vector<RowVectorPtr>& batches,
    RowVector* batch,
    int32_t columnIndex,
    VectorPtr child,
    const std::vector<uint64_t>& hitRows,
    int32_t rowIndex) {
  auto kind = child->typeKind();
  if (kind == TypeKind::ROW || kind == TypeKind::ARRAY ||
      kind == TypeKind::MAP) {
    return true;
  }
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      checkLoadWithHook,
      kind,
      batches,
      batch,
      columnIndex,
      child,
      hitRows,
      rowIndex);
}

void E2EFilterTestBase::testReadWithFilterLazy(
    const std::shared_ptr<common::ScanSpec>& spec,
    const MutationSpec& mutations,
    const std::vector<RowVectorPtr>& batches,
    const std::vector<uint64_t>& hitRows) {
  SCOPED_TRACE("Lazy");
  // Test with LazyVectors for non-filtered columns.
  uint64_t timeWithFilter = 0;
  for (auto& childSpec : spec->children()) {
    childSpec->setExtractValues(false);
    for (auto& grandchild : childSpec->children()) {
      grandchild->setExtractValues(false);
    }
  }
  readWithFilter(spec, mutations, batches, hitRows, timeWithFilter, false);
  timeWithFilter = 0;
  readWithFilter(spec, mutations, batches, hitRows, timeWithFilter, true);
}

void E2EFilterTestBase::testFilterSpecs(
    const std::vector<RowVectorPtr>& batches,
    const std::vector<FilterSpec>& filterSpecs) {
  MutationSpec mutations;
  std::vector<uint64_t> hitRows;
  auto filters = filterGenerator_->makeSubfieldFilters(
      filterSpecs, batches, &mutations, hitRows);
  auto spec = filterGenerator_->makeScanSpec(std::move(filters));
  uint64_t timeWithFilter = 0;
  readWithFilter(spec, mutations, batches, hitRows, timeWithFilter, false);

  if (FLAGS_timing_repeats) {
    for (auto i = 0; i < FLAGS_timing_repeats; ++i) {
      readWithFilter(
          spec, mutations, batches, hitRows, timeWithFilter, false, true);
    }
    LOG(INFO) << fmt::format(
        "    {} hits in {} us, {} input rows/s\n",
        hitRows.size(),
        timeWithFilter,
        batches[0]->size() * batches.size() * FLAGS_timing_repeats /
            (timeWithFilter / 1000000.0));
  }
  testReadWithFilterLazy(spec, mutations, batches, hitRows);
}

void E2EFilterTestBase::testNoRowGroupSkip(
    const std::vector<RowVectorPtr>& batches,
    const std::vector<std::string>& filterable,
    int32_t numCombinations) {
  SCOPED_TRACE("No row group skip");
  auto spec = filterGenerator_->makeScanSpec(SubfieldFilters{});

  uint64_t timeWithNoFilter = 0;
  readWithoutFilter(spec, batches, timeWithNoFilter);

  for (auto i = 0; i < numCombinations; ++i) {
    std::vector<FilterSpec> specs =
        filterGenerator_->makeRandomSpecs(filterable, 125);
    testFilterSpecs(batches, specs);
  }
}

void E2EFilterTestBase::testRowGroupSkip(
    const std::vector<RowVectorPtr>& batches,
    const std::vector<std::string>& filterable) {
  SCOPED_TRACE("Row group skip");
  std::vector<FilterSpec> specs;
  // Makes a row group skipping filter for the first bigint column.
  for (auto& field : filterable) {
    VectorPtr child = getChildBySubfield(batches[0].get(), Subfield(field));
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

  testFilterSpecs(batches, specs);
  EXPECT_LT(0, runtimeStats_.skippedStrides);
}

void E2EFilterTestBase::testPruningWithFilter(
    std::vector<RowVectorPtr>& batches,
    const std::vector<std::string>& filterable) {
  SCOPED_TRACE("Subfield pruning");
  std::vector<std::string> prunable;
  for (int i = 0; i < rowType_->size(); ++i) {
    auto kind = rowType_->childAt(i)->kind();
    if (kind != TypeKind::ROW && kind != TypeKind::ARRAY &&
        kind != TypeKind::MAP) {
      continue;
    }
    auto& field = rowType_->nameOf(i);
    bool ok = true;
    for (auto& path : filterable) {
      if (path.size() >= field.size() &&
          strncmp(path.data(), field.data(), field.size()) == 0) {
        ok = false;
        break;
      }
    }
    if (ok) {
      prunable.push_back(field);
    }
  }
  VLOG(1) << prunable.size() << " of the fields are prunable";
  if (prunable.empty()) {
    return;
  }
  auto scanSpec =
      filterGenerator_->makeScanSpec(prunable, batches, leafPool_.get());
  MutationSpec mutations;
  std::vector<uint64_t> hitRows;
  auto filterSpecs = filterGenerator_->makeRandomSpecs(filterable, 125);
  auto filters = filterGenerator_->makeSubfieldFilters(
      filterSpecs, batches, &mutations, hitRows);
  FilterGenerator::addToScanSpec(filters, *scanSpec);
  uint64_t timeWithFilter = 0;
  readWithFilter(scanSpec, mutations, batches, hitRows, timeWithFilter, false);
  testReadWithFilterLazy(scanSpec, mutations, batches, hitRows);
}

void E2EFilterTestBase::testScenario(
    const std::string& columns,
    std::function<void()> customize,
    bool wrapInStruct,
    const std::vector<std::string>& filterable,
    int32_t numCombinations) {
  rowType_ = DataSetBuilder::makeRowType(columns, wrapInStruct);
  filterGenerator_ = std::make_unique<FilterGenerator>(rowType_, seed_);

  auto batches = makeDataset(customize, false);
  writeToMemory(rowType_, batches, false);
  testNoRowGroupSkip(batches, filterable, numCombinations);
  testPruningWithFilter(batches, filterable);

  if (testRowGroupSkip_) {
    batches = makeDataset(customize, true);
    writeToMemory(rowType_, batches, true);
    testRowGroupSkip(batches, filterable);
  }
}

void E2EFilterTestBase::testMetadataFilterImpl(
    const std::vector<RowVectorPtr>& batches,
    common::Subfield filterField,
    std::unique_ptr<common::Filter> filter,
    core::ExpressionEvaluator* evaluator,
    const std::string& remainingFilter,
    std::function<bool(int64_t, int64_t)> validationFilter) {
  SCOPED_TRACE(fmt::format("remainingFilter={}", remainingFilter));
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  if (filter) {
    spec->getOrCreateChild(std::move(filterField))
        ->setFilter(std::move(filter));
  }
  auto untypedExpr = parse::parseExpr(remainingFilter, {});
  auto typedExpr = core::Expressions::inferTypes(
      untypedExpr, batches[0]->type(), leafPool_.get());
  auto metadataFilter =
      std::make_shared<MetadataFilter>(*spec, *typedExpr, evaluator);
  auto specA = spec->getOrCreateChild(common::Subfield("a"));
  auto specB = spec->getOrCreateChild(common::Subfield("b"));
  auto specC = spec->getOrCreateChild(common::Subfield("b.c"));
  specA->setProjectOut(true);
  specA->setChannel(0);
  specB->setProjectOut(true);
  specB->setChannel(1);
  specC->setProjectOut(true);
  specC->setChannel(0);
  ReaderOptions readerOpts{leafPool_.get()};
  RowReaderOptions rowReaderOpts;
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(sinkData_),
      readerOpts.getMemoryPool());
  auto reader = makeReader(readerOpts, std::move(input));
  setUpRowReaderOptions(rowReaderOpts, spec);
  rowReaderOpts.setMetadataFilter(metadataFilter);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(batches[0]->type(), 1, leafPool_.get());
  int64_t originalIndex = 0;
  auto nextExpectedIndex = [&]() -> int64_t {
    for (;;) {
      if (originalIndex >= batches.size() * batchSize_) {
        return -1;
      }
      auto& batch = batches[originalIndex / batchSize_];
      auto vecA = batch->as<RowVector>()->childAt(0)->asFlatVector<int64_t>();
      auto vecC = batch->as<RowVector>()
                      ->childAt(1)
                      ->as<RowVector>()
                      ->childAt(0)
                      ->asFlatVector<int64_t>();
      auto j = originalIndex++ % batchSize_;
      auto a = vecA->valueAt(j);
      auto c = vecC->valueAt(j);
      if (validationFilter(a, c)) {
        return originalIndex - 1;
      }
    }
  };
  while (rowReader->next(1000, result)) {
    for (int i = 0; i < result->size(); ++i) {
      auto totalIndex = nextExpectedIndex();
      ASSERT_GE(totalIndex, 0);
      auto& expected = batches[totalIndex / batchSize_];
      vector_size_t j = totalIndex % batchSize_;
      ASSERT_TRUE(result->equalValueAt(expected.get(), i, j))
          << result->toString(i) << " vs " << expected->toString(j);
    }
  }
  ASSERT_EQ(nextExpectedIndex(), -1);
}

void E2EFilterTestBase::testMetadataFilter() {
  flushEveryNBatches_ = 1;
  batchSize_ = 10;
  test::VectorMaker vectorMaker(leafPool_.get());
  functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();
  core::QueryCtx queryCtx;
  exec::SimpleExpressionEvaluator evaluator(&queryCtx, leafPool_.get());

  // a: bigint, b: struct<c: bigint>
  std::vector<RowVectorPtr> batches;
  for (int i = 0; i < 10; ++i) {
    auto a = BaseVector::create<FlatVector<int64_t>>(
        BIGINT(), batchSize_, leafPool_.get());
    auto c = BaseVector::create<FlatVector<int64_t>>(
        BIGINT(), batchSize_, leafPool_.get());
    for (int j = 0; j < batchSize_; ++j) {
      a->set(j, i);
      c->set(j, i);
    }
    auto b = std::make_shared<RowVector>(
        leafPool_.get(),
        ROW({{"c", c->type()}}),
        nullptr,
        c->size(),
        std::vector<VectorPtr>({c}));
    batches.push_back(std::make_shared<RowVector>(
        leafPool_.get(),
        ROW({{"a", a->type()}, {"b", b->type()}}),
        nullptr,
        a->size(),
        std::vector<VectorPtr>({a, b})));
  }
  writeToMemory(batches[0]->type(), batches, false);

  testMetadataFilterImpl(
      batches,
      common::Subfield("a"),
      nullptr,
      &evaluator,
      "a >= 9 or not (a < 4 and b.c >= 2)",
      [](int64_t a, int64_t c) { return a >= 9 || !(a < 4 && c >= 2); });
  testMetadataFilterImpl(
      batches,
      common::Subfield("a"),
      exec::greaterThanOrEqual(1),
      &evaluator,
      "a >= 9 or not (a < 4 and b.c >= 2)",
      [](int64_t a, int64_t c) {
        return a >= 1 && (a >= 9 || !(a < 4 && c >= 2));
      });
  testMetadataFilterImpl(
      batches,
      common::Subfield("a"),
      nullptr,
      &evaluator,
      "a in (1, 3, 8) or a >= 9",
      [](int64_t a, int64_t) { return a == 1 || a == 3 || a == 8 || a >= 9; });
  testMetadataFilterImpl(
      batches,
      common::Subfield("a"),
      nullptr,
      &evaluator,
      "not (a not in (2, 3, 5, 7))",
      [](int64_t a, int64_t) {
        return !!(a == 2 || a == 3 || a == 5 || a == 7);
      });

  {
    SCOPED_TRACE("Values not unique in row group");
    auto a = vectorMaker.flatVector<int64_t>(batchSize_, folly::identity);
    auto c = vectorMaker.flatVector<int64_t>(batchSize_, folly::identity);
    auto b = vectorMaker.rowVector({"c"}, {c});
    batches = {vectorMaker.rowVector({"a", "b"}, {a, b})};
    writeToMemory(batches[0]->type(), batches, false);
    testMetadataFilterImpl(
        batches,
        common::Subfield("a"),
        nullptr,
        &evaluator,
        "not (a = 1 and b.c = 2)",
        [](int64_t a, int64_t c) { return !(a == 1 && c == 2); });
  }
  {
    SCOPED_TRACE("Leaf node lifecycle");
    auto column = vectorMaker.flatVector<int64_t>(batchSize_, folly::identity);
    batches = {
        vectorMaker.rowVector({"a", "b", "c"}, {column, column, column})};
    writeToMemory(batches[0]->type(), batches, false);
    auto spec = std::make_shared<common::ScanSpec>("<root>");
    spec->addAllChildFields(*batches[0]->type());
    auto untypedExpr = parse::parseExpr("a = 1 or b + c = 2", {});
    auto typedExpr = core::Expressions::inferTypes(
        untypedExpr, batches[0]->type(), leafPool_.get());
    auto metadataFilter =
        std::make_shared<MetadataFilter>(*spec, *typedExpr, &evaluator);
    // Top level metadata filter is null, so leaf node shoud not be referenced
    // from ScanSpec.
    ASSERT_EQ(spec->childByName("a")->numMetadataFilters(), 0);
    ASSERT_EQ(spec->childByName("b")->numMetadataFilters(), 0);
    ASSERT_EQ(spec->childByName("c")->numMetadataFilters(), 0);
  }
}

void E2EFilterTestBase::testSubfieldsPruning() {
  test::VectorMaker vectorMaker(leafPool_.get());
  std::vector<RowVectorPtr> batches;
  constexpr int kMapSize = 5;
  for (int i = 0; i < batchCount_; ++i) {
    auto a = vectorMaker.flatVector<int64_t>(
        batchSize_, [&](auto j) { return i + j; });
    auto b = vectorMaker.mapVector<int64_t, int64_t>(
        batchSize_,
        [&](auto) { return kMapSize; },
        [](auto j) { return j % kMapSize; },
        [](auto j) { return j; },
        [&](auto j) { return j >= i + 1 && j % 23 == (i + 1) % 23; });
    auto c = vectorMaker.arrayVector<int64_t>(
        batchSize_,
        [](auto j) { return 7 + (j % 3); },
        [](auto j) { return j; },
        [&](auto j) { return j >= i + 1 && j % 19 == (i + 1) % 19; });
    auto d = vectorMaker.mapVector<int64_t, StringView>(
        batchSize_,
        [](auto) { return 1; },
        [](auto) { return 0; },
        [](auto) { return "foofoofoofoofoo"_sv; });
    batches.push_back(
        vectorMaker.rowVector({"a", "b", "c", "d"}, {a, b, c, d}));
  }
  writeToMemory(batches[0]->type(), batches, false);
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  std::vector<int64_t> requiredA;
  for (int i = 0; i <= batchSize_ + batchCount_ - 2; ++i) {
    if (i % 13 != 0) {
      requiredA.push_back(i);
    }
  }
  spec->addFieldRecursively("a", *BIGINT(), 0)
      ->setFilter(common::createBigintValues(requiredA, false));
  std::vector<int64_t> requiredB;
  for (int i = 0; i < kMapSize; i += 2) {
    requiredB.push_back(i);
  }
  auto specB = spec->addFieldRecursively("b", *MAP(BIGINT(), BIGINT()), 1);
  specB->setFilter(exec::isNotNull());
  specB->childByName(common::ScanSpec::kMapKeysFieldName)
      ->setFilter(common::createBigintValues(requiredB, false));
  spec->addFieldRecursively("c", *ARRAY(BIGINT()), 2)
      ->setMaxArrayElementsCount(6);
  auto specD = spec->addFieldRecursively("d", *MAP(BIGINT(), VARCHAR()), 3);
  specD->childByName(common::ScanSpec::kMapKeysFieldName)
      ->setFilter(common::createBigintValues({1}, false));
  ReaderOptions readerOpts{leafPool_.get()};
  RowReaderOptions rowReaderOpts;
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(sinkData_),
      readerOpts.getMemoryPool());
  auto reader = makeReader(readerOpts, std::move(input));
  setUpRowReaderOptions(rowReaderOpts, spec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(batches[0]->type(), 1, leafPool_.get());
  int totalIndex = 0;
  while (rowReader->next(10, result)) {
    ASSERT_EQ(*result->type(), *batches[0]->type());
    auto actual = result->as<RowVector>();
    for (int ii = 0; ii < result->size(); ++totalIndex) {
      ASSERT_LT(totalIndex, batchCount_ * batchSize_);
      int i = totalIndex / batchSize_;
      int j = totalIndex % batchSize_;
      auto* expected = batches[i]->asUnchecked<RowVector>();
      auto* a = expected->childAt(0)->asFlatVector<int64_t>();
      auto* b = expected->childAt(1)->asUnchecked<MapVector>();
      if (a->valueAt(j) % 13 == 0 || b->isNullAt(j)) {
        continue;
      }
      ASSERT_TRUE(actual->childAt(0)->equalValueAt(a, ii, j));
      auto* bb = actual->childAt(1)->asUnchecked<MapVector>();
      ASSERT_FALSE(bb->isNullAt(ii));
      ASSERT_EQ(bb->sizeAt(ii), (kMapSize + 1) / 2);
      for (int k = 0; k < kMapSize; k += 2) {
        int k1 = bb->offsetAt(ii) + k / 2;
        int k2 = b->offsetAt(j) + k;
        ASSERT_TRUE(bb->mapKeys()->equalValueAt(b->mapKeys().get(), k1, k2));
        ASSERT_TRUE(
            bb->mapValues()->equalValueAt(b->mapValues().get(), k1, k2));
      }
      auto* c = expected->childAt(2)->asUnchecked<ArrayVector>();
      auto* cc = actual->childAt(2)->loadedVector()->asUnchecked<ArrayVector>();
      if (c->isNullAt(j)) {
        ASSERT_TRUE(cc->isNullAt(ii));
      } else {
        ASSERT_FALSE(cc->isNullAt(ii));
        for (int k = 0; k < cc->sizeAt(ii); ++k) {
          int k1 = cc->offsetAt(ii) + k;
          int k2 = c->offsetAt(j) + k;
          ASSERT_TRUE(
              cc->elements()->equalValueAt(c->elements().get(), k1, k2));
        }
      }
      auto* dd = actual->childAt(3)->loadedVector()->asUnchecked<MapVector>();
      ASSERT_FALSE(dd->isNullAt(ii));
      ASSERT_EQ(dd->sizeAt(ii), 0);
      ++ii;
    }
  }
}

void E2EFilterTestBase::testMutationCornerCases() {
  test::VectorMaker vectorMaker(leafPool_.get());
  flushEveryNBatches_ = 1;
  std::vector<RowVectorPtr> batches;
  for (int i = 0; i < 10; ++i) {
    auto a = vectorMaker.flatVector<int64_t>(
        100, [&](auto j) { return 100 * i + j; });
    batches.push_back(vectorMaker.rowVector({"a"}, {a}));
  }
  auto& rowType = batches[0]->type();
  writeToMemory(rowType, batches, false);
  ReaderOptions readerOpts{leafPool_.get()};
  auto input = std::make_unique<BufferedInput>(
      std::make_shared<InMemoryReadFile>(sinkData_),
      readerOpts.getMemoryPool());
  auto reader = makeReader(readerOpts, std::move(input));

  // 1. Interleave batches with and without deletions.
  // 2. Whole batch deletion.
  // 3. Delete last a few rows in a batch.
  auto spec = std::make_shared<common::ScanSpec>("<root>");
  spec->addAllChildFields(*rowType);
  RowReaderOptions rowReaderOpts;
  setUpRowReaderOptions(rowReaderOpts, spec);
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto result = BaseVector::create(rowType, 0, leafPool_.get());
  constexpr int kReadBatchSize = 10;
  auto nwords = bits::nwords(kReadBatchSize);
  std::vector<uint64_t> allSet(nwords, static_cast<uint64_t>(-1));
  std::vector<uint64_t> oddSet(nwords, 0xAAAAAAAAAAAAAAAAull);
  std::vector<uint64_t> noneSet(nwords, 0);
  int64_t totalScanned = 0;
  for (int i = 0;; ++i) {
    auto nextRowNumber = rowReader->nextRowNumber();
    if (nextRowNumber == RowReader::kAtEnd) {
      break;
    }
    ASSERT_EQ(nextRowNumber, totalScanned);
    uint64_t size = kReadBatchSize;
    Mutation mutation;
    switch (i % 5) {
      case 0:
        mutation.deletedRows = allSet.data();
        break;
      case 1:
        mutation.deletedRows = oddSet.data();
        break;
      case 2:
        mutation.deletedRows = noneSet.data();
        break;
      case 3:
        size = kReadBatchSize + 2;
        break;
    }
    auto numScanned = rowReader->next(size, result, &mutation);
    ASSERT_GT(numScanned, 0);
    totalScanned += numScanned;
    auto* a = result->asUnchecked<RowVector>()
                  ->childAt(0)
                  ->loadedVector()
                  ->asFlatVector<int64_t>();
    if (i % 5 == 0) {
      ASSERT_EQ(result->size(), 0);
    } else if (i % 5 == 1) {
      ASSERT_EQ(2 * result->size(), numScanned);
      for (int j = 0; j < result->size(); ++j) {
        ASSERT_EQ(a->valueAtFast(j), nextRowNumber + j * 2);
      }
    } else {
      ASSERT_EQ(result->size(), numScanned);
      for (int j = 0; j < result->size(); ++j) {
        ASSERT_EQ(a->valueAtFast(j), nextRowNumber + j);
      }
    }
  }
  ASSERT_EQ(totalScanned, 1000);

  // No child reader.
  spec = std::make_shared<common::ScanSpec>("<root>");
  setUpRowReaderOptions(rowReaderOpts, spec);
  rowReader = reader->createRowReader(rowReaderOpts);
  result = BaseVector::create(ROW({}), 0, leafPool_.get());
  totalScanned = 0;
  for (;;) {
    auto nextRowNumber = rowReader->nextRowNumber();
    if (nextRowNumber == RowReader::kAtEnd) {
      break;
    }
    ASSERT_EQ(nextRowNumber, totalScanned);
    Mutation mutation;
    mutation.deletedRows = oddSet.data();
    auto numScanned = rowReader->next(kReadBatchSize, result, &mutation);
    ASSERT_GT(numScanned, 0);
    ASSERT_EQ(2 * result->size(), numScanned);
    totalScanned += numScanned;
  }
  ASSERT_EQ(totalScanned, 1000);
}

void OwnershipChecker::check(const VectorPtr& batch) {
  // Check the 6 first pairs of previous, next batch to see that
  // fetching the next does not overwrite parts reachable from a
  // retained reference to the previous one.
  if (batchCounter_ > 11) {
    return;
  }
  // We fill filter-only columns with nulls to make the RowVector well formed
  // for copy.
  auto rowVector = batch->as<RowVector>();
  // Columns corresponding to filter-only access will not be filled in and have
  // a zero-length or null child vector. Fill these in with nulls for the size
  // of the batch to make the batch well formed for copy.
  for (auto i = 0; i < rowVector->childrenSize(); ++i) {
    auto& child = rowVector->children()[i];
    if (!child || child->size() != batch->size()) {
      child = BaseVector::createNullConstant(
          batch->type()->childAt(i), batch->size(), batch->pool());
    }
  }
  if (batchCounter_ % 2 == 0) {
    previousBatch_ = std::make_shared<RowVector>(
        batch->pool(),
        batch->type(),
        BufferPtr(nullptr),
        batch->size(),
        batch->as<RowVector>()->children());
    previousBatchCopy_ = BaseVector::copy(*batch);
  }
  if (batchCounter_ % 2 == 1) {
    for (auto i = 0; i < previousBatch_->size(); ++i) {
      ASSERT_TRUE(previousBatch_->equalValueAt(previousBatchCopy_.get(), i, i))
          << "Retained reference of a batch has been overwritten by the next: "
          << " index  " << i << " batch " << previousBatch_->toString(i)
          << " original " << previousBatchCopy_->toString(i);
    }
  }
  ++batchCounter_;
}

} // namespace facebook::velox::dwio::common
