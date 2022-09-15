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

#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/vector/tests/utils/VectorMaker.h"

#include <gtest/gtest.h>

namespace facebook::velox::dwio::parquet {

class ParquetReaderTestBase : public testing::Test {
 protected:
  dwio::common::RowReaderOptions getReaderOpts(const RowTypePtr& rowType) {
    dwio::common::RowReaderOptions rowReaderOpts;
    rowReaderOpts.select(
        std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
            rowType, rowType->names()));

    return rowReaderOpts;
  }

  static RowTypePtr sampleSchema() {
    return ROW({"a", "b"}, {BIGINT(), DOUBLE()});
  }

  static RowTypePtr dateSchema() {
    return ROW({"date"}, {DATE()});
  }

  static RowTypePtr intSchema() {
    return ROW({"int", "bigint"}, {INTEGER(), BIGINT()});
  }

  template <typename T>
  VectorPtr rangeVector(size_t size, T start) {
    std::vector<T> vals(size);
    for (size_t i = 0; i < size; ++i) {
      vals[i] = start + static_cast<T>(i);
    }
    return vectorMaker_->flatVector(vals);
  }

  // Check that actual vector is equal to a part of expected vector
  // at a specified offset.
  void assertEqualVectorPart(
      const VectorPtr& expected,
      const VectorPtr& actual,
      vector_size_t offset) {
    ASSERT_GE(expected->size(), actual->size() + offset);
    ASSERT_EQ(expected->typeKind(), actual->typeKind());
    for (vector_size_t i = 0; i < actual->size(); i++) {
      ASSERT_TRUE(expected->equalValueAt(actual.get(), i + offset, i))
          << "at " << (i + offset) << ": expected "
          << expected->toString(i + offset) << ", but got "
          << actual->toString(i);
    }
  }

  void assertReadExpected(
      std::shared_ptr<const RowType> outputType,
      dwio::common::RowReader& reader,
      RowVectorPtr expected,
      memory::MemoryPool& memoryPool) {
    uint64_t total = 0;
    VectorPtr result = BaseVector::create(outputType, 0, &memoryPool);

    while (total < expected->size()) {
      auto part = reader.next(1000, result);
      EXPECT_GT(part, 0);
      if (part > 0) {
        assertEqualVectorPart(expected, result, total);
        total += part;
      } else {
        break;
      }
    }
    EXPECT_EQ(total, expected->size());
    EXPECT_EQ(reader.next(1000, result), 0);
  }

  void assertReadExpected(
      dwio::common::RowReader& reader,
      RowVectorPtr expected) {
    uint64_t total = 0;
    VectorPtr result;
    while (total < expected->size()) {
      auto part = reader.next(1000, result);
      EXPECT_GT(part, 0);
      if (part > 0) {
        assertEqualVectorPart(expected, result, total);
        total += part;
      } else {
        break;
      }
    }
    EXPECT_EQ(total, expected->size());
    EXPECT_EQ(reader.next(1000, result), 0);
  }

  std::shared_ptr<velox::common::ScanSpec> makeScanSpec(
      const RowTypePtr& rowType) {
    auto scanSpec = std::make_shared<velox::common::ScanSpec>("");

    for (auto i = 0; i < rowType->size(); ++i) {
      auto child = scanSpec->getOrCreateChild(
          velox::common::Subfield(rowType->nameOf(i)));
      child->setProjectOut(true);
      child->setChannel(i);
    }

    return scanSpec;
  }

  using FilterMap =
      std::unordered_map<std::string, std::unique_ptr<velox::common::Filter>>;

  void assertReadWithReaderAndFilters(
      const std::unique_ptr<dwio::common::Reader> reader,
      const std::string& /* fileName */,
      const RowTypePtr& fileSchema,
      FilterMap filters,
      const RowVectorPtr& expected) {
    auto scanSpec = makeScanSpec(fileSchema);
    for (auto&& [column, filter] : filters) {
      scanSpec->getOrCreateChild(velox::common::Subfield(column))
          ->setFilter(std::move(filter));
    }

    auto rowReaderOpts = getReaderOpts(fileSchema);
    rowReaderOpts.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOpts);
    assertReadExpected(*rowReader, expected);
  }

  std::string getExampleFilePath(const std::string& fileName) {
    return test::getDataFilePath(
        "velox/dwio/parquet/tests/reader", "../examples/" + fileName);
  }

  std::unique_ptr<memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  std::unique_ptr<test::VectorMaker> vectorMaker_{
      std::make_unique<test::VectorMaker>(pool_.get())};
};
} // namespace facebook::velox::dwio::parquet
