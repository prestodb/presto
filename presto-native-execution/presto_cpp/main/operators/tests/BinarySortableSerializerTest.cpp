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
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <algorithm>

#include "velox/core/PlanNode.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "presto_cpp/main/operators/BinarySortableSerializer.h"

namespace facebook::presto::operators::test {
namespace {
// return -1 if key1 < key2, 0 if key1 == key2, 1 if key1 > key 2
int lexicographicalCompare(std::string key1, std::string key2) {
  // doing unsinged byte comparison following the Cosco test suite's semantic.
  const auto begin1 = reinterpret_cast<unsigned char*>(key1.data());
  const auto end1 = begin1 + key1.size();
  const auto begin2 = reinterpret_cast<unsigned char*>(key2.data());
  const auto end2 = begin2 + key2.size();
  bool lessThan = std::lexicographical_compare(begin1, end1, begin2, end2);

  bool equal = std::equal(begin1, end1, begin2, end2);

  return lessThan ? -1 : (equal ? 0 : 1);
}

void serializeRow(
    const BinarySortableSerializer& binarySortableSerializer,
    velox::vector_size_t index,
    velox::vector_size_t offset,
    velox::StringVectorBuffer* out) {
  binarySortableSerializer.serialize(/*rowId=*/index + offset, out);
  out->flushRow(index);
}

class BinarySortableSerializerTest : public ::testing::Test,
                                     public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
  }

  int compareRowVector(
      const velox::RowVectorPtr& rowVector,
      const std::vector<
          std::shared_ptr<const velox::core::FieldAccessTypedExpr>>& fields,
      const std::vector<velox::core::SortOrder>& ordering) {
    VELOX_CHECK_EQ(rowVector->size(), 2);
    BinarySortableSerializer binarySortableSerializer(
        rowVector, ordering, fields);

    auto vector =
        velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
            velox::VARBINARY(), 2, pool_.get());
    // Create a ResizableVectorBuffer with initial and max capacity.
    velox::StringVectorBuffer buffer(vector.get(), 10, 1000);
    serializeRow(binarySortableSerializer, /*index=*/0, /*offset=*/0, &buffer);
    serializeRow(binarySortableSerializer, /*index=*/1, /*offset=*/0, &buffer);

    return lexicographicalCompare(vector->valueAt(0), vector->valueAt(1));
  }

  template <typename T>
  int singlePrimitiveFieldCompare(
      const std::vector<std::optional<T>>& values,
      const velox::core::SortOrder& ordering) {
    VELOX_CHECK_EQ(values.size(), 2);
    auto c0 = vectorMaker_.flatVectorNullable<T>(values);
    auto rowVector = vectorMaker_.rowVector({c0});

    std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
        fields;
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::CppToType<T>::create(), /*name=*/"c0"));
    return compareRowVector(rowVector, fields, {ordering});
  }

  template <typename T>
  int singleArrayFieldCompare(
      const std::vector<std::optional<std::vector<std::optional<T>>>>& values,
      const velox::core::SortOrder& ordering) {
    VELOX_CHECK_EQ(values.size(), 2);
    auto c0 = makeNullableArrayVector<T>(values);
    // auto c0 = makeNullableArrayVector<T>(values);
    auto rowVector = vectorMaker_.rowVector({c0});

    std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
        fields;
    fields.push_back(std::make_shared<velox::core::FieldAccessTypedExpr>(
        velox::CppToType<T>::create(), /*name=*/"c0"));
    return compareRowVector(rowVector, fields, {ordering});
  }

  template <typename T0, typename T1>
  int singleRowFieldCompare(
      const std::vector<std::optional<T0>>& col0,
      const std::vector<std::optional<T1>>& col1,
      const velox::core::SortOrder& ordering,
      const std::function<bool(velox::vector_size_t /* row */)>& isNullAt =
          nullptr) {
    VELOX_CHECK_EQ(col0.size(), 2);
    VELOX_CHECK_EQ(col1.size(), 2);
    auto c0 = vectorMaker_.flatVectorNullable<T0>(col0);
    auto c1 = vectorMaker_.flatVectorNullable<T1>(col1);
    auto rowField = vectorMaker_.rowVector({c0, c1});
    auto rowVector = vectorMaker_.rowVector({rowField});

    if (isNullAt) {
      rowVector->childAt(0)->setNull(0, isNullAt(0));
      rowVector->childAt(0)->setNull(1, isNullAt(1));
    }
    std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
        fields;
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        rowField->type(), /*name=*/"c0"));
    return compareRowVector(rowVector, fields, {ordering});
  }

  std::shared_ptr<velox::memory::MemoryPool> pool_ =
      velox::memory::deprecatedAddDefaultLeafMemoryPool();
  velox::test::VectorMaker vectorMaker_{pool_.get()};
  std::unique_ptr<velox::StreamArena> streamArena_ =
      std::make_unique<velox::StreamArena>(pool_.get());
};

class BinarySortableSerializerFuzzerTest : public ::testing::Test,
                                           public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
  }

  void runFuzzerTest(const velox::RowTypePtr& rowType) {
    const auto seed = 1;
    // Create a random number generator and seed it
    std::mt19937 rng(seed);

    const boost::random::uniform_int_distribution<int> distribution(
        0, sortingOrders_.size() - 1);

    // Generate random sort ordering
    std::vector<velox::core::SortOrder> testOrdering;
    for (uint32_t i = 0; i < rowType->size(); ++i) {
      const int randomIndex = distribution(rng);
      testOrdering.push_back(sortingOrders_[randomIndex]);
    }

    const auto rowVector = makeData(rowType);
    const auto fields = getFields(rowType);

    checkSizeCalculation(rowVector, fields, testOrdering);
    checkBatchedSizeCalculation(
        rowVector, /*offset=*/0, rowVector->size(), fields, testOrdering);
    ensureSorted(rowVector, fields, testOrdering);
  }

  void runFuzzerTestBatchedSizeCalculation(
      const velox::RowTypePtr& rowType,
      velox::vector_size_t batchSize) {
    const auto seed = 1;
    // Create a random number generator and seed it
    std::mt19937 rng(seed);

    const boost::random::uniform_int_distribution<int> distribution(
        0, sortingOrders_.size() - 1);

    // Generate random sort ordering
    std::vector<velox::core::SortOrder> testOrdering;
    for (uint32_t i = 0; i < rowType->size(); ++i) {
      const int randomIndex = distribution(rng);
      testOrdering.push_back(sortingOrders_[randomIndex]);
    }

    const auto rowVector = makeData(rowType);
    const auto fields = getFields(rowType);

    for (velox::vector_size_t offset = 0; offset + batchSize < rowVector->size();
         offset += batchSize) {
      checkBatchedSizeCalculation(
          rowVector, offset, batchSize, fields, testOrdering);
    }
  }

  bool sortedAfterSerialization(
      const velox::RowVectorPtr& rowVector,
      const std::vector<
          std::shared_ptr<const velox::core::FieldAccessTypedExpr>>& fields,
      const std::vector<velox::core::SortOrder>& ordering,
      int vectorSize) {
    BinarySortableSerializer binarySortableSerializer(
        rowVector, ordering, fields);

    auto vec = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        velox::VARBINARY(), vectorSize, pool_.get());
    // Create a ResizableVectorBuffer with initial and max capacity.
    velox::StringVectorBuffer buffer(vec.get(), 1024, 1 << 20);
    for (velox::vector_size_t i = 0; i < vectorSize; ++i) {
      serializeRow(
          binarySortableSerializer, /*index=*/i, /*offset=*/0, &buffer);
    }

    for (velox::vector_size_t i = 0; i < vec->size() - 1; ++i) {
      if (lexicographicalCompare(vec->valueAt(i), vec->valueAt(i + 1)) > 0) {
        return false;
      }
    }
    return true; // ensure all elements are in non-descending order
  }

  void ensureSorted(
      const velox::RowVectorPtr& input,
      const std::vector<
          std::shared_ptr<const velox::core::FieldAccessTypedExpr>>& keys,
      const std::vector<velox::core::SortOrder>& ordering) {
    const auto planNode = std::make_shared<velox::core::OrderByNode>(
        "orderBy",
        keys,
        ordering,
        false, // isPartial
        std::make_shared<velox::core::ValuesNode>(
            "values", std::vector<velox::RowVectorPtr>{input}));

    velox::exec::test::AssertQueryBuilder builder(planNode);
    const auto sortedVector = builder.copyResults(pool_.get());
    // Ensure that sorting order is preserved after serialization.
    EXPECT_TRUE(sortedAfterSerialization(
        sortedVector, keys, ordering, sortedVector->size()));
  }

  void checkSizeCalculation(
      const velox::RowVectorPtr& input,
      const std::vector<
          std::shared_ptr<const velox::core::FieldAccessTypedExpr>>& keys,
      const std::vector<velox::core::SortOrder>& ordering) {
    BinarySortableSerializer binarySortableSerializer(input, ordering, keys);
    auto vec = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        velox::VARBINARY(), input->size(), pool_.get());
    // Create a ResizableVectorBuffer with initial and max capacity.
    velox::StringVectorBuffer buffer(vec.get(), 1024, 1 << 20);
    for (velox::vector_size_t i = 0; i < input->size(); ++i) {
      const size_t expectedSize =
          binarySortableSerializer.serializedSizeInBytes(i);
      serializeRow(
          binarySortableSerializer, /*index=*/i, /*offset=*/0, &buffer);
      EXPECT_EQ(expectedSize, vec->valueAt(i).size());
    }
  }

  void checkBatchedSizeCalculation(
      const velox::RowVectorPtr& input,
      velox::vector_size_t offset,
      velox::vector_size_t size,
      const std::vector<
          std::shared_ptr<const velox::core::FieldAccessTypedExpr>>& keys,
      const std::vector<velox::core::SortOrder>& ordering) {
    BinarySortableSerializer binarySortableSerializer(input, ordering, keys);

    // Create size variables and pointer array for batched call
    std::vector<velox::vector_size_t> sizes(size, 0);
    std::vector<velox::vector_size_t*> sizePointers(size);
    for (velox::vector_size_t i = 0; i < size; ++i) {
      sizePointers[i] = &sizes[i];
    }

    // Call batched size calculation
    velox::Scratch scratch;
    binarySortableSerializer.serializedSizeInBytes(
        offset, size, sizePointers.data(), scratch);

    // Create vector for actual serialization to verify sizes
    auto vec = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        velox::VARBINARY(), size, pool_.get());
    velox::StringVectorBuffer buffer(vec.get(), 1024, 1 << 20);

    // Serialize each row and compare with batched size calculation
    for (velox::vector_size_t i = 0; i < size; ++i) {
      serializeRow(binarySortableSerializer, /*index=*/i, offset, &buffer);
      EXPECT_EQ(sizes[i], vec->valueAt(i).size())
          << "Batched size calculation mismatch at row " << i << ": expected "
          << sizes[i] << ", actual " << vec->valueAt(i).size();
    }
  }

  velox::RowVectorPtr makeData(const velox::RowTypePtr& rowType) {
    velox::VectorFuzzer::Options options;
    options.vectorSize = 1'000;
    options.allowDictionaryVector = true;
    options.allowConstantVector = true;
    options.stringVariableLength = true;
    options.containerVariableLength = true;
    options.allowLazyVector = true;
    options.nullRatio = 0.1;

    const auto seed = folly::Random::rand32();

    // For reproducibility.
    LOG(ERROR) << "Seed: " << seed;
    SCOPED_TRACE(fmt::format("seed: {}", seed));
    velox::VectorFuzzer fuzzer(options, pool_.get(), seed);

    return fuzzer.fuzzInputRow(rowType);
  }

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
  getFields(const velox::RowTypePtr& rowType) {
    std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>>
        fieldAccessExprs;
    for (const auto& fieldName : rowType->names()) {
      auto fieldExpr = std::make_shared<velox::core::FieldAccessTypedExpr>(
          rowType->findChild(fieldName), fieldName);
      fieldAccessExprs.push_back(std::move(fieldExpr));
    }
    return fieldAccessExprs;
  }

  const std::vector<velox::core::SortOrder> sortingOrders_ = {
      velox::core::kAscNullsFirst,
      velox::core::kAscNullsLast,
      velox::core::kDescNullsFirst,
      velox::core::kDescNullsLast};
  std::shared_ptr<velox::memory::MemoryPool> pool_ =
      velox::memory::deprecatedAddDefaultLeafMemoryPool();
  velox::test::VectorMaker vectorMaker_{pool_.get()};
};
} // namespace

TEST_F(BinarySortableSerializerFuzzerTest, fuzzerTestString) {
  const auto rowType =
      velox::ROW({"c1", "c2"}, {velox::BIGINT(), velox::VARCHAR()});
  runFuzzerTest(rowType);
}

TEST_F(BinarySortableSerializerFuzzerTest, fuzzerTestArray) {
  const auto rowType = velox::ROW(
      {"c1", "c2", "c3"},
      {velox::BIGINT(),
       velox::ARRAY(velox::BIGINT()),
       velox::ARRAY(velox::VARBINARY())});
  runFuzzerTest(rowType);
}

TEST_F(BinarySortableSerializerFuzzerTest, fuzzerTestStruct) {
  const auto rowType = velox::ROW(
      {"c1", "c2"},
      {velox::BIGINT(),
       velox::ROW(
           {velox::BIGINT(),
            velox::DOUBLE(),
            velox::BOOLEAN(),
            velox::TINYINT(),
            velox::REAL()})});
  runFuzzerTest(rowType);
}

TEST_F(BinarySortableSerializerFuzzerTest, fuzzerTestNestedStruct) {
  const auto innerRowType = velox::ROW(
      {velox::BIGINT(),
       velox::DOUBLE(),
       velox::VARCHAR(),
       velox::ROW({velox::TINYINT(), velox::VARCHAR()})});
  const auto rowType =
      velox::ROW({"c0", "c1"}, {velox::BIGINT(), innerRowType});
  runFuzzerTest(rowType);
}

TEST_F(BinarySortableSerializerFuzzerTest, fuzzerTestBatchedSizeCalculation) {
  const auto innerRowType = velox::ROW(
      {velox::ARRAY(velox::VARCHAR()),
       velox::DOUBLE(),
       velox::VARCHAR(),
       velox::ROW({velox::TINYINT(), velox::VARCHAR()})});
  const auto rowType =
      velox::ROW({"c0", "c1"}, {velox::BIGINT(), innerRowType});

  runFuzzerTestBatchedSizeCalculation(rowType, 10);
}

TEST_F(BinarySortableSerializerTest, LongTypeAllFields) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  // (0,1,2) < (2,1,0)
  EXPECT_TRUE(result < 0);
}

TEST_F(BinarySortableSerializerTest, LongTypeAllFieldsDescending) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kDescNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  // (0d,1,2) > (2d,1,0)
  EXPECT_TRUE(result > 0);
}

TEST_F(BinarySortableSerializerTest, LongTypeAllFieldsWithNulls) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  // (null,null,2) > (null,null,0)
  EXPECT_TRUE(result > 0);
}

TEST_F(BinarySortableSerializerTest, DefaultNullsOrdering) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({0, std::nullopt});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  // (null,null,0) > (null,null,null)
  EXPECT_TRUE(result > 0);
}

TEST_F(
    BinarySortableSerializerTest,
    ExplicitNullsOrdering_ascending_nulls_last) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsLast),
      velox::core::SortOrder(velox::core::kAscNullsLast)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({0, std::nullopt});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  // (null,null,0) < (null,null,null)
  EXPECT_TRUE(result < 0);
}

TEST_F(
    BinarySortableSerializerTest,
    ExplicitNullsOrdering_descending_nulls_last) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kDescNullsLast),
      velox::core::SortOrder(velox::core::kDescNullsLast),
      velox::core::SortOrder(velox::core::kDescNullsLast)};
  auto c0 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c1 =
      vectorMaker_.flatVectorNullable<int64_t>({std::nullopt, std::nullopt});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({0, std::nullopt});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  // (null,null,0) < (null,null,null)
  EXPECT_TRUE(result < 0);
}

TEST_F(BinarySortableSerializerTest, LongTypeSubsetOfFields) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
      velox::BIGINT(), /*name=*/"c2"));
  fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
      velox::BIGINT(), /*name=*/"c0"));

  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  // (2, 0) > (0, 2)
  EXPECT_TRUE(result > 0);
}

TEST_F(BinarySortableSerializerTest, ComplexTypeDictionaryEncoded) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields =
      {
          std::make_shared<const velox::core::FieldAccessTypedExpr>(
              velox::ROW(
                  {"f0", "f1", "f2"},
                  {velox::BIGINT(), velox::BIGINT(), velox::BIGINT()}),
              "c0"),
          std::make_shared<const velox::core::FieldAccessTypedExpr>(
              velox::ARRAY(velox::INTEGER()), "c1"),
      };

  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({2, 0});

  auto baseRow = vectorMaker_.rowVector({"f0", "f1", "f2"}, {c0, c1, c2});
  auto baseArray = makeArrayVector<int32_t>({{1, 2, 3}, {4, 5, 6}});
  auto rowVector = vectorMaker_.rowVector(
      {"c0", "c1"},
      {
          velox::BaseVector::wrapInDictionary(
              nullptr, makeIndicesInReverse(2), 2, baseRow),
          velox::BaseVector::wrapInDictionary(
              nullptr, makeIndicesInReverse(2), 2, baseArray),
      });

  auto result = compareRowVector(rowVector, fields, ordering);

  // 0: {{0, 1, 2}, [1, 2, 3]} < 1: {{2, 1, 0}, [4, 5,6]}
  // the dictionary indices are reversed, so result is > 0.
  EXPECT_TRUE(result > 0);
}

TEST_F(BinarySortableSerializerTest, DictionaryEncodedLazyVector) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst)};

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields =
      {
          std::make_shared<const velox::core::FieldAccessTypedExpr>(
              velox::ROW(
                  {"f0", "f1", "f2"},
                  {velox::BIGINT(), velox::BIGINT(), velox::BIGINT()}),
              "c0"),
          std::make_shared<const velox::core::FieldAccessTypedExpr>(
              velox::ARRAY(velox::INTEGER()), "c1"),
      };

  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.flatVectorNullable<int64_t>({1, 2});

  auto baseRow = vectorMaker_.rowVector({"f0", "f1", "f2"}, {c0, c1, c2});
  auto baseArray = makeArrayVector<int32_t>({{1, 2, 3}, {4, 5, 6}});
  auto rowVector = vectorMaker_.rowVector(
      {"c0", "c1"},
      {
          velox::VectorFuzzer::wrapInLazyVector(
              velox::BaseVector::wrapInDictionary(
                  nullptr, makeIndicesInReverse(2), 2, baseRow)),
          velox::VectorFuzzer::wrapInLazyVector(
              velox::BaseVector::wrapInDictionary(
                  nullptr, makeIndicesInReverse(2), 2, baseArray)),
      });

  auto result = compareRowVector(rowVector, fields, ordering);

  // 0: {{0, 1, 1}, [1, 2, 3]} < 1: {{2, 1, 2}, [4, 5,6]}
  // the dictionary indices are reversed, so result is > 0.
  EXPECT_TRUE(result > 0);
}

TEST_F(BinarySortableSerializerTest, ConstantVector) {
  auto ordering = {
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsFirst),
      velox::core::SortOrder(velox::core::kAscNullsLast)};
  auto c0 = vectorMaker_.flatVectorNullable<int64_t>({0, 2});
  auto c1 = vectorMaker_.flatVectorNullable<int64_t>({1, 1});
  auto c2 = vectorMaker_.constantVector<int64_t>({2});

  std::vector<std::shared_ptr<const velox::core::FieldAccessTypedExpr>> fields;
  fields.reserve(3);
  for (int32_t i = 0; i < 3; ++i) {
    fields.push_back(std::make_shared<const velox::core::FieldAccessTypedExpr>(
        velox::BIGINT(), fmt::format("c{}", i)));
  }
  auto rowVector = vectorMaker_.rowVector({c0, c1, c2});

  auto result = compareRowVector(rowVector, fields, ordering);

  // (0,1,2) < (2,1,2)
  EXPECT_TRUE(result < 0);
}

TEST_F(BinarySortableSerializerTest, BooleanTypeSingleFieldTests) {
  // true == true
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {true, true}, velox::core::kAscNullsFirst) == 0);

  // false < true
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {false, true}, velox::core::kAscNullsFirst) < 0);

  // true < false (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {true, false}, velox::core::kDescNullsLast) < 0);

  // null < false
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<bool>(
          {std::nullopt, false}, velox::core::kAscNullsFirst) < 0);
}

TEST_F(BinarySortableSerializerTest, DoubleTypeSingleFieldTests) {
  // 0.1 == 0.1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {0.1, 0.1}, velox::core::kAscNullsFirst) == 0);

  // 0.01 < 0.1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {0.01, 0.1}, velox::core::kAscNullsFirst) < 0);

  // 0.1 < 0.01 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {0.1, 0.01}, velox::core::kDescNullsLast) < 0);

  // Double.NaN == Double.NaN
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {std::numeric_limits<double>::quiet_NaN(),
           std::numeric_limits<double>::quiet_NaN()},
          velox::core::kAscNullsFirst) == 0);

  // null < Double.NaN
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {std::nullopt, std::numeric_limits<double>::quiet_NaN()},
          velox::core::kAscNullsFirst) < 0);

  // null < Double.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<double>(
          {std::nullopt, std::numeric_limits<double>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(BinarySortableSerializerTest, FloatTypeSingleFieldTests) {
  // 0.1 == 0.1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {0.1f, 0.1f}, velox::core::kAscNullsFirst) == 0);

  // 0.01 < 0.1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {0.01f, 0.1f}, velox::core::kAscNullsFirst) < 0);

  // 0.1 < 0.01 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {0.1f, 0.01f}, velox::core::kDescNullsLast) < 0);

  // Float.NaN == Float.NaN
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {std::numeric_limits<float>::quiet_NaN(),
           std::numeric_limits<float>::quiet_NaN()},
          velox::core::kAscNullsFirst) == 0);

  // null < Float.NaN
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {std::nullopt, std::numeric_limits<float>::quiet_NaN()},
          velox::core::kAscNullsFirst) < 0);

  // null < Float.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<float>(
          {std::nullopt, std::numeric_limits<float>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(BinarySortableSerializerTest, ByteTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>({1, 2}, velox::core::kAscNullsFirst) <
      0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>({2, 1}, velox::core::kDescNullsLast) <
      0);

  // Byte.MinValue < Byte.MaxValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::numeric_limits<int8_t>::min(),
           std::numeric_limits<int8_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  // Byte.MaxValue < Byte.MinValue (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::numeric_limits<int8_t>::min(),
           std::numeric_limits<int8_t>::max()},
          velox::core::kDescNullsLast) > 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  // null < Byte.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int8_t>(
          {std::nullopt, std::numeric_limits<int8_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(BinarySortableSerializerTest, ShortTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  // Short.MinValue < Short.MaxValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::numeric_limits<int16_t>::min(),
           std::numeric_limits<int16_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  // Short.MaxValue < Short.MinValue (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::numeric_limits<int16_t>::min(),
           std::numeric_limits<int16_t>::max()},
          velox::core::kDescNullsLast) > 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  // null < Short.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int16_t>(
          {std::nullopt, std::numeric_limits<int16_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(BinarySortableSerializerTest, IntegerTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  // Int.MinValue < Int.MaxValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::numeric_limits<int32_t>::min(),
           std::numeric_limits<int32_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  // Int.MaxValue < Int.MinValue (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::numeric_limits<int32_t>::min(),
           std::numeric_limits<int32_t>::max()},
          velox::core::kDescNullsLast) > 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  // null < Int.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::nullopt, std::numeric_limits<int32_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(BinarySortableSerializerTest, DateTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int32_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);
}

TEST_F(BinarySortableSerializerTest, LongTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {1, 1}, velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {1, 2}, velox::core::kAscNullsFirst) < 0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {2, 1}, velox::core::kDescNullsLast) < 0);

  // Long.MinValue < Long.MaxValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::numeric_limits<int64_t>::min(),
           std::numeric_limits<int64_t>::max()},
          velox::core::kAscNullsFirst) < 0);

  // Long.MaxValue < Long.MinValue (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::numeric_limits<int64_t>::min(),
           std::numeric_limits<int64_t>::max()},
          velox::core::kDescNullsLast) > 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::nullopt, 1}, velox::core::kAscNullsFirst) < 0);

  // null < Long.MinValue
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<int64_t>(
          {std::nullopt, std::numeric_limits<int64_t>::min()},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(BinarySortableSerializerTest, TimestampTypeSingleFieldTests) {
  // 1 == 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {velox::Timestamp::fromMicros(1), velox::Timestamp::fromMicros(1)},
          velox::core::kAscNullsFirst) == 0);

  // 1 < 2
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {velox::Timestamp::fromMicros(1), velox::Timestamp::fromMicros(2)},
          velox::core::kAscNullsFirst) < 0);

  // 2 < 1 (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {velox::Timestamp::fromMicros(2), velox::Timestamp::fromMicros(1)},
          velox::core::kDescNullsLast) < 0);

  // null < 1
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<velox::Timestamp>(
          {std::nullopt, velox::Timestamp::fromMicros(1)},
          velox::core::kAscNullsFirst) < 0);
}

TEST_F(BinarySortableSerializerTest, StringTypeSingleFieldTests) {
  // "abc" == "abc"
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"abc", "abc"}, velox::core::kAscNullsFirst) == 0);

  // "aaa" < "abc"
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"aaa", "abc"}, velox::core::kAscNullsFirst) < 0);

  // "aaa" < "aaaa"
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"aaa", "aaaa"}, velox::core::kAscNullsFirst) < 0);

  // "abc" < "aaa" (descending)
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {"abc", "aaa"}, velox::core::kDescNullsLast) < 0);

  // null < ""
  EXPECT_TRUE(
      singlePrimitiveFieldCompare<std::string>(
          {std::nullopt, ""}, velox::core::kAscNullsFirst) < 0);
}

TEST_F(BinarySortableSerializerTest, ArrayTypeSingleFieldTests) {
  // [1, 1] == [1, 1]
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L}}, {{1L, 1L}}}, velox::core::kAscNullsFirst) == 0);

  // [1, 1] < [1, 1, 1]
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L}}, {{1L, 1L, 1L}}}, velox::core::kAscNullsFirst) < 0);

  // [1, 1, 1] < [1, 1] (descending)
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L, 1L}}, {{1L, 1L}}}, velox::core::kDescNullsLast) < 0);

  // [1, 1, 1] < [1, 2]
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, 1L, 1L}}, {{1L, 2L}}}, velox::core::kAscNullsFirst) < 0);

  //[ 1, null, 1 ] < [ 1, 0, 1 ]
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {{{1L, std::nullopt, 1L}}, {{1L, 0L, 1L}}},
          velox::core::kAscNullsFirst) < 0);

  // null < [1]
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {std::nullopt, {{1L}}}, velox::core::kAscNullsFirst) < 0);

  // null < [null]
  std::vector<std::optional<int64_t>> arrayWithNull{std::nullopt};
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {std::nullopt, arrayWithNull}, velox::core::kAscNullsFirst) < 0);

  // null < []
  EXPECT_TRUE(
      singleArrayFieldCompare<int64_t>(
          {std::nullopt, {std::vector<std::optional<int64_t>>{}}}, velox::core::kAscNullsFirst) < 0);
}

TEST_F(BinarySortableSerializerTest, RowTypeSingleFieldTests) {
  // (1, "aaa") == (1, "aaa")
  int cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {"aaa", "aaa"}, velox::core::kAscNullsFirst);
  EXPECT_TRUE(cmp == 0);

  // (1, "aaa") < (1, "abc")
  cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {"aaa", "abc"}, velox::core::kAscNullsFirst);
  EXPECT_TRUE(cmp < 0);

  // (1, "abc") < (1, "aaa") (descending)
  cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {"aaa", "abc"}, velox::core::kDescNullsLast);
  EXPECT_TRUE(cmp > 0);

  // (1, null) < (1, "abc")
  cmp = singleRowFieldCompare<int64_t, std::string>(
      {1L, 1L}, {std::nullopt, "abc"}, velox::core::kAscNullsFirst);
  EXPECT_TRUE(cmp < 0);

  // null < (null, null)
  cmp = singleRowFieldCompare<int64_t, std::string>(
      {std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt},
      velox::core::kAscNullsFirst,
      [](velox::vector_size_t row) { return row == 0 ? true : false; });
  EXPECT_TRUE(cmp < 0);
}
} // namespace facebook::presto::operators::test
