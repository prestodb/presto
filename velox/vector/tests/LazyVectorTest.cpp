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

#include <gtest/gtest.h>

#include "velox/common/base/RawVector.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class LazyVectorTest : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(LazyVectorTest, lazyInDictionary) {
  // We have a dictionary over LazyVector. We load for some indices in
  // the dictionary. We check that the loads on the wrapped lazy
  // vector are properly translated and deduplicated.
  static constexpr int32_t kInnerSize = 100;
  static constexpr int32_t kOuterSize = 1000;
  std::vector<vector_size_t> loadedRows;
  auto lazy = std::make_shared<LazyVector>(
      pool_.get(),
      INTEGER(),
      kInnerSize,
      std::make_unique<test::SimpleVectorLoader>([&](auto rows) {
        for (auto row : rows) {
          loadedRows.push_back(row);
        }
        return makeFlatVector<int32_t>(
            rows.back() + 1, [](auto row) { return row; });
      }));
  auto wrapped = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(kOuterSize, [](auto row) { return row / 10; }),
      kOuterSize,
      lazy);

  // We expect a single level of dictionary and rows loaded for the selected
  // indices in rows.

  SelectivityVector rows(kOuterSize, false);
  // We select 3 rows, the 2 first fall on 0 and the last on 5 in 'base'.
  rows.setValid(1, true);
  rows.setValid(9, true);
  rows.setValid(55, true);
  rows.updateBounds();
  LazyVector::ensureLoadedRows(wrapped, rows);
  EXPECT_EQ(wrapped->encoding(), VectorEncoding::Simple::DICTIONARY);
  EXPECT_EQ(wrapped->valueVector()->encoding(), VectorEncoding::Simple::FLAT);

  EXPECT_EQ(loadedRows, (std::vector<vector_size_t>{0, 5}));
  assertCopyableVector(wrapped);
}

TEST_F(LazyVectorTest, rowVectorWithLazyChild) {
  constexpr vector_size_t size = 1000;
  auto columnType = ROW({"a", "b"}, {INTEGER(), INTEGER()});

  auto lazyVectorA = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [&](vector_size_t i) { return i % 5; },
      [](vector_size_t i) { return i % 7 == 0; });
  auto lazyVectorB = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [&](vector_size_t i) { return i % 3; },
      [](vector_size_t i) { return i % 11 == 0; });

  VectorPtr rowVector = makeRowVector({lazyVectorA, lazyVectorB});
  EXPECT_TRUE(isLazyNotLoaded(*rowVector.get()));

  SelectivityVector rows(rowVector->size(), false);
  LazyVector::ensureLoadedRows(rowVector, rows);
  EXPECT_FALSE(isLazyNotLoaded(*rowVector.get()));
}

TEST_F(LazyVectorTest, rowVectorWithLazyChildPartiallyLoaded) {
  constexpr vector_size_t size = 1000;
  auto columnType = ROW({"a", "b"}, {INTEGER(), INTEGER()});

  auto lazyVectorA = vectorMaker_.lazyFlatVector<int32_t>(
      size, [&](vector_size_t) { return 2222; });
  auto lazyVectorB = vectorMaker_.lazyFlatVector<int32_t>(
      size, [&](vector_size_t) { return 1111; });

  VectorPtr rowVector = makeRowVector({lazyVectorA, lazyVectorB});
  EXPECT_FALSE(lazyVectorA->isLoaded());
  EXPECT_TRUE(isLazyNotLoaded(*rowVector.get()));

  // Only load first row, the row vector should still be valid.
  SelectivityVector rows(1);
  LazyVector::ensureLoadedRows(rowVector, rows);
  EXPECT_FALSE(isLazyNotLoaded(*rowVector.get()));

  auto rowVectorPtr = rowVector->asUnchecked<RowVector>();
  auto child1 = rowVectorPtr->childAt(0);
  auto child2 = rowVectorPtr->childAt(1);

  EXPECT_FALSE(child1->isNullAt(0));
  EXPECT_FALSE(child2->isNullAt(0));
  EXPECT_EQ(child1->loadedVector()->asFlatVector<int32_t>()->valueAt(0), 2222);
  EXPECT_EQ(child2->loadedVector()->asFlatVector<int32_t>()->valueAt(0), 1111);
  EXPECT_FALSE(rowVectorPtr->childAt(1)->isNullAt(0));

  EXPECT_EQ(rowVectorPtr->childAt(0)->size(), 1000);
  EXPECT_EQ(rowVectorPtr->childAt(1)->size(), 1000);
}

TEST_F(LazyVectorTest, dictionaryOverRowVectorWithLazyChild) {
  constexpr vector_size_t size = 1000;
  auto columnType = ROW({"a", "b"}, {INTEGER(), INTEGER()});

  auto lazyVectorA = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [&](vector_size_t i) { return i % 5; },
      [](vector_size_t i) { return i % 7 == 0; });
  auto lazyVectorB = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [&](vector_size_t i) { return i % 3; },
      [](vector_size_t i) { return i % 11 == 0; });

  VectorPtr rowVector = makeRowVector({lazyVectorA, lazyVectorB});
  VectorPtr dict = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(size, [](auto row) { return row; }),
      size,
      BaseVector::wrapInDictionary(
          nullptr,
          makeIndices(size, [](auto row) { return row; }),
          size,
          rowVector));
  EXPECT_TRUE(isLazyNotLoaded(*dict.get()));

  SelectivityVector rows(dict->size(), false);
  LazyVector::ensureLoadedRows(dict, rows);
  EXPECT_FALSE(isLazyNotLoaded(*dict.get()));
  // Ensure encoding layer is correctly initialized after lazy loading.
  EXPECT_NO_THROW(dict->mayHaveNullsRecursive());
}

TEST_F(LazyVectorTest, selectiveRowVectorWithLazyChild) {
  constexpr vector_size_t size = 1000;
  auto columnType = ROW({"a", "b"}, {INTEGER(), INTEGER()});
  int loadedA = 0, loadedB = 0;
  int expectedLoadedA = 0, expectedLoadedB = 0;

  auto lazyVectorA =
      vectorMaker_.lazyFlatVector<int32_t>(size, [&](vector_size_t i) {
        ++loadedA;
        return i % 5;
      });
  auto lazyVectorB =
      vectorMaker_.lazyFlatVector<int32_t>(size, [&](vector_size_t i) {
        ++loadedB;
        return i % 3;
      });

  VectorPtr rowVector = makeRowVector({lazyVectorA, lazyVectorB});
  EXPECT_TRUE(isLazyNotLoaded(*rowVector.get()));

  SelectivityVector rows(rowVector->size(), false);
  for (int i = 0; i < size; ++i) {
    if (i % 7) {
      rows.setValid(i, true);
      ++expectedLoadedA;
      ++expectedLoadedB;
    }
  }
  rows.updateBounds();
  LazyVector::ensureLoadedRows(rowVector, rows);
  EXPECT_FALSE(isLazyNotLoaded(*rowVector.get()));
  EXPECT_LT(expectedLoadedA, size);
  EXPECT_EQ(loadedA, expectedLoadedA);
  EXPECT_EQ(loadedB, expectedLoadedB);
}

TEST_F(LazyVectorTest, lazyRowVectorWithLazyChildren) {
  constexpr vector_size_t size = 1000;
  auto columnType =
      ROW({"inner_row"}, {ROW({"a", "b"}, {INTEGER(), INTEGER()})});

  auto lazyVectorA = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [](vector_size_t i) { return i % 5; },
      [](vector_size_t i) { return i % 7 == 0; });
  auto lazyVectorB = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [](vector_size_t i) { return i % 3; },
      [](vector_size_t i) { return i % 11 == 0; });

  VectorPtr lazyInnerRowVector = std::make_shared<LazyVector>(
      pool_.get(),
      columnType->childAt(0),
      size,
      std::make_unique<SimpleVectorLoader>([&](RowSet /* rowSet */) {
        return vectorMaker_.rowVector({lazyVectorA, lazyVectorB});
      }));

  VectorPtr rowVector = std::make_shared<LazyVector>(
      pool_.get(),
      columnType,
      size,
      std::make_unique<SimpleVectorLoader>([&](RowSet /* rowSet */) {
        return vectorMaker_.rowVector({lazyInnerRowVector});
      }));

  EXPECT_TRUE(isLazyNotLoaded(*rowVector));

  DecodedVector decodedVector;
  SelectivityVector baseRows;
  LazyVector::ensureLoadedRows(
      rowVector, SelectivityVector(size, true), decodedVector, baseRows);

  EXPECT_FALSE(isLazyNotLoaded(*rowVector.get()));

  RowVector* loadedVector = rowVector->loadedVector()->as<RowVector>();
  auto child = loadedVector->childAt(0);
  // EXPECT_FALSE(child->isLazy());
  assertEqualVectors(child, lazyInnerRowVector);
}

TEST_F(LazyVectorTest, dictionaryOverLazyRowVectorWithLazyChildren) {
  constexpr vector_size_t size = 1000;
  auto columnType =
      ROW({"inner_row"}, {ROW({"a", "b"}, {INTEGER(), INTEGER()})});

  auto lazyVectorA = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [](vector_size_t i) { return i % 5; },
      [](vector_size_t i) { return i % 7 == 0; });
  auto lazyVectorB = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [](vector_size_t i) { return i % 3; },
      [](vector_size_t i) { return i % 11 == 0; });

  VectorPtr lazyInnerRowVector = std::make_shared<LazyVector>(
      pool_.get(),
      columnType->childAt(0),
      size,
      std::make_unique<SimpleVectorLoader>([&](RowSet /* rowSet */) {
        return vectorMaker_.rowVector({lazyVectorA, lazyVectorB});
      }));

  VectorPtr rowVector = std::make_shared<LazyVector>(
      pool_.get(),
      columnType,
      size,
      std::make_unique<SimpleVectorLoader>([&](RowSet /* rowSet */) {
        return vectorMaker_.rowVector({lazyInnerRowVector});
      }));

  VectorPtr dict = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(size, [](auto row) { return row; }),
      size,
      BaseVector::wrapInDictionary(
          nullptr,
          makeIndices(size, [](auto row) { return row; }),
          size,
          rowVector));

  EXPECT_TRUE(isLazyNotLoaded(*dict));

  DecodedVector decodedVector;
  SelectivityVector baseRows;
  LazyVector::ensureLoadedRows(
      dict, SelectivityVector(size, true), decodedVector, baseRows);

  EXPECT_FALSE(isLazyNotLoaded(*dict.get()));

  RowVector* loadedVector =
      dict->valueVector()->valueVector()->loadedVector()->as<RowVector>();
  auto child = loadedVector->childAt(0);
  EXPECT_FALSE(child->isLazy());
  assertEqualVectors(child, lazyInnerRowVector);
}

TEST_F(
    LazyVectorTest,
    dictionaryOverLazyRowVectorWithDictionaryOverLazyChildren) {
  // Input: dict(row(lazy(dict(row(lazy)))))
  constexpr vector_size_t size = 1000;

  auto lazyVectorA = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [](vector_size_t i) { return i % 5; },
      [](vector_size_t i) { return i % 7 == 0; });

  VectorPtr innerRowVector = vectorMaker_.rowVector({lazyVectorA});
  VectorPtr innerDict = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(size, [](auto row) { return row; }),
      size,
      innerRowVector);

  VectorPtr rowVector = vectorMaker_.rowVector({std::make_shared<LazyVector>(
      pool_.get(),
      innerRowVector->type(),
      size,
      std::make_unique<SimpleVectorLoader>(
          [&](RowSet /* rowSet */) { return innerDict; }))});

  VectorPtr dict = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(size, [](auto row) { return row; }),
      size,
      rowVector);

  EXPECT_TRUE(isLazyNotLoaded(*dict));

  DecodedVector decodedVector;
  SelectivityVector baseRows;
  LazyVector::ensureLoadedRows(
      dict, SelectivityVector(size, true), decodedVector, baseRows);

  EXPECT_FALSE(isLazyNotLoaded(*dict.get()));

  // Ensure encoding layer is correctly initialized after lazy loading.
  EXPECT_NO_THROW(dict->mayHaveNullsRecursive());
  EXPECT_NO_THROW(innerDict->mayHaveNullsRecursive());
  // Verify that the correct vector is loaded.
  ASSERT_EQ(
      innerDict.get(),
      dict->valueVector()
          ->asUnchecked<RowVector>()
          ->childAt(0)
          ->loadedVector());
}

TEST_F(LazyVectorTest, lazyRowVectorWithLazyChildrenPartiallyLoaded) {
  constexpr vector_size_t size = 1000;
  auto columnType =
      ROW({"inner_row"}, {ROW({"a", "b"}, {INTEGER(), INTEGER()})});

  auto lazyVectorA = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [](vector_size_t i) { return i % 5; },
      [](vector_size_t i) { return i % 7 == 0; });
  auto lazyVectorB = vectorMaker_.lazyFlatVector<int32_t>(
      size,
      [](vector_size_t i) { return i % 3; },
      [](vector_size_t i) { return i % 11 == 0; });

  VectorPtr lazyInnerRowVector = std::make_shared<LazyVector>(
      pool_.get(),
      columnType->childAt(0),
      size,
      std::make_unique<SimpleVectorLoader>([&](RowSet /* rowSet */) {
        return vectorMaker_.rowVector({lazyVectorA, lazyVectorB});
      }));

  VectorPtr rowVector = std::make_shared<LazyVector>(
      pool_.get(),
      columnType,
      size,
      std::make_unique<SimpleVectorLoader>([&](RowSet /* rowSet */) {
        return vectorMaker_.rowVector({lazyInnerRowVector});
      }));

  EXPECT_TRUE(isLazyNotLoaded(*rowVector));

  raw_vector<vector_size_t> rowNumbers;
  auto iota = facebook::velox::iota(size, rowNumbers);
  RowSet rowSet(iota, size);
  rowVector->asUnchecked<LazyVector>()->load(rowSet, nullptr);
  EXPECT_TRUE(isLazyNotLoaded(*rowVector));

  DecodedVector decodedVector;
  SelectivityVector baseRows;
  LazyVector::ensureLoadedRows(
      rowVector, SelectivityVector(size, true), decodedVector, baseRows);

  EXPECT_FALSE(isLazyNotLoaded(*rowVector.get()));

  RowVector* loadedVector = rowVector->loadedVector()->as<RowVector>();
  auto child = loadedVector->childAt(0);
  EXPECT_FALSE(isLazyNotLoaded(*child));
  assertEqualVectors(child, lazyInnerRowVector);
}

TEST_F(LazyVectorTest, lazyInConstant) {
  // Wrap Lazy vector in a Constant, load some indices and verify that the
  // results.
  static constexpr int32_t kInnerSize = 100;
  static constexpr int32_t kOuterSize = 1000;
  auto base = makeFlatVector<int32_t>(kInnerSize, [](auto row) { return row; });
  std::vector<vector_size_t> loadedRows;
  auto lazy = std::make_shared<LazyVector>(
      pool_.get(),
      INTEGER(),
      kInnerSize,
      std::make_unique<test::SimpleVectorLoader>([&](auto rows) {
        for (auto row : rows) {
          loadedRows.push_back(row);
        }
        return base;
      }));
  VectorPtr wrapped =
      std::make_shared<ConstantVector<int32_t>>(pool_.get(), 10, 7, lazy);

  SelectivityVector rows(kOuterSize, false);
  rows.setValid(1, true);
  rows.setValid(9, true);
  rows.setValid(55, true);
  rows.updateBounds();
  LazyVector::ensureLoadedRows(wrapped, rows);
  EXPECT_EQ(wrapped->encoding(), VectorEncoding::Simple::CONSTANT);
  EXPECT_EQ(loadedRows, (std::vector<vector_size_t>{7}));

  EXPECT_EQ(wrapped->as<SimpleVector<int32_t>>()->valueAt(1), 7);
  EXPECT_EQ(wrapped->as<SimpleVector<int32_t>>()->valueAt(9), 7);
  EXPECT_EQ(wrapped->as<SimpleVector<int32_t>>()->valueAt(55), 7);
}

TEST_F(LazyVectorTest, lazyInDoubleDictionary) {
  // We have dictionaries over LazyVector. We load for some indices in
  // the top dictionary. The intermediate dictionaries refer to
  // non-loaded items in the base of the LazyVector, including indices
  // past its end.
  // This test make sure the we have valid vector after loading.
  static constexpr int32_t kInnerSize = 100;
  static constexpr int32_t kOuterSize = 1000;

  VectorPtr lazy;
  vector_size_t loadEnd = 0;

  auto makeWrapped = [&](BufferPtr nulls) {
    loadEnd = 0;
    lazy = std::make_shared<LazyVector>(
        pool_.get(),
        INTEGER(),
        kOuterSize,
        std::make_unique<test::SimpleVectorLoader>([&](auto rows) {
          loadEnd = rows.back() + 1;
          return makeFlatVector<int32_t>(loadEnd, [](auto row) { return row; });
        }));

    return BaseVector::wrapInDictionary(
        std::move(nulls),
        makeIndices(kInnerSize, [](auto row) { return row; }),
        kInnerSize,
        BaseVector::wrapInDictionary(
            nullptr,
            makeIndices(kOuterSize, [](auto row) { return row; }),
            kOuterSize,
            lazy));
  };
  SelectivityVector rows(kInnerSize);

  // No nulls.
  {
    auto wrapped = makeWrapped(nullptr);

    LazyVector::ensureLoadedRows(wrapped, rows);
    EXPECT_EQ(kInnerSize, loadEnd);
    auto expected =
        makeFlatVector<int32_t>(kInnerSize, [](auto row) { return row; });
    assertEqualVectors(wrapped, expected);
  }

  // With nulls.
  {
    auto wrapped = makeWrapped(makeNulls(kInnerSize, nullEvery(7)));
    LazyVector::ensureLoadedRows(wrapped, rows);

    EXPECT_EQ(kInnerSize, loadEnd);
    auto expected = makeFlatVector<int32_t>(
        kInnerSize, [](auto row) { return row; }, nullEvery(7));
    assertEqualVectors(wrapped, expected);
  }

  // With nulls at the end.
  {
    auto wrapped = makeWrapped(makeNulls(kInnerSize, nullEvery(3)));
    LazyVector::ensureLoadedRows(wrapped, rows);

    EXPECT_EQ(kInnerSize - 1, loadEnd);

    auto expected = makeFlatVector<int32_t>(
        kInnerSize, [](auto row) { return row; }, nullEvery(3));
    assertEqualVectors(wrapped, expected);
  }
}

TEST_F(LazyVectorTest, lazySlice) {
  auto lazy = std::make_shared<LazyVector>(
      pool_.get(),
      INTEGER(),
      100,
      std::make_unique<test::SimpleVectorLoader>([&](auto rows) {
        return makeFlatVector<int32_t>(
            rows.back() + 1, [](auto row) { return row; });
      }));
  EXPECT_THROW(lazy->slice(0, 10), VeloxRuntimeError);
  lazy->loadedVector();
  auto slice = lazy->slice(0, 10);
  for (int i = 0; i < slice->size(); ++i) {
    EXPECT_TRUE(slice->equalValueAt(lazy.get(), i, i));
  }
}

TEST_F(LazyVectorTest, lazyInMultipleDictionaryAllResultantNullRows) {
  // Verifies that lazy loading works for a lazy vector that is wrapped in
  // multiple layers of dictionary encoding such that the rows that it needs to
  // be loaded for all end up pointing to nulls. This results in a zero sized
  // base vector which when wrapped in a dictionary layer can run into invalid
  // internal state for row indices that were not asked to be loaded.
  static constexpr int32_t kVectorSize = 10;
  auto lazy = std::make_shared<LazyVector>(
      pool_.get(),
      INTEGER(),
      kVectorSize,
      std::make_unique<test::SimpleVectorLoader>([&](auto rows) {
        return makeFlatVector<int32_t>(
            rows.back() + 1, [](auto row) { return row; });
      }));
  auto wrapped = BaseVector::wrapInDictionary(
      makeNulls(kVectorSize, [](vector_size_t /*row*/) { return true; }),
      makeIndices(kVectorSize, [](auto row) { return row; }),
      kVectorSize,
      lazy);
  wrapped = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(kVectorSize, [](auto row) { return row; }),
      kVectorSize,
      wrapped);
  SelectivityVector rows(kVectorSize, true);
  rows.setValid(1, false);
  LazyVector::ensureLoadedRows(wrapped, rows);
  auto expected =
      BaseVector::createNullConstant(lazy->type(), wrapped->size(), pool());
  assertEqualVectors(expected, wrapped);
}

TEST_F(LazyVectorTest, lazyInDictionaryNoRowsToLoad) {
  // Verifies that lazy loading works for a lazy vector that is wrapped a
  // dictionary with no extra nulls when loading for 0 selected rows.
  static constexpr int32_t kVectorSize = 10;
  auto lazy = std::make_shared<LazyVector>(
      pool_.get(),
      INTEGER(),
      kVectorSize,
      std::make_unique<test::SimpleVectorLoader>([&](auto rows) {
        return makeFlatVector<int32_t>(
            rows.back() + 1, [](auto row) { return row; });
      }));
  auto wrapped = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(kVectorSize, [](auto row) { return row; }),
      kVectorSize,
      lazy);
  SelectivityVector rows(kVectorSize, false);
  LazyVector::ensureLoadedRows(wrapped, rows);
  for (int i = 0; i < wrapped->size(); i++) {
    EXPECT_TRUE(wrapped->isNullAt(i));
  }
}

TEST_F(LazyVectorTest, lazyWithDictionaryInConstant) {
  // Wrap Lazy vector in a Dictionary in a Lazy vector in a constant, load some
  // indices and verify that the results.
  static constexpr int32_t kInnerSize = 1000;
  static constexpr int32_t kOuterSize = 10;
  auto base = makeFlatVector<int32_t>(kInnerSize, [](auto row) { return row; });
  auto innerLazy = std::make_shared<LazyVector>(
      pool_.get(),
      INTEGER(),
      kInnerSize,
      std::make_unique<test::SimpleVectorLoader>(
          [&](auto /* rows */) { return base; }));
  auto wrapped = BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(kInnerSize, [](auto row) { return row; }),
      kInnerSize,
      innerLazy);
  auto outerLazy = std::make_shared<LazyVector>(
      pool_.get(),
      INTEGER(),
      kInnerSize,
      std::make_unique<test::SimpleVectorLoader>(
          [&](auto /* rows */) { return wrapped; }));
  VectorPtr constant = std::make_shared<ConstantVector<int32_t>>(
      pool_.get(), kOuterSize, 7, outerLazy);

  SelectivityVector rows(kOuterSize, true);
  LazyVector::ensureLoadedRows(constant, rows);
  EXPECT_EQ(constant->encoding(), VectorEncoding::Simple::CONSTANT);

  for (int i = 0; i < kOuterSize; i++) {
    EXPECT_EQ(constant->as<SimpleVector<int32_t>>()->valueAt(i), 7);
  }
}

TEST_F(LazyVectorTest, reset) {
  static constexpr int32_t kVectorSize = 10;
  auto loader = [&](RowSet rows) {
    return makeFlatVector<int32_t>(
        rows.back() + 1, [](auto row) { return row; });
  };
  LazyVector lazy(
      pool_.get(),
      INTEGER(),
      kVectorSize,
      std::make_unique<test::SimpleVectorLoader>(loader));
  lazy.setNull(0, true);
  lazy.reset(std::make_unique<test::SimpleVectorLoader>(loader), kVectorSize);
  ASSERT_EQ(lazy.nulls(), nullptr);
}

TEST_F(LazyVectorTest, runtimeStats) {
  TestRuntimeStatWriter writer;
  RuntimeStatWriterScopeGuard guard(&writer);
  auto lazy = std::make_shared<LazyVector>(
      pool_.get(),
      INTEGER(),
      10,
      std::make_unique<test::SimpleVectorLoader>([&](auto rows) {
        return makeFlatVector<int32_t>(rows.back() + 1, folly::identity);
      }));
  ASSERT_EQ(lazy->loadedVector()->size(), 10);
  auto stats = writer.stats();
  std::sort(stats.begin(), stats.end(), [](auto& x, auto& y) {
    return x.first < y.first;
  });
  ASSERT_EQ(stats.size(), 3);
  ASSERT_EQ(stats[0].first, LazyVector::kCpuNanos);
  ASSERT_GE(stats[0].second.value, 0);
  ASSERT_EQ(stats[1].first, LazyVector::kInputBytes);
  ASSERT_GE(stats[1].second.value, 0);
  ASSERT_EQ(stats[2].first, LazyVector::kWallNanos);
  ASSERT_GE(stats[2].second.value, 0);
}
