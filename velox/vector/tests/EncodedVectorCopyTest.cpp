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

#include "velox/vector/EncodedVectorCopy.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/RandomSeed.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox {
namespace {

struct TestParams {
  using Type = std::tuple<bool>;
};

template <typename T>
bool bufferEqual(
    const Buffer& actual,
    const Buffer& expected,
    vector_size_t size) {
  return std::memcmp(actual.as<T>(), expected.as<T>(), size * sizeof(T)) == 0;
}

void compareVectors(const VectorPtr& actual, const VectorPtr& expected) {
  if (expected->isLazy()) {
    compareVectors(actual, BaseVector::loadedVectorShared(expected));
    return;
  }
  ASSERT_EQ(actual->encoding(), expected->encoding());
  ASSERT_EQ(actual->size(), expected->size());
  if (expected->rawNulls()) {
    ASSERT_TRUE(actual->rawNulls());
    for (vector_size_t i = 0; i < actual->size(); ++i) {
      ASSERT_EQ(
          bits::isBitNull(actual->rawNulls(), i),
          bits::isBitNull(expected->rawNulls(), i));
    }
  } else {
    ASSERT_FALSE(actual->rawNulls());
  }
  switch (actual->encoding()) {
    case VectorEncoding::Simple::DICTIONARY:
      ASSERT_TRUE(bufferEqual<vector_size_t>(
          *actual->wrapInfo(), *expected->wrapInfo(), actual->size()));
      compareVectors(actual->valueVector(), expected->valueVector());
      break;
    case VectorEncoding::Simple::ROW: {
      auto* actualRow = actual->asChecked<RowVector>();
      auto* expectedRow = expected->asChecked<RowVector>();
      for (column_index_t i = 0; i < actualRow->childrenSize(); ++i) {
        compareVectors(actualRow->childAt(i), expectedRow->childAt(i));
      }
      break;
    }
    case VectorEncoding::Simple::MAP: {
      auto* actualMap = actual->asChecked<MapVector>();
      auto* expectedMap = expected->asChecked<MapVector>();
      ASSERT_TRUE(bufferEqual<vector_size_t>(
          *actualMap->offsets(), *expectedMap->offsets(), actualMap->size()));
      ASSERT_TRUE(bufferEqual<vector_size_t>(
          *actualMap->sizes(), *expectedMap->sizes(), actualMap->size()));
      compareVectors(actualMap->mapKeys(), expectedMap->mapKeys());
      compareVectors(actualMap->mapValues(), expectedMap->mapValues());
      break;
    }
    case VectorEncoding::Simple::ARRAY: {
      auto* actualArray = actual->asChecked<ArrayVector>();
      auto* expectedArray = expected->asChecked<ArrayVector>();
      ASSERT_TRUE(bufferEqual<vector_size_t>(
          *actualArray->offsets(),
          *expectedArray->offsets(),
          actualArray->size()));
      ASSERT_TRUE(bufferEqual<vector_size_t>(
          *actualArray->sizes(), *expectedArray->sizes(), actualArray->size()));
      compareVectors(actualArray->elements(), expectedArray->elements());
      break;
    }
    default:
      test::assertEqualVectors(expected, actual);
  }
}

class EncodedVectorCopyTest : public testing::TestWithParam<TestParams::Type>,
                              public test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
  }

  void copy(
      const VectorPtr& source,
      const folly::Range<const BaseVector::CopyRange*>& ranges,
      VectorPtr& target) {
    EncodedVectorCopyOptions options;
    options.pool = pool();
    options.reuseSource = reuseSource();
    withSource(source, [&](auto& source) {
      encodedVectorCopy(options, source, ranges, target);
    });
  }

  void runTests(
      const VectorPtr& source,
      const folly::Range<const BaseVector::CopyRange*>& ranges,
      VectorPtr& target,
      const VectorPtr& expected,
      VectorPtr expectedNewSliceCopy = nullptr) {
    {
      SCOPED_TRACE("New full copy");
      BaseVector::CopyRange range = {0, 0, source->size()};
      VectorPtr actual;
      copy(source, folly::Range(&range, 1), actual);
      compareVectors(actual, source);
    }
    {
      SCOPED_TRACE("New slice copy");
      VELOX_CHECK_GE(source->size(), 3);
      BaseVector::CopyRange range = {1, 0, source->size() - 2};
      VectorPtr actual;
      copy(source, folly::Range(&range, 1), actual);
      if (!expectedNewSliceCopy) {
        expectedNewSliceCopy = source->slice(range.sourceIndex, range.count);
      }
      compareVectors(actual, expectedNewSliceCopy);
    }
    {
      SCOPED_TRACE("Immutable target");
      auto actual = target;
      copy(source, ranges, actual);
      compareVectors(actual, expected);
      ASSERT_NE(actual.get(), target.get());
    }
    {
      SCOPED_TRACE("Mutable target");
      VELOX_CHECK_EQ(target.use_count(), 1);
      copy(source, ranges, target);
      compareVectors(target, expected);
    }
  }

  bool reuseSource() {
    return std::get<0>(GetParam());
  }

 private:
  template <typename F>
  void withSource(const VectorPtr& source, F&& f) {
    std::shared_ptr<memory::MemoryPool> sourcePool;
    VectorPtr sourceCopy;
    if (reuseSource()) {
      sourceCopy = source;
    } else {
      sourcePool = rootPool_->addLeafChild("SourcePool");
      sourceCopy = source->copyPreserveEncodings(sourcePool.get());
    }
    f(sourceCopy);
  }
};

VELOX_INSTANTIATE_TEST_SUITE_P(
    ,
    EncodedVectorCopyTest,
    testing::Combine(testing::Bool()));

TEST_P(EncodedVectorCopyTest, constantToConstant) {
  auto source = makeConstant<int64_t>(42, 10);
  auto target = makeConstant<int64_t>(43, 10);
  BaseVector::CopyRange ranges[] = {{0, 1, 1}, {4, 3, 2}};
  auto expected = wrapInDictionary(
      makeIndices({0, 1, 0, 1, 1, 0, 0, 0, 0, 0}),
      makeFlatVector<int64_t>({43, 42}));
  runTests(source, folly::Range(ranges, 2), target, expected);
}

TEST_P(EncodedVectorCopyTest, constantToConstantSameValue) {
  auto source = makeConstant<int64_t>(42, 10);
  auto target = makeConstant<int64_t>(42, 5);
  BaseVector::CopyRange range = {2, 2, 9};
  auto expected = makeConstant<int64_t>(42, 11);
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, constantToDictionary) {
  auto source = makeConstant<int64_t>(42, 10);
  auto target =
      wrapInDictionary(makeIndices({1, 0}), makeFlatVector<int64_t>({43, 44}));
  BaseVector::CopyRange range = {3, 2, 2};
  auto expected = wrapInDictionary(
      makeIndices({1, 0, 2, 2}), makeFlatVector<int64_t>({43, 44, 42}));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, constantToFlat) {
  auto source = makeConstant<int64_t>(42, 10);
  VectorPtr target = makeFlatVector<int64_t>({43, 44});
  BaseVector::CopyRange range = {3, 1, 2};
  auto expected = makeFlatVector<int64_t>({43, 42, 42});
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, dictionaryToConstant) {
  auto source = wrapInDictionary(
      makeIndices({1, 0, 1, 0}), makeFlatVector<int64_t>({43, 44}));
  auto target = makeConstant<int64_t>(42, 5);
  BaseVector::CopyRange range = {0, 1, 2};
  auto expected = wrapInDictionary(
      makeIndices({0, 2, 1, 0, 0}), makeFlatVector<int64_t>({42, 43, 44}));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, dictionaryToDictionary) {
  auto source = wrapInDictionary(
      makeIndices({1, 0, 1, 0}), makeFlatVector<int64_t>({42, 43}));
  auto target = wrapInDictionary(
      makeIndices({0, 1, 0}), makeFlatVector<int64_t>({44, 45}));
  BaseVector::CopyRange range = {1, 2, 2};
  auto expected = wrapInDictionary(
      makeIndices({0, 1, 2, 3}), makeFlatVector<int64_t>({44, 45, 42, 43}));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, dictionaryToFlat) {
  auto source = wrapInDictionary(
      makeIndices({1, 0, 1, 0}), makeFlatVector<int64_t>({42, 43}));
  VectorPtr target = makeFlatVector<int64_t>({44, 45});
  BaseVector::CopyRange range = {0, 1, 2};
  auto expected = makeFlatVector<int64_t>({44, 43, 42});
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, dictionaryCompactBase) {
  auto sourceBase = makeFlatVector<int64_t>({42, 43});
  auto source = wrapInDictionary(makeIndices({1, 0, 1, 0}), sourceBase);
  {
    SCOPED_TRACE("New full base");
    BaseVector::CopyRange ranges[] = {{0, 1, 1}, {1, 0, 1}};
    VectorPtr target;
    copy(source, folly::Range(ranges, 2), target);
    auto expected = wrapInDictionary(
        makeIndices({0, 1}), makeFlatVector<int64_t>({42, 43}));
    compareVectors(target, expected);
  }
  {
    SCOPED_TRACE("New compacted base");
    BaseVector::CopyRange ranges[] = {{1, 0, 1}, {3, 1, 1}};
    VectorPtr target;
    copy(source, folly::Range(ranges, 2), target);
    auto expectedBase =
        reuseSource() ? sourceBase : makeFlatVector(std::vector<int64_t>({42}));
    auto expected = wrapInDictionary(makeIndices({0, 0}), expectedBase);
    compareVectors(target, expected);
    if (reuseSource()) {
      ASSERT_EQ(target->valueVector().get(), sourceBase.get());
    }
  }
  {
    SCOPED_TRACE("Compact target base");
    auto target = wrapInDictionary(
        makeIndices({0, 0, 2, 2}), makeFlatVector<int64_t>({44, 45, 46}));
    BaseVector::CopyRange range = {0, 2, 4};
    copy(source, folly::Range(&range, 1), target);
    auto expected = wrapInDictionary(
        makeIndices({0, 0, 2, 1, 2, 1}), makeFlatVector<int64_t>({44, 42, 43}));
    compareVectors(target, expected);
  }
  {
    SCOPED_TRACE("Drop unused target base tail");
    auto targetBase = makeFlatVector<int64_t>({44, 45, 46});
    auto* targetBasePtr = targetBase.get();
    auto target =
        wrapInDictionary(makeIndices({0, 1, 2}), std::move(targetBase));
    target->resize(0);
    BaseVector::CopyRange range = {0, 0, 4};
    copy(source, folly::Range(&range, 1), target);
    compareVectors(target, source);
    ASSERT_EQ(target->valueVector().get(), targetBasePtr);
  }
}

TEST_P(EncodedVectorCopyTest, allNullsDictionary) {
  auto source = BaseVector::wrapInDictionary(
      makeNulls({true, true, true}),
      makeIndices({0, 1, 2}),
      3,
      makeFlatVector<int64_t>({42, 43, 44}));
  {
    SCOPED_TRACE("New full copy");
    BaseVector::CopyRange range = {0, 0, source->size()};
    VectorPtr actual;
    copy(source, folly::Range(&range, 1), actual);
    test::assertEqualVectors(source, actual);
  }
  {
    SCOPED_TRACE("New slice copy");
    BaseVector::CopyRange range = {1, 0, source->size() - 2};
    VectorPtr actual;
    copy(source, folly::Range(&range, 1), actual);
    test::assertEqualVectors(
        source->slice(range.sourceIndex, range.count), actual);
  }
  auto target =
      wrapInDictionary(makeIndices({0, 1}), makeFlatVector<int64_t>({45, 46}));
  std::vector<BaseVector::CopyRange> ranges = {{0, 2, 3}};
  auto expected = BaseVector::wrapInDictionary(
      makeNulls({false, false, true, true, true}),
      makeIndices({0, 1, 0, 0, 0}),
      5,
      makeFlatVector<int64_t>({45, 46}));
  {
    SCOPED_TRACE("Immutable target");
    auto actual = target;
    copy(source, ranges, actual);
    compareVectors(actual, expected);
    ASSERT_NE(actual.get(), target.get());
  }
  {
    SCOPED_TRACE("Mutable target");
    VELOX_CHECK_EQ(target.use_count(), 1);
    copy(source, ranges, target);
    compareVectors(target, expected);
  }
}

TEST_P(EncodedVectorCopyTest, flatToConstant) {
  auto source = makeFlatVector<int64_t>({43, 44, 45});
  auto target = makeConstant<int64_t>(42, 5);
  BaseVector::CopyRange range = {0, 1, 2};
  auto expected = wrapInDictionary(
      makeIndices({0, 1, 2, 0, 0}), makeFlatVector<int64_t>({42, 43, 44}));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, flatToDictionary) {
  auto source = makeFlatVector<int64_t>({42, 43, 44});
  auto target = wrapInDictionary(
      makeIndices({0, 1, 0}), makeFlatVector<int64_t>({45, 46}));
  BaseVector::CopyRange range = {0, 1, 2};
  auto expected = wrapInDictionary(
      makeIndices({0, 1, 2}), makeFlatVector<int64_t>({45, 42, 43}));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, flatToFlat) {
  VectorPtr source = makeFlatVector<int64_t>({42, 43, 44});
  VectorPtr target = makeFlatVector<int64_t>({45, 46, 47});
  BaseVector::CopyRange range = {0, 1, 2};
  auto expected = makeFlatVector<int64_t>({45, 42, 43});
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, flatRow) {
  auto source = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  VectorPtr target = makeRowVector({makeFlatVector<int64_t>({4, 5, 6})});
  BaseVector::CopyRange range = {1, 2, 2};
  auto expected = makeRowVector({makeFlatVector<int64_t>({4, 5, 2, 3})});
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, constantRow) {
  auto type = ROW({"c0"}, {BIGINT()});
  auto source = makeConstantRow(type, variant::row({42ll}), 3);
  auto target = makeConstantRow(type, variant::row({43ll}), 2);
  BaseVector::CopyRange range = {0, 2, 2};
  auto expected = wrapInDictionary(
      makeIndices({0, 0, 1, 1}),
      makeRowVector({makeFlatVector<int64_t>({43, 42})}));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, dictionaryOfRow) {
  auto source = wrapInDictionary(
      makeIndices({0, 0, 1, 1}),
      makeRowVector({makeFlatVector<int64_t>({42, 43})}));
  auto target = wrapInDictionary(
      makeIndices({0, 0, 1, 1}),
      makeRowVector({makeFlatVector<int64_t>({44, 45})}));
  BaseVector::CopyRange range = {0, 4, 4};
  auto expected = wrapInDictionary(
      makeIndices({0, 0, 1, 1, 2, 2, 3, 3}),
      makeRowVector({makeFlatVector<int64_t>({44, 45, 42, 43})}));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, rowOfDictionary) {
  auto source = makeRowVector({
      wrapInDictionary(
          makeIndices({0, 0, 1, 1}), makeFlatVector<int64_t>({42, 43})),
  });
  VectorPtr target = makeRowVector({
      wrapInDictionary(
          makeIndices({0, 0, 1, 1}), makeFlatVector<int64_t>({44, 45})),
  });
  BaseVector::CopyRange range = {0, 4, 4};
  auto expected = makeRowVector({
      wrapInDictionary(
          makeIndices({0, 0, 1, 1, 2, 2, 3, 3}),
          makeFlatVector<int64_t>({44, 45, 42, 43})),
  });
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, constantRowToFlat) {
  VectorPtr target = makeRowVector({makeFlatVector<int64_t>({42, 43, 44})});
  auto source =
      makeConstantRow(asRowType(target->type()), variant::row({45ll}), 3);
  BaseVector::CopyRange range = {0, 2, 3};
  auto expected = wrapInDictionary(
      makeIndices({0, 1, 3, 3, 3}),
      makeRowVector({makeFlatVector<int64_t>({42, 43, 44, 45})}));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, dictionaryRowToFlat) {
  auto source = wrapInDictionary(
      makeIndices({0, 0, 1, 1}),
      makeRowVector({makeFlatVector<int64_t>({42, 43})}));
  VectorPtr target = makeRowVector({makeFlatVector<int64_t>({44, 45})});
  BaseVector::CopyRange range = {1, 2, 2};
  auto expected = wrapInDictionary(
      makeIndices({0, 1, 2, 3}),
      makeRowVector({makeFlatVector<int64_t>({44, 45, 42, 43})}));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, flatMap) {
  auto sourceElements = makeFlatVector<int64_t>({1, 2, 3});
  auto targetElements = makeFlatVector<int64_t>({4, 5, 6});
  auto source = makeMapVector({0, 1, 2}, sourceElements, sourceElements);
  VectorPtr target = makeMapVector({0, 1, 2}, targetElements, targetElements);
  BaseVector::CopyRange range = {1, 2, 2};
  auto expectedElements = makeFlatVector<int64_t>({4, 5, 6, 2, 3});
  auto expected = std::make_shared<MapVector>(
      pool(),
      source->type(),
      nullptr,
      4,
      makeIndices({0, 1, 3, 4}),
      makeIndices({1, 1, 1, 1}),
      expectedElements,
      expectedElements);
  auto expectedNewSliceCopy = std::make_shared<MapVector>(
      pool(),
      source->type(),
      nullptr,
      1,
      makeIndices({0}),
      makeIndices({1}),
      makeFlatVector(std::vector<int64_t>({2})),
      makeFlatVector(std::vector<int64_t>({2})));
  runTests(
      source, folly::Range(&range, 1), target, expected, expectedNewSliceCopy);
}

TEST_P(EncodedVectorCopyTest, constantMap) {
  auto sourceElements = makeFlatVector<int64_t>({42, 43});
  auto source = BaseVector::wrapInConstant(
      3, 1, makeMapVector({0, 1}, sourceElements, sourceElements));
  auto targetElements = makeFlatVector<int64_t>({44, 45});
  auto target = BaseVector::wrapInConstant(
      3, 1, makeMapVector({0, 1}, targetElements, targetElements));
  BaseVector::CopyRange range = {1, 2, 2};
  auto expected = wrapInDictionary(
      makeIndices({0, 0, 1, 1}),
      makeMapVector(
          {0, 1},
          makeFlatVector<int64_t>({45, 43}),
          makeFlatVector<int64_t>({45, 43})));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, dictionaryMap) {
  auto sourceElements = makeFlatVector<int64_t>({42, 43});
  auto source = wrapInDictionary(
      makeIndices({0, 0, 1, 1}),
      makeMapVector({0, 1}, sourceElements, sourceElements));
  auto targetElements = makeFlatVector<int64_t>({44, 45});
  auto target = wrapInDictionary(
      makeIndices({0, 0, 1, 1}),
      makeMapVector({0, 1}, targetElements, targetElements));
  BaseVector::CopyRange range = {1, 1, 2};
  auto expectedElements = makeFlatVector<int64_t>({44, 45, 42, 43});
  auto expected = wrapInDictionary(
      makeIndices({0, 2, 3, 1}),
      makeMapVector({0, 1, 2, 3}, expectedElements, expectedElements));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, mapCompact) {
  auto sourceElements = makeFlatVector<int64_t>({42, 43, 44});
  auto source = makeMapVector({0, 1, 2}, sourceElements, sourceElements);
  VectorPtr target = std::make_shared<MapVector>(
      pool(),
      source->type(),
      nullptr,
      1,
      makeIndices({1}),
      makeIndices({1}),
      makeFlatVector<int64_t>({45, 46, 47}),
      makeFlatVector<int64_t>({45, 46, 47}));
  BaseVector::CopyRange range = {1, 1, 1};
  auto expectedNewSliceCopy = std::make_shared<MapVector>(
      pool(),
      source->type(),
      nullptr,
      1,
      makeIndices({0}),
      makeIndices({1}),
      makeFlatVector(std::vector<int64_t>({43})),
      makeFlatVector(std::vector<int64_t>({43})));
  auto expected = std::make_shared<MapVector>(
      pool(),
      source->type(),
      nullptr,
      2,
      makeIndices({0, 1}),
      makeIndices({1, 1}),
      makeFlatVector<int64_t>({46, 43}),
      makeFlatVector<int64_t>({46, 43}));
  runTests(
      source, folly::Range(&range, 1), target, expected, expectedNewSliceCopy);
}

TEST_P(EncodedVectorCopyTest, arrayCompactFullReplacement) {
  auto source =
      makeArrayVector({0, 1, 2}, makeFlatVector<int64_t>({42, 43, 44}));
  auto targetElements = makeFlatVector<int64_t>({45, 46, 47});
  auto* targetElementsPtr = targetElements.get();
  VectorPtr target = makeArrayVector({0, 1, 2}, std::move(targetElements));
  target->resize(0);
  BaseVector::CopyRange range = {0, 0, 3};
  copy(source, folly::Range(&range, 1), target);
  compareVectors(target, source);
  ASSERT_EQ(
      target->asChecked<ArrayVector>()->elements().get(), targetElementsPtr);
}

TEST_P(EncodedVectorCopyTest, allNullsMap) {
  auto source = makeAllNullMapVector(3, BIGINT(), BIGINT());
  VectorPtr target = makeAllNullMapVector(3, BIGINT(), BIGINT());
  BaseVector::CopyRange range = {1, 2, 2};
  auto expected = makeAllNullMapVector(4, BIGINT(), BIGINT());
  ;
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, flatArray) {
  auto sourceElements = makeFlatVector<int64_t>({1, 2, 3});
  auto targetElements = makeFlatVector<int64_t>({4, 5, 6});
  auto source = makeArrayVector({0, 1, 2}, sourceElements);
  VectorPtr target = makeArrayVector({0, 1, 2}, targetElements);
  BaseVector::CopyRange range = {1, 2, 2};
  auto expectedElements = makeFlatVector<int64_t>({4, 5, 6, 2, 3});
  auto expected = std::make_shared<ArrayVector>(
      pool(),
      source->type(),
      nullptr,
      4,
      makeIndices({0, 1, 3, 4}),
      makeIndices({1, 1, 1, 1}),
      expectedElements);
  auto expectedNewSliceCopy = std::make_shared<ArrayVector>(
      pool(),
      source->type(),
      nullptr,
      1,
      makeIndices({0}),
      makeIndices({1}),
      makeFlatVector(std::vector<int64_t>({2})));
  runTests(
      source, folly::Range(&range, 1), target, expected, expectedNewSliceCopy);
}

TEST_P(EncodedVectorCopyTest, constantArray) {
  auto sourceElements = makeFlatVector<int64_t>({42, 43});
  auto source =
      BaseVector::wrapInConstant(3, 1, makeArrayVector({0, 1}, sourceElements));
  auto targetElements = makeFlatVector<int64_t>({44, 45});
  auto target =
      BaseVector::wrapInConstant(3, 1, makeArrayVector({0, 1}, targetElements));
  BaseVector::CopyRange range = {1, 2, 2};
  auto expected = wrapInDictionary(
      makeIndices({0, 0, 1, 1}),
      makeArrayVector({0, 1}, makeFlatVector<int64_t>({45, 43})));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, dictionaryArray) {
  auto sourceElements = makeFlatVector<int64_t>({42, 43});
  auto source = wrapInDictionary(
      makeIndices({0, 0, 1, 1}), makeArrayVector({0, 1}, sourceElements));
  auto targetElements = makeFlatVector<int64_t>({44, 45});
  auto target = wrapInDictionary(
      makeIndices({0, 0, 1, 1}), makeArrayVector({0, 1}, targetElements));
  BaseVector::CopyRange range = {1, 1, 2};
  auto expectedElements = makeFlatVector<int64_t>({44, 45, 42, 43});
  auto expected = wrapInDictionary(
      makeIndices({0, 2, 3, 1}),
      makeArrayVector({0, 1, 2, 3}, expectedElements));
  runTests(source, folly::Range(&range, 1), target, expected);
}

TEST_P(EncodedVectorCopyTest, fuzzer) {
  VectorFuzzer::Options fuzzerOptions;
  fuzzerOptions.allowLazyVector = reuseSource();
  fuzzerOptions.nullRatio = 0.05;
  auto seed = common::testutil::getRandomSeed(42);
  VectorFuzzer fuzzer(fuzzerOptions, pool(), seed);
  fuzzer::FuzzerGenerator rng(seed);
#ifndef NDEBUG
  constexpr int kNumIterations = 20;
#else
  constexpr int kNumIterations = 1000;
#endif
  for (int i = 0; i < kNumIterations; ++i) {
    auto type = fuzzer.randType();
    SCOPED_TRACE(fmt::format("i={} type={}", i, type->toString()));
    auto source = fuzzer.fuzz(type);
    BaseVector::CopyRange range;
    range.sourceIndex = folly::Random::rand32(source->size() - 1, rng);
    range.count =
        folly::Random::rand32(1, source->size() - range.sourceIndex, rng);
    {
      SCOPED_TRACE("Null target");
      VectorPtr target;
      range.targetIndex = 0;
      copy(source, folly::Range(&range, 1), target);
      test::assertEqualVectors(
          source->slice(range.sourceIndex, range.count), target);
    }
    auto target = fuzzer.fuzz(type);
    range.targetIndex = folly::Random::rand32(0, target->size() - 1, rng);
    auto makeExpected = [&](auto& expected) {
      if (expected->size() < range.targetIndex + range.count) {
        expected->resize(range.targetIndex + range.count);
      }
      expected->copyRanges(source.get(), folly::Range(&range, 1));
    };
    {
      SCOPED_TRACE("Immutable target");
      auto actual = target;
      copy(source, folly::Range(&range, 1), actual);
      auto expected = BaseVector::copy(*target);
      makeExpected(expected);
      test::assertEqualVectors(expected, actual);
    }
    {
      SCOPED_TRACE("Mutable target");
      auto expected = BaseVector::copy(*target);
      copy(source, folly::Range(&range, 1), target);
      makeExpected(expected);
      test::assertEqualVectors(expected, target);
    }
  }
}

} // namespace
} // namespace facebook::velox
