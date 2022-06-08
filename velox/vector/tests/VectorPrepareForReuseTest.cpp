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
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;

class VectorPrepareForReuseTest : public testing::Test,
                                  public test::VectorTestBase {
 protected:
  VectorPrepareForReuseTest() {
    pool()->setMemoryUsageTracker(memory::MemoryUsageTracker::create());
  }
};

class MemoryAllocationChecker {
 public:
  explicit MemoryAllocationChecker(memory::MemoryPool* pool)
      : tracker_{pool->getMemoryUsageTracker().get()},
        numAllocations_{tracker_->getNumAllocs()} {}

  bool assertOne() {
    bool ok = numAllocations_ + 1 == tracker_->getNumAllocs();
    numAllocations_ = tracker_->getNumAllocs();
    return ok;
  }

  bool assertAtLeastOne() {
    bool ok = numAllocations_ < tracker_->getNumAllocs();
    numAllocations_ = tracker_->getNumAllocs();
    return ok;
  }

  ~MemoryAllocationChecker() {
    EXPECT_EQ(numAllocations_, tracker_->getNumAllocs());
  }

 private:
  memory::MemoryUsageTracker* tracker_;
  int64_t numAllocations_;
};

TEST_F(VectorPrepareForReuseTest, strings) {
  std::vector<std::string> largeStrings = {
      std::string(20, '.'),
      std::string(30, '-'),
      std::string(40, '='),
  };

  auto stringAt = [&](auto row) {
    return row % 3 == 0 ? StringView(largeStrings[(row / 3) % 3]) : ""_sv;
  };

  VectorPtr vector = makeFlatVector<StringView>(1'000, stringAt);
  auto originalBytes = vector->retainedSize();
  BaseVector* originalVector = vector.get();

  // Verify that string buffers get reused rather than appended to.
  {
    MemoryAllocationChecker allocationChecker(pool());

    BaseVector::prepareForReuse(vector, vector->size());
    ASSERT_EQ(originalVector, vector.get());
    ASSERT_EQ(originalBytes, vector->retainedSize());

    // Verify that StringViews are reset to empty strings.
    for (auto i = 0; i < vector->size(); i++) {
      ASSERT_EQ("", vector->asFlatVector<StringView>()->valueAt(i).str());
    }

    for (auto i = 0; i < vector->size(); i++) {
      vector->asFlatVector<StringView>()->set(i, stringAt(i));
    }
    ASSERT_EQ(originalBytes, vector->retainedSize());
  }

  // Verify that string buffers get dropped if not singly referenced.
  auto getStringBuffers = [](const VectorPtr& vector) {
    return vector->asFlatVector<StringView>()->stringBuffers();
  };

  {
    MemoryAllocationChecker allocationChecker(pool());

    auto stringBuffers = getStringBuffers(vector);
    ASSERT_FALSE(stringBuffers.empty());

    BaseVector::prepareForReuse(vector, vector->size());
    ASSERT_EQ(originalVector, vector.get());
    ASSERT_GT(originalBytes, vector->retainedSize());
    ASSERT_TRUE(getStringBuffers(vector).empty());

    for (auto i = 0; i < vector->size(); i++) {
      vector->asFlatVector<StringView>()->set(i, stringAt(i));
    }
    ASSERT_EQ(originalBytes, vector->retainedSize());

    ASSERT_TRUE(allocationChecker.assertAtLeastOne());
  }

  // Verify that only one string buffer is kept for re-use.
  {
    std::vector<std::string> extraLargeStrings = {
        std::string(200, '.'),
        std::string(300, '-'),
        std::string(400, '='),
    };

    VectorPtr extraLargeVector = makeFlatVector<StringView>(
        1'000,
        [&](auto row) { return StringView(extraLargeStrings[row % 3]); });
    ASSERT_LT(1, getStringBuffers(extraLargeVector).size());

    auto originalExtraLargeBytes = extraLargeVector->retainedSize();
    BaseVector* originalExtraLargeVector = extraLargeVector.get();

    MemoryAllocationChecker allocationChecker(pool());

    BaseVector::prepareForReuse(extraLargeVector, extraLargeVector->size());
    ASSERT_EQ(originalExtraLargeVector, extraLargeVector.get());
    ASSERT_GT(originalExtraLargeBytes, extraLargeVector->retainedSize());
    ASSERT_EQ(1, getStringBuffers(extraLargeVector).size());

    for (auto i = 0; i < extraLargeVector->size(); i++) {
      extraLargeVector->asFlatVector<StringView>()->set(i, stringAt(i));
    }
    ASSERT_EQ(originalBytes, extraLargeVector->retainedSize());
  }
}

TEST_F(VectorPrepareForReuseTest, nulls) {
  VectorPtr vector = makeFlatVector<int32_t>(
      1'000, [](auto row) { return row; }, nullEvery(7));
  auto originalBytes = vector->retainedSize();
  BaseVector* originalVector = vector.get();

  // Verify that nulls buffer is reused.
  {
    MemoryAllocationChecker allocationChecker(pool());

    ASSERT_TRUE(vector->nulls() != nullptr);

    BaseVector::prepareForReuse(vector, vector->size());
    ASSERT_EQ(originalVector, vector.get());
    ASSERT_EQ(originalBytes, vector->retainedSize());
  }

  // Verify that nulls buffer is freed if there are no nulls.
  {
    MemoryAllocationChecker allocationChecker(pool());

    for (auto i = 0; i < vector->size(); i++) {
      vector->setNull(i, false);
    }
    ASSERT_TRUE(vector->nulls() != nullptr);
    ASSERT_EQ(originalBytes, vector->retainedSize());

    BaseVector::prepareForReuse(vector, vector->size());
    ASSERT_EQ(originalVector, vector.get());
    ASSERT_TRUE(vector->nulls() == nullptr);
    ASSERT_GT(originalBytes, vector->retainedSize());

    vector->setNull(12, true);
    ASSERT_EQ(originalBytes, vector->retainedSize());

    ASSERT_TRUE(allocationChecker.assertOne());
  }

  // Verify that nulls buffer is dropped if not singly-referenced.
  {
    MemoryAllocationChecker allocationChecker(pool());

    ASSERT_TRUE(vector->nulls() != nullptr);
    ASSERT_EQ(originalBytes, vector->retainedSize());

    auto nulls = vector->nulls();
    BaseVector::prepareForReuse(vector, vector->size());
    ASSERT_EQ(originalVector, vector.get());
    ASSERT_TRUE(vector->nulls() == nullptr);
    ASSERT_GT(originalBytes, vector->retainedSize());

    vector->setNull(12, true);
    ASSERT_EQ(originalBytes, vector->retainedSize());

    ASSERT_TRUE(allocationChecker.assertOne());
  }
}

TEST_F(VectorPrepareForReuseTest, arrays) {
  VectorPtr vector = makeArrayVector<int32_t>(
      1'000,
      [](auto row) { return 1; },
      [](auto row, auto index) { return row + index; });
  auto originalSize = vector->retainedSize();
  BaseVector* originalVector = vector.get();

  auto otherVector = makeArrayVector<int32_t>(
      1'000,
      [](auto row) { return 1; },
      [](auto row, auto index) { return 2 * row + index; });

  MemoryAllocationChecker allocationChecker(pool());
  BaseVector::prepareForReuse(vector, vector->size());
  ASSERT_EQ(originalVector, vector.get());
  ASSERT_EQ(originalSize, vector->retainedSize());

  for (auto i = 0; i < 1'000; i++) {
    ASSERT_EQ(0, vector->as<ArrayVector>()->sizeAt(i));
    ASSERT_EQ(0, vector->as<ArrayVector>()->offsetAt(i));
  }

  vector->copy(otherVector.get(), 0, 0, 1'000);
  ASSERT_EQ(originalSize, vector->retainedSize());
}
