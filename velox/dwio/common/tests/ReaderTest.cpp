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

#include "velox/dwio/common/Reader.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::dwio::common {
namespace {

using namespace facebook::velox::common;

class ReaderTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(ReaderTest, getOrCreateChild) {
  constexpr int kSize = 5;
  auto input = makeRowVector(
      {"c.0", "c.1"},
      {
          makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
          makeFlatVector<int64_t>({2, 4, 6, 7, 8}),
      });

  common::ScanSpec spec("<root>");
  spec.addField("c.0", 0);
  // Create child from name.
  spec.getOrCreateChild("c.1")->setFilter(
      common::createBigintValues({2, 4, 6}, false));

  auto actual = RowReader::projectColumns(input, spec, nullptr);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
  });
  test::assertEqualVectors(expected, actual);

  // Create child from subfield.
  spec.getOrCreateChild(common::Subfield("c.1"))
      ->setFilter(common::createBigintValues({2, 4, 6}, false));
  VELOX_ASSERT_USER_THROW(
      RowReader::projectColumns(input, spec, nullptr),
      "Field not found: c. Available fields are: c.0, c.1.");
}

TEST_F(ReaderTest, projectColumnsFilterStruct) {
  constexpr int kSize = 10;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kSize, folly::identity),
      makeRowVector({
          makeFlatVector<int64_t>(kSize, folly::identity),
      }),
  });
  common::ScanSpec spec("<root>");
  spec.addField("c0", 0);
  spec.getOrCreateChild(common::Subfield("c1.c0"))
      ->setFilter(common::createBigintValues({2, 4, 6}, false));
  auto actual = RowReader::projectColumns(input, spec, nullptr);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({2, 4, 6}),
  });
  test::assertEqualVectors(expected, actual);
}

TEST_F(ReaderTest, projectColumnsFilterArray) {
  constexpr int kSize = 10;
  auto input = makeRowVector({
      makeFlatVector<int64_t>(kSize, folly::identity),
      makeArrayVector<int64_t>(
          kSize,
          [](auto) { return 1; },
          [](auto i) { return i; },
          [](auto i) { return i % 2 != 0; }),
  });
  common::ScanSpec spec("<root>");
  spec.addField("c0", 0);
  auto* c1 = spec.getOrCreateChild(common::Subfield("c1"));
  {
    SCOPED_TRACE("IS NULL");
    c1->setFilter(std::make_unique<common::IsNull>());
    auto actual = RowReader::projectColumns(input, spec, nullptr);
    auto expected = makeRowVector({
        makeFlatVector<int64_t>({1, 3, 5, 7, 9}),
    });
    test::assertEqualVectors(expected, actual);
  }
  {
    SCOPED_TRACE("IS NOT NULL");
    c1->setFilter(std::make_unique<common::IsNotNull>());
    auto actual = RowReader::projectColumns(input, spec, nullptr);
    auto expected = makeRowVector({
        makeFlatVector<int64_t>({0, 2, 4, 6, 8}),
    });
    test::assertEqualVectors(expected, actual);
  }
}

TEST_F(ReaderTest, projectColumnsMutation) {
  constexpr int kSize = 10;
  auto input = makeRowVector({makeFlatVector<int64_t>(kSize, folly::identity)});
  common::ScanSpec spec("<root>");
  spec.addAllChildFields(*input->type());
  std::vector<uint64_t> deleted(bits::nwords(kSize));
  bits::setBit(deleted.data(), 2);
  Mutation mutation;
  mutation.deletedRows = deleted.data();
  auto actual = RowReader::projectColumns(input, spec, &mutation);
  auto expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 3, 4, 5, 6, 7, 8, 9}),
  });
  test::assertEqualVectors(expected, actual);
  random::setSeed(42);
  random::RandomSkipTracker randomSkip(0.5);
  mutation.randomSkip = &randomSkip;
  actual = RowReader::projectColumns(input, spec, &mutation);
  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 3, 5, 6, 8}),
  });
  test::assertEqualVectors(expected, actual);
}

} // namespace
} // namespace facebook::velox::dwio::common
