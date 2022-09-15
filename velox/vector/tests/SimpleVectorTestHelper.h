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

#include <gtest/gtest.h>
#include <string>

#include "velox/vector/SimpleVector.h"
#include "velox/vector/tests/VectorTestUtils.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::test {

/**
 * This class is a bit of a workaround to allow the different test cases of
 * SimpleVector's to be re-used for asserting different things. To use, one
 * should import this header into a file, and implement
 * SimpleVectorTest::runTest. Given that the SimpleVectorTest name will
 * conflict, the test file importing this file should be in its own build target
 * entry.
 */
class SimpleVectorTest : public ::testing::Test {
 public:
  /**
   * Runs a test case. This method should be implemented per instruction above.
   * @param expected The state of the vector to set up for test.
   * @param vectorType The encoding type that this test case should use.
   */
  template <typename T>
  void runTest(ExpectedData<T> expected, VectorEncoding::Simple vectorType);

  // Type coverage roundtrip tests
  ExpectedData<int64_t> genIntRoundTripData() {
    return {
        400, 700, 0, 100, std::nullopt, 300, 500, 600, 800, std::nullopt, 1000};
  }

  ExpectedData<StringView> genStringRoundTripDataVariableWidth() {
    return {
        "foo"_sv,
        std::nullopt,
        "baz"_sv,
        "baz"_sv,
        "baz"_sv,
        "baz"_sv,
        "baz"_sv,
        "baz"_sv,
        "banana"_sv,
        "banana"_sv,
        "banana"_sv,
    };
  }

  ExpectedData<StringView> genStringRoundTripFixedWidthData() {
    return {
        "foo"_sv,
        std::nullopt,
        "baz"_sv,
        "baz"_sv,
        "baz"_sv,
        "baz"_sv,
        "baz"_sv,
        "baz"_sv,
        "grr"_sv,
        "grr"_sv,
        "grr"_sv,
    };
  }

  ExpectedData<bool> genBoolRoundTripData() {
    return {
        false,
        std::nullopt,
        true,
        false,
        false,
        std::nullopt,
        true,
        false,
        true,
        std::nullopt,
        false,
    };
  }

  ExpectedData<double> genDoubleRoundTripData() {
    return {1.1, 1.2, std::nullopt, 1.234, 3.14159, std::nullopt};
  }

 protected:
  std::unique_ptr<memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  VectorMaker maker_{pool_.get()};
};

TEST_F(SimpleVectorTest, stringRoundTripAllNullDictionary) {
  ExpectedData<StringView> expected = {
      std::nullopt, std::nullopt, std::nullopt};
  runTest(std::move(expected), VectorEncoding::Simple::DICTIONARY);
}

TEST_F(SimpleVectorTest, integerRoundTripAllNullDictionary) {
  ExpectedData<int64_t> expected = {std::nullopt, std::nullopt, std::nullopt};
  runTest(std::move(expected), VectorEncoding::Simple::DICTIONARY);
}

TEST_F(SimpleVectorTest, doubleRoundTripAllNullDictionary) {
  ExpectedData<double> expected = {std::nullopt, std::nullopt, std::nullopt};
  runTest(std::move(expected), VectorEncoding::Simple::DICTIONARY);
}

// Integer tests
TEST_F(SimpleVectorTest, integerRoundTripConstant) {
  ExpectedData<int64_t> expected = {1234, 1234, 1234};
  runTest(std::move(expected), VectorEncoding::Simple::CONSTANT);
}

TEST_F(SimpleVectorTest, integerRoundTripConstantNull) {
  ExpectedData<int64_t> expected = {std::nullopt, std::nullopt, std::nullopt};
  runTest(std::move(expected), VectorEncoding::Simple::CONSTANT);
}

TEST_F(SimpleVectorTest, integerRoundTripSequence) {
  runTest(genIntRoundTripData(), VectorEncoding::Simple::SEQUENCE);
}

TEST_F(SimpleVectorTest, integerRoundTripFlatArray) {
  runTest(genIntRoundTripData(), VectorEncoding::Simple::FLAT);
}

TEST_F(SimpleVectorTest, integerRoundTripBiased) {
  runTest(genIntRoundTripData(), VectorEncoding::Simple::BIASED);
}

TEST_F(SimpleVectorTest, integerRoundTripDictionary) {
  runTest(genIntRoundTripData(), VectorEncoding::Simple::DICTIONARY);
}

// double tests
TEST_F(SimpleVectorTest, doubleRoundTripConstant) {
  ExpectedData<double> expected = {3.14159, 3.14159, 3.14159};
  runTest(std::move(expected), VectorEncoding::Simple::CONSTANT);
}

TEST_F(SimpleVectorTest, doubleRoundTripConstantNull) {
  ExpectedData<double> expected = {std::nullopt, std::nullopt, std::nullopt};
  runTest(std::move(expected), VectorEncoding::Simple::CONSTANT);
}

TEST_F(SimpleVectorTest, doubleRoundTripSequence) {
  runTest(genDoubleRoundTripData(), VectorEncoding::Simple::SEQUENCE);
}

TEST_F(SimpleVectorTest, doubleRoundTripFlatArray) {
  runTest(genDoubleRoundTripData(), VectorEncoding::Simple::FLAT);
}

TEST_F(SimpleVectorTest, doubleRoundTripDictionary) {
  runTest(genDoubleRoundTripData(), VectorEncoding::Simple::DICTIONARY);
}

// double biased vector not supported

// String tests
TEST_F(SimpleVectorTest, stringRoundTripConstant) {
  ExpectedData<StringView> expected = {
      StringView("1234"), StringView("1234"), StringView("1234")};
  runTest(std::move(expected), VectorEncoding::Simple::CONSTANT);
}

// String Variable Width
TEST_F(SimpleVectorTest, stringRoundTripConstantNull) {
  ExpectedData<StringView> expected = {
      std::nullopt, std::nullopt, std::nullopt};
  runTest(std::move(expected), VectorEncoding::Simple::CONSTANT);
}

TEST_F(SimpleVectorTest, stringRoundTripSequenceVariableWidth) {
  runTest(
      genStringRoundTripDataVariableWidth(), VectorEncoding::Simple::SEQUENCE);
}

TEST_F(SimpleVectorTest, stringRoundTripFlatArrayVariableWidth) {
  runTest(genStringRoundTripDataVariableWidth(), VectorEncoding::Simple::FLAT);
}

TEST_F(SimpleVectorTest, stringRoundTripDictionaryVariableWidth) {
  runTest(
      genStringRoundTripDataVariableWidth(),
      VectorEncoding::Simple::DICTIONARY);
}

// String Fixed Width
TEST_F(SimpleVectorTest, stringRoundTripSequenceFixedWidth) {
  runTest(genStringRoundTripFixedWidthData(), VectorEncoding::Simple::SEQUENCE);
}

TEST_F(SimpleVectorTest, stringRoundTripFlatArrayFixedWidth) {
  runTest(genStringRoundTripFixedWidthData(), VectorEncoding::Simple::FLAT);
}

TEST_F(SimpleVectorTest, stringRoundTripDictionaryFixedWidth) {
  runTest(
      genStringRoundTripFixedWidthData(), VectorEncoding::Simple::DICTIONARY);
}

TEST_F(SimpleVectorTest, boolRoundTripConstant) {
  ExpectedData<bool> expected = {true, true, true};
  runTest(std::move(expected), VectorEncoding::Simple::CONSTANT);
}

TEST_F(SimpleVectorTest, boolRoundTripConstantNull) {
  ExpectedData<bool> expected = {std::nullopt, std::nullopt, std::nullopt};
  runTest(std::move(expected), VectorEncoding::Simple::CONSTANT);
}

TEST_F(SimpleVectorTest, boolRoundTripSequence) {
  runTest(genBoolRoundTripData(), VectorEncoding::Simple::SEQUENCE);
}

TEST_F(SimpleVectorTest, boolRoundTripFlatArray) {
  runTest(genBoolRoundTripData(), VectorEncoding::Simple::FLAT);
}

// DictionaryBool is not supported / nonsensical

} // namespace facebook::velox::test
