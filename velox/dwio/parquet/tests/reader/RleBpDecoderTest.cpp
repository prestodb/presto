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

#include "velox/dwio/common/BitPackDecoder.h"

#include <arrow/util/rle_encoding.h> // @manual
#include <gtest/gtest.h>

#include <random>

using namespace facebook::velox;
using namespace facebook::velox::dwio::common;

template <typename T>
class RleBpDecoderTest {
 public:
  RleBpDecoderTest() {
    inputValues_.resize(numValues_, 0);
    outputValues_.resize(numValues_, 0);
    encodedValues_.resize(numValues_ * 4, 0);
  }

  RleBpDecoderTest(uint32_t numValues) : numValues_(numValues) {
    VELOX_CHECK(numValues % 8 == 0);

    inputValues_.resize(numValues_, 0);
    outputValues_.resize(numValues_, 0);
    encodedValues_.resize(numValues_ * 4, 0);
  }

  void testDecodeRandomData(uint8_t bitWidth) {
    bitWidth_ = bitWidth;

    populateInputValues();

    testDecode();
  }

  void testDecodeSuppliedData(std::vector<T> inputValues, uint8_t bitWidth) {
    numValues_ = inputValues.size();
    inputValues_ = inputValues;
    bitWidth_ = bitWidth;

    encodeInputValues();
    testDecode();
  }

 private:
  void testDecode() {
    const uint8_t* inputIter = encodedValues_.data();
    T* output = outputValues_.data();
    facebook::velox::dwio::common::unpack<T>(
        inputIter, bytes(bitWidth_), numValues_, bitWidth_, output);

    inputIter = encodedValues_.data();
    T* expectedOutput = inputValues_.data();

    for (int i = 0; i < numValues_; i++) {
      if (output[i] != expectedOutput[i]) {
        break;
      }
      ASSERT_EQ(output[i], expectedOutput[i]);
    }
  }

  void populateInputValues() {
    auto maxValue = (1L << bitWidth_) - 1;

    for (auto j = 0; j < numValues_; j++) {
      inputValues_[j] = rand() % maxValue;
    }

    encodeInputValues();
  }

  void encodeInputValues() {
    arrow::util::RleEncoder arrowEncoder(
        reinterpret_cast<uint8_t*>(inputValues_.data()),
        bytes(bitWidth_),
        bitWidth_);

    for (auto i = 0; i < numValues_; i++) {
      arrowEncoder.Put(inputValues_[i]);
    }
    arrowEncoder.Flush();
  }

  uint32_t bytes(uint8_t bitWidth) {
    return (numValues_ * bitWidth + 7) / 8;
  }

  // multiple of 8
  uint32_t numValues_ = 1024;
  std::vector<T> inputValues_;
  std::vector<T> outputValues_;
  std::vector<uint8_t> encodedValues_;
  uint8_t bitWidth_;
};

TEST(RleBpDecoderTest, uint8) {
  RleBpDecoderTest<uint8_t> test(1024);

  test.testDecodeRandomData(1);
  test.testDecodeRandomData(2);
  test.testDecodeRandomData(3);
  test.testDecodeRandomData(4);
  test.testDecodeRandomData(5);
  test.testDecodeRandomData(6);
  test.testDecodeRandomData(7);
  test.testDecodeRandomData(8);
}

TEST(RleBpDecoderTest, uint16) {
  RleBpDecoderTest<uint16_t> test(1024);

  for (uint8_t i = 1; i <= 16; i++) {
    test.testDecodeRandomData(i);
  }
}

TEST(RleBpDecoderTest, uint32) {
  RleBpDecoderTest<uint32_t> test(1024);

  for (uint8_t i = 1; i <= 32; i++) {
    test.testDecodeRandomData(i);
  }
}

TEST(RleBpDecoderTest, allOnes) {
  std::vector<uint8_t> allOnesVector(1024, 1);
  RleBpDecoderTest<uint8_t> test;
  test.testDecodeSuppliedData(allOnesVector, 1);
}
