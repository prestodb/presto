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

#include "velox/common/encode/Base64.h"

#include <gtest/gtest.h>
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::encoding {

class Base64Test : public ::testing::Test {};

TEST_F(Base64Test, fromBase64) {
  EXPECT_EQ(
      "Hello, World!",
      Base64::decode(folly::StringPiece("SGVsbG8sIFdvcmxkIQ==")));
  EXPECT_EQ(
      "Base64 encoding is fun.",
      Base64::decode(folly::StringPiece("QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4=")));
  EXPECT_EQ(
      "Simple text", Base64::decode(folly::StringPiece("U2ltcGxlIHRleHQ=")));
  EXPECT_EQ(
      "1234567890", Base64::decode(folly::StringPiece("MTIzNDU2Nzg5MA==")));

  // Check encoded strings without padding
  EXPECT_EQ(
      "Hello, World!",
      Base64::decode(folly::StringPiece("SGVsbG8sIFdvcmxkIQ")));
  EXPECT_EQ(
      "Base64 encoding is fun.",
      Base64::decode(folly::StringPiece("QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4")));
  EXPECT_EQ(
      "Simple text", Base64::decode(folly::StringPiece("U2ltcGxlIHRleHQ")));
  EXPECT_EQ("1234567890", Base64::decode(folly::StringPiece("MTIzNDU2Nzg5MA")));
}

TEST_F(Base64Test, calculateDecodedSizeProperSize) {
  size_t encoded_size{0};

  encoded_size = 20;
  EXPECT_EQ(
      13, Base64::calculateDecodedSize("SGVsbG8sIFdvcmxkIQ==", encoded_size));
  EXPECT_EQ(18, encoded_size);

  encoded_size = 18;
  EXPECT_EQ(
      13, Base64::calculateDecodedSize("SGVsbG8sIFdvcmxkIQ", encoded_size));
  EXPECT_EQ(18, encoded_size);

  encoded_size = 21;
  VELOX_ASSERT_THROW(
      Base64::calculateDecodedSize("SGVsbG8sIFdvcmxkIQ==", encoded_size),
      "Base64::decode() - invalid input string: string length cannot be 1 more than a multiple of 4.");

  encoded_size = 32;
  EXPECT_EQ(
      23,
      Base64::calculateDecodedSize(
          "QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4=", encoded_size));
  EXPECT_EQ(31, encoded_size);

  encoded_size = 31;
  EXPECT_EQ(
      23,
      Base64::calculateDecodedSize(
          "QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4", encoded_size));
  EXPECT_EQ(31, encoded_size);

  encoded_size = 16;
  EXPECT_EQ(10, Base64::calculateDecodedSize("MTIzNDU2Nzg5MA==", encoded_size));
  EXPECT_EQ(14, encoded_size);

  encoded_size = 14;
  EXPECT_EQ(10, Base64::calculateDecodedSize("MTIzNDU2Nzg5MA", encoded_size));
  EXPECT_EQ(14, encoded_size);
}

TEST_F(Base64Test, checksPadding) {
  EXPECT_TRUE(Base64::isPadded("ABC=", 4));
  EXPECT_FALSE(Base64::isPadded("ABC", 3));
}

TEST_F(Base64Test, countsPaddingCorrectly) {
  EXPECT_EQ(0, Base64::numPadding("ABC", 3));
  EXPECT_EQ(1, Base64::numPadding("ABC=", 4));
  EXPECT_EQ(2, Base64::numPadding("AB==", 4));
}
} // namespace facebook::velox::encoding
