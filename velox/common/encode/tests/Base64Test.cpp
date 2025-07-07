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
  size_t encodedSize = 20;
  EXPECT_EQ(
      13,
      Base64::calculateDecodedSize("SGVsbG8sIFdvcmxkIQ==", encodedSize)
          .value());
  EXPECT_EQ(18, encodedSize);

  encodedSize = 18;
  EXPECT_EQ(
      13,
      Base64::calculateDecodedSize("SGVsbG8sIFdvcmxkIQ", encodedSize).value());
  EXPECT_EQ(18, encodedSize);

  encodedSize = 21;
  EXPECT_EQ(
      Status::UserError(
          "Base64::decode() - invalid input string: string length is not a multiple of 4."),
      Base64::calculateDecodedSize("SGVsbG8sIFdvcmxkIQ===", encodedSize)
          .error());

  encodedSize = 32;
  EXPECT_EQ(
      23,
      Base64::calculateDecodedSize(
          "QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4=", encodedSize)
          .value());
  EXPECT_EQ(31, encodedSize);

  encodedSize = 31;
  EXPECT_EQ(
      23,
      Base64::calculateDecodedSize(
          "QmFzZTY0IGVuY29kaW5nIGlzIGZ1bi4", encodedSize)
          .value());
  EXPECT_EQ(31, encodedSize);

  encodedSize = 16;
  EXPECT_EQ(
      10,
      Base64::calculateDecodedSize("MTIzNDU2Nzg5MA==", encodedSize).value());
  EXPECT_EQ(14, encodedSize);

  encodedSize = 14;
  EXPECT_EQ(
      10, Base64::calculateDecodedSize("MTIzNDU2Nzg5MA", encodedSize).value());
  EXPECT_EQ(14, encodedSize);
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

TEST_F(Base64Test, calculateMimeDecodedSize) {
  EXPECT_EQ(0, Base64::calculateMimeDecodedSize("", 0).value());
  EXPECT_EQ(0, Base64::calculateMimeDecodedSize("#", 1).value());
  EXPECT_EQ(3, Base64::calculateMimeDecodedSize("TWFu", 4).value());
  EXPECT_EQ(1, Base64::calculateMimeDecodedSize("AQ==", 4).value());
  EXPECT_EQ(2, Base64::calculateMimeDecodedSize("TWE=", 4).value());
  EXPECT_EQ(3, Base64::calculateMimeDecodedSize("TWFu\r\n", 6).value());
  EXPECT_EQ(3, Base64::calculateMimeDecodedSize("!TW!Fu!", 7).value());
  EXPECT_EQ(1, Base64::calculateMimeDecodedSize("TQ", 2).value());
  EXPECT_EQ(
      Base64::calculateMimeDecodedSize("A", 1).error(),
      Status::UserError(
          "Input should at least have 2 bytes for base64 bytes."));
}

TEST_F(Base64Test, decodeMime) {
  auto decodeMime = [](const std::string& in) {
    size_t decSize =
        Base64::calculateMimeDecodedSize(in.data(), in.size()).value();
    std::string out(decSize, '\0');
    auto result = Base64::decodeMime(in.data(), in.size(), out.data());
    if (!result.ok()) {
      VELOX_USER_FAIL(result.message());
    }
    return out;
  };
  EXPECT_EQ("", decodeMime(""));
  EXPECT_EQ("Man", decodeMime("TWFu"));
  EXPECT_EQ("ManMan", decodeMime("TWFu\r\nTWFu"));
  EXPECT_EQ("\x01", decodeMime("AQ=="));
  EXPECT_EQ("\xff\xee", decodeMime("/+4="));
  VELOX_ASSERT_USER_THROW(
      decodeMime("QUFBx"), "Last unit does not have enough valid bits");
  VELOX_ASSERT_USER_THROW(
      decodeMime("xx=y"), "Input byte array has wrong 4-byte ending unit");
  VELOX_ASSERT_USER_THROW(
      decodeMime("xx="), "Input byte array has wrong 4-byte ending unit");
  VELOX_ASSERT_USER_THROW(
      decodeMime("QUFB="), "Input byte array has wrong 4-byte ending unit");
  VELOX_ASSERT_USER_THROW(
      decodeMime("AQ==y"), "Input byte array has incorrect ending");
}

TEST_F(Base64Test, calculateMimeEncodedSize) {
  EXPECT_EQ(0, Base64::calculateMimeEncodedSize(0));
  EXPECT_EQ(8, Base64::calculateMimeEncodedSize(4));
  EXPECT_EQ(76, Base64::calculateMimeEncodedSize(57));
  EXPECT_EQ(82, Base64::calculateMimeEncodedSize(58));
  EXPECT_EQ(274, Base64::calculateMimeEncodedSize(200));
}

TEST_F(Base64Test, encodeMime) {
  auto encodeMime = [](const std::string& in) {
    size_t len = Base64::calculateMimeEncodedSize(in.size());
    std::string out(len, '\0');
    Base64::encodeMime(in.data(), in.size(), out.data());
    return out;
  };
  EXPECT_EQ("", encodeMime(""));
  EXPECT_EQ("TWFu", encodeMime("Man"));
  EXPECT_EQ("AQ==", encodeMime("\x01"));
  EXPECT_EQ("/+4=", encodeMime("\xff\xee"));
  EXPECT_EQ(
      "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFB",
      encodeMime(std::string(57, 'A')));
  EXPECT_EQ(
      "QUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFBQUFB\r\nQQ==",
      encodeMime(std::string(58, 'A')));
}

} // namespace facebook::velox::encoding
