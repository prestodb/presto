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

#include <gtest/gtest.h>
#include "velox/dwio/dwrf/common/Checksum.h"

using namespace ::testing;
using namespace facebook::velox::dwrf;

class ChecksumTests : public Test {
 protected:
  void SetUp() override {
    for (size_t i = 0; i < data.size(); ++i) {
      data.data()[i] = static_cast<char>(i % 0xff);
    }
  }

  void TearDown() override {}

  void runTest(proto::ChecksumAlgorithm type, int64_t c1, int64_t c2) {
    auto checksum = ChecksumFactory::create(type);
    checksum->update(data.data(), data.size());
    auto v1 = checksum->getDigest();
    checksum->update(data.data(), data.size());
    auto v2 = checksum->getDigest(false);
    ASSERT_EQ(v1, v2);
    ASSERT_EQ(v2, c1);
    checksum->update(data.data(), data.size());
    v2 = checksum->getDigest();
    ASSERT_EQ(v2, c2);
  }

  std::array<char, 4096> data;
};

TEST_F(ChecksumTests, Null) {
  auto checksum = ChecksumFactory::create(proto::ChecksumAlgorithm::NULL_);
  ASSERT_EQ(checksum, nullptr);
}

TEST_F(ChecksumTests, xxHash) {
  runTest(
      proto::ChecksumAlgorithm::XXHASH,
      -5912557168167935695,
      2625948533963027735);
}

TEST_F(ChecksumTests, Crc32) {
  runTest(proto::ChecksumAlgorithm::CRC32, 4133052486, 3074245904);
}
