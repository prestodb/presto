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

#include "presto_cpp/presto_protocol/DataSize.h"

using namespace facebook::presto::protocol;

namespace {
void assertDataSize(
    DataSize dataSize,
    double value,
    DataUnit unit,
    const std::string& serialized) {
  EXPECT_EQ(dataSize.getValue(), value);
  EXPECT_EQ(dataSize.getDataUnit(), unit);
  EXPECT_EQ(dataSize.toString(), serialized);

  DataSize duplicate(value, unit);
  EXPECT_EQ(duplicate.getValue(), value);
  EXPECT_EQ(duplicate.getDataUnit(), unit);
  EXPECT_EQ(duplicate.toString(), serialized);
}
} // namespace

class DataSizeTest : public ::testing::Test {};

TEST_F(DataSizeTest, basic) {
  assertDataSize(DataSize(), 0, DataUnit::BYTE, "0.000000B");
  assertDataSize(DataSize("1kB"), 1, DataUnit::KILOBYTE, "1.000000kB");
  assertDataSize(DataSize("1MB"), 1, DataUnit::MEGABYTE, "1.000000MB");
  assertDataSize(DataSize("5.5GB"), 5.5, DataUnit::GIGABYTE, "5.500000GB");

  // Test rounding
  assertDataSize(DataSize("6.556 TB"), 6.556, DataUnit::TERABYTE, "6.560000TB");

  ASSERT_THROW(DataSize("0x4 MB"), DataSizeStringInvalid);
  ASSERT_THROW(DataSize("AA MB");, DataSizeStringInvalid);

  // Seems like this could be allowed, but it's not :(
  ASSERT_THROW(DataSize(".556 MB");, DataSizeStringInvalid);

  ASSERT_THROW(DataSize("6.556 kK"), DataSizeDataUnitUnsupported);

  DataSize d("4 GB");
  assertDataSize(d, 4, DataUnit::GIGABYTE, "4.000000GB");

  ASSERT_NEAR(d.getValue(DataUnit::BYTE), 4294967296, 0.0000000001);
  ASSERT_NEAR(d.getValue(DataUnit::KILOBYTE), 4194304, 0.0000000001);
  ASSERT_NEAR(d.getValue(DataUnit::MEGABYTE), 4096, 0.0000000001);
  ASSERT_NEAR(d.getValue(DataUnit::GIGABYTE), 4, 0.0000000001);
  ASSERT_NEAR(d.getValue(DataUnit::TERABYTE), 0.00390625, 0.0000000001);
  ASSERT_NEAR(d.getValue(DataUnit::PETABYTE), 3.814697265625e-06, 0.0000000001);
}
