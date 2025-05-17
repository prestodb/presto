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
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include "presto_cpp/main/common/tests/test_json.h"

namespace fs = boost::filesystem;

using namespace facebook::presto;

namespace {

template <typename T>
void testJsonRoundTrip(const std::string& str) {
  json j = json::parse(str);
  T p = j;

  testJsonRoundtrip(j, p);
}

template <typename T>
void testJsonRoundTripFile(const std::string& filename) {
  testJsonRoundTrip<T>(slurp(getDataPath("/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/", filename)));
}
} // namespace

class TestPlanNodes : public ::testing::Test {};

TEST_F(TestPlanNodes, TestExchangeNode) {
  testJsonRoundTripFile<protocol::ExchangeNode>("ExchangeNode.json");
}

TEST_F(TestPlanNodes, TestFilterNode) {
  testJsonRoundTripFile<protocol::FilterNode>("FilterNode.json");
}

TEST_F(TestPlanNodes, TestOutputNode) {
  testJsonRoundTripFile<protocol::OutputNode>("OutputNode.json");
}

TEST_F(TestPlanNodes, TestValuesNode) {
  testJsonRoundTripFile<protocol::ValuesNode>("ValuesNode.json");
}
