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
  testJsonRoundTrip<T>(slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      filename)));
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

TEST_F(TestPlanNodes, TestRemoteSourceNodeTransportTypeUcx) {
  std::string str = slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      "RemoteSourceNodeUcx.json"));
  json j = json::parse(str);
  protocol::RemoteSourceNode node = j;

  ASSERT_NE(node.transportType, nullptr);
  ASSERT_EQ(*node.transportType, protocol::TransportType::UCX);

  // Round-trip: serialize back to JSON and verify transportType is preserved.
  json r = node;
  ASSERT_EQ(r["transportType"], "UCX");
  testJsonRoundtrip(j, node);
}

TEST_F(TestPlanNodes, TestRemoteSourceNodeTransportTypeHttp) {
  std::string str = slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      "RemoteSourceNodeHttp.json"));
  json j = json::parse(str);
  protocol::RemoteSourceNode node = j;

  ASSERT_NE(node.transportType, nullptr);
  ASSERT_EQ(*node.transportType, protocol::TransportType::HTTP);

  json r = node;
  ASSERT_EQ(r["transportType"], "HTTP");
  testJsonRoundtrip(j, node);
}

TEST_F(TestPlanNodes, TestRemoteSourceNodeTransportTypeAbsent) {
  // RemoteSourceNode JSON without transportType field should leave ptr null.
  std::string str = R"({
    "@type": "com.facebook.presto.sql.planner.plan.RemoteSourceNode",
    "id": "42",
    "sourceFragmentIds": ["1"],
    "outputVariables": [
      {"@type": "variable", "name": "col", "type": "bigint"}
    ],
    "ensureSourceOrdering": false,
    "exchangeType": "GATHER",
    "encoding": "COLUMNAR"
  })";
  json j = json::parse(str);
  protocol::RemoteSourceNode node = j;

  ASSERT_EQ(node.transportType, nullptr);
}

TEST_F(TestPlanNodes, TestPlanFragmentOutputTransportTypeUcx) {
  std::string str = slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      "PlanFragmentWithRemoteSource.json"));
  json j = json::parse(str);
  j["outputTransportType"] = "UCX";

  protocol::PlanFragment fragment = j;
  ASSERT_NE(fragment.outputTransportType, nullptr);
  ASSERT_EQ(*fragment.outputTransportType, protocol::TransportType::UCX);

  json r = fragment;
  ASSERT_EQ(r["outputTransportType"], "UCX");
}

TEST_F(TestPlanNodes, TestPlanFragmentOutputTransportTypeHttp) {
  std::string str = slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      "PlanFragmentWithRemoteSource.json"));
  json j = json::parse(str);
  j["outputTransportType"] = "HTTP";

  protocol::PlanFragment fragment = j;
  ASSERT_NE(fragment.outputTransportType, nullptr);
  ASSERT_EQ(*fragment.outputTransportType, protocol::TransportType::HTTP);

  json r = fragment;
  ASSERT_EQ(r["outputTransportType"], "HTTP");
}

TEST_F(TestPlanNodes, TestPlanFragmentOutputTransportTypeAbsent) {
  // PlanFragment JSON without outputTransportType should leave ptr null.
  std::string str = slurp(getDataPath(
      "/github/presto-trunk/presto-native-execution/presto_cpp/presto_protocol/tests/data/",
      "PlanFragmentWithRemoteSource.json"));
  json j = json::parse(str);
  // Ensure the field is NOT present.
  ASSERT_FALSE(j.count("outputTransportType"));

  protocol::PlanFragment fragment = j;
  ASSERT_EQ(fragment.outputTransportType, nullptr);
}
