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

#include "presto_cpp/main/SessionPropertyReporter.h"

using namespace facebook::presto;
using namespace std;

class SessionPropertyReporterTest : public testing::Test {};

TEST_F(SessionPropertyReporterTest, getSessionProperties) {
    SessionPropertyReporter propertyReporterObject;
    std::string expectedJsonString = R"([
        {
          "name": "join_spill_enabled",
          "description": "Native Execution only. Enable join spilling on native engine",
          "typeSignature": "boolean",
          "defaultValue": "false",
          "hidden": false
        },
        {
          "name": "max_spill_level",
          "description": "Native Execution only. The maximum allowed spilling level for hash join build.\n0 is the initial spilling level, -1 means unlimited.",
          "typeSignature": "integer",
          "defaultValue": "4",
          "hidden": false
        },
        {
          "name": "spill_write_buffer_size",
          "description": "Native Execution only. The maximum size in bytes to buffer the serialized spill data before writing to disk for IO efficiency.\nIf set to zero, buffering is disabled.",
          "typeSignature": "bigint",
          "defaultValue": "1048576",
          "hidden": false
        }
    ])";
    json expectedJson = json::parse(expectedJsonString);
    json actualJson = propertyReporterObject.getJsonMetaDataSessionProperty();
    EXPECT_EQ(expectedJson, actualJson);
}

