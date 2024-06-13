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

class SessionPropertyReporterTest : public testing::Test {};

TEST_F(SessionPropertyReporterTest, getSessionProperties) {
  SessionPropertyReporter propertyReporterObject;
  std::string expectedJsonString = R"([
      {
        "defaultValue":"1000",
        "description":"Native Execution only. The cpu time slice limit in ms that a driver thread.If not zero, can continuously run without yielding. If it is zero,then there is no limit.",
        "hidden":false,
        "name":"driver_cpu_time_slice_limit_ms",
        "typeSignature":"integer"
      },
      {
        "defaultValue":"true",
        "description":"Native Execution only. Use legacy TIME & TIMESTAMP semantics. Warning: this will be removed",
        "hidden":false,
        "name":"legacy_timestamp",
        "typeSignature":"boolean"
      },
      {
        "defaultValue":"0",
        "description":"Native Execution only. The max memory that a final aggregation can use before spilling. If it is 0, then there is no limit",
        "hidden":false,
        "name":"native_aggregation_spill_memory_threshold",
        "typeSignature":"integer"
      },
      {
        "defaultValue":"false",
        "description":"If set to true, then during execution of tasks, the output vectors of every operator are validated for consistency. This is an expensive check so should only be used for debugging. It can help debug issues where malformed vector cause failures or crashes by helping identify which operator is generating them.",
        "hidden":false,
        "name":"native_debug_validate_output_from_operators",
        "typeSignature":"boolean"
      },
      {
        "defaultValue":"true",
        "description":"Native Execution only. Enable join spilling on native engine",
        "hidden":false,
        "name":"native_join_spill_enabled",
        "typeSignature":"boolean"
      },
      {
        "defaultValue":"0",
        "description":"Native Execution only. The max memory that hash join can use before spilling. If it is 0, then there is no limit",
        "hidden":false,
        "name":"native_join_spill_memory_threshold",
        "typeSignature":"integer"
      },
      {
        "defaultValue":"2",
        "description":"Native Execution only. The number of bits (N) used to calculate the spilling partition number for hash join and RowNumber: 2 ^ N",
        "hidden":false,
        "name":"native_join_spiller_partition_bits",
        "typeSignature":"integer"
      },
      {
        "defaultValue":"0",
        "description":"The max allowed spill file size. If it is zero, then there is no limit.",
        "hidden":false,
        "name":"native_max_spill_file_size",
        "typeSignature":"integer"
      },
      {
        "defaultValue":"4",
        "description":"Native Execution only. The maximum allowed spilling level for hash join build.\n 0 is the initial spilling level, -1 means unlimited.",
        "hidden":false,
        "name":"native_max_spill_level",
        "typeSignature":"integer"
      },
      {
        "defaultValue":"0",
        "description":"Native Execution only. The max memory that order by can use before spilling. If it is 0, then there is no limit",
        "hidden":false,
        "name":"native_order_by_spill_memory_threshold",
        "typeSignature":"integer"
      },
      {
        "defaultValue":"true",
        "description":"Native Execution only. Enable row number spilling on native engine",
        "hidden":false,
        "name":"native_row_number_spill_enabled",
        "typeSignature":"boolean"
      },
      {
        "defaultValue":"false",
        "description":"Native Execution only. Enable simplified path in expression evaluation",
        "hidden":false,
        "name":"native_simplified_expression_evaluation_enabled",
        "typeSignature":"boolean"
      },
      {
        "defaultValue":"none",
        "description":"Native Execution only. The compression algorithm type to compress the spilled data.\n Supported compression codecs are: ZLIB, SNAPPY, LZO, ZSTD, LZ4 and GZIP. NONE means no compression.",
        "hidden":false,
        "name":"native_spill_compression_codec",
        "typeSignature":"varchar"
      },
      {
        "defaultValue":"",
        "description":"Native Execution only. Config used to create spill files. This config is \nprovided to underlying file system and the config is free form. The form should be\ndefined by the underlying file system.",
        "hidden":false,
        "name":"native_spill_file_create_config",
        "typeSignature":"varchar"
      },
      {
        "defaultValue":"1048576",
        "description":"Native Execution only. The maximum size in bytes to buffer the serialized spill data before writing to disk for IO efficiency.\n If set to zero, buffering is disabled.",
        "hidden":false,
        "name":"native_spill_write_buffer_size",
        "typeSignature":"bigint"
      },
      {
        "defaultValue":"true",
        "description":"Native Execution only. Enable topN row number spilling on native engine",
        "hidden":false,
        "name":"native_topn_row_number_spill_enabled",
        "typeSignature":"boolean"
      },
      {
        "defaultValue":"true",
        "description":"Native Execution only. Enable window spilling on native engine",
        "hidden":false,
        "name":"native_window_spill_enabled",
        "typeSignature":"boolean"
      },
      {
        "defaultValue":"true",
        "description":"Native Execution only. Enable writer spilling on native engine",
        "hidden":false,
        "name":"native_writer_spill_enabled",
        "typeSignature":"boolean"
      }
    ])";
  json expectedJson = json::parse(expectedJsonString);
  json actualJson = propertyReporterObject.getJsonMetaDataSessionProperty();
  EXPECT_EQ(expectedJson, actualJson);
}
