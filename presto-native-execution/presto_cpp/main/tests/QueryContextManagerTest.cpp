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
#include "presto_cpp/main/QueryContextManager.h"
#include <gtest/gtest.h>
#include "presto_cpp/main/TaskManager.h"

namespace facebook::presto {

class QueryContextManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    taskManager_ = std::make_unique<TaskManager>();
  }
  std::unique_ptr<TaskManager> taskManager_;
};

TEST_F(QueryContextManagerTest, nativeSessionProperties) {
  protocol::TaskId taskId = "scan.0.0.1.0";
  protocol::SessionRepresentation session{
      .systemProperties = {
          {"native_max_spill_level", "1"},
          {"native_spill_compression_codec", "NONE"},
          {"native_join_spill_enabled", "true"},
          {"native_spill_write_buffer_size", "1024"},
          {"aggregation_spill_all", "true"}}};
  auto queryCtx = taskManager_->getQueryContextManager()->findOrCreateQueryCtx(
      taskId, session);
  EXPECT_EQ(queryCtx->queryConfig().maxSpillLevel(), 1);
  EXPECT_EQ(queryCtx->queryConfig().spillCompressionKind(), "NONE");
  EXPECT_TRUE(queryCtx->queryConfig().joinSpillEnabled());
  EXPECT_EQ(queryCtx->queryConfig().spillWriteBufferSize(), 1024);
  EXPECT_TRUE(queryCtx->queryConfig().aggregationSpillAll());
}

} // namespace facebook::presto
