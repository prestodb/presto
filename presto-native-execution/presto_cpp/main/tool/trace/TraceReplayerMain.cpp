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

#include <folly/init/Init.h>

#include "fb_velox/warm_storage/WSFileSystemRegistration.h"
#include "presto_cpp/main/operators/PartitionAndSerialize.h"
#include "presto_cpp/main/tool/trace/PartitionAndSerializeReplayer.h"
#include "presto_cpp/main/types/PrestoToVeloxQueryPlan.h"
#include "velox/tool/trace/TraceReplayRunner.h"

using namespace facebook::velox;
using namespace facebook::presto;

namespace {
class PrestoTraceReplayRunner
    : public facebook::velox::tool::trace::TraceReplayRunner {
 public:
  void init() override {
    // Register Presto plan node SerDe
    registerPrestoPlanNodeSerDe();

    // Register Presto trace node factories
    registerPrestoTraceNodeFactories();

    // Register custom Presto operators
    exec::Operator::registerOperator(
        std::make_unique<operators::PartitionAndSerializeTranslator>());

    // Register WarmStorage filesystem to support ws:// URLs
    registerWarmStorageFileSystem(
        "presto_cpp", "presto_on_spark", "presto.native");

    // Call base init
    TraceReplayRunner::init();
  }

 private:
  std::unique_ptr<tool::trace::OperatorReplayerBase> createReplayer()
      const override {
    const auto nodeName = taskTraceMetadataReader_->nodeName(FLAGS_node_id);
    const auto queryCapacityBytes = (1ULL * FLAGS_query_memory_capacity_mb)
        << 20;

    if (nodeName == "PartitionAndSerialize") {
      return std::make_unique<tool::trace::PartitionAndSerializeReplayer>(
          FLAGS_root_dir,
          FLAGS_query_id,
          FLAGS_task_id,
          FLAGS_node_id,
          nodeName,
          FLAGS_driver_ids,
          queryCapacityBytes,
          cpuExecutor_.get());
    }

    // Fall back to base class for standard Velox operators
    return TraceReplayRunner::createReplayer();
  }
};
} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  PrestoTraceReplayRunner runner;
  runner.init();
  runner.run();
  return 0;
}
