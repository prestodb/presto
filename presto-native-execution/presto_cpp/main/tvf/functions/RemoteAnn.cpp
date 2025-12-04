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

#include "presto_cpp/main/tvf/spi/TableFunction.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::tvf {

namespace {

class RemoteAnnAnalysis : public TableFunctionAnalysis {
 public:
  explicit RemoteAnnAnalysis() : TableFunctionAnalysis() {}
};

class RemoteAnnDataProcessor : public TableFunctionDataProcessor {
 public:
  explicit RemoteAnnDataProcessor(velox::memory::MemoryPool* pool)
      : TableFunctionDataProcessor("remote_ann", pool, nullptr) {}

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) override {
    return std::make_shared<TableFunctionResult>(
        TableFunctionResult::TableFunctionState::kFinished);
  }
};
} // namespace

void registerRemoteAnn(const std::string& name) {
  registerTableFunction(
      name,
      {},
      std::make_shared<GenericTableReturnType>(),
      [](const std::unordered_map<std::string, std::shared_ptr<Argument>>& args)
          -> std::unique_ptr<TableFunctionAnalysis> {
        return std::make_unique<RemoteAnnAnalysis>();
      },
      [](const std::shared_ptr<const TableFunctionHandle>& handle,
         velox::memory::MemoryPool* pool,
         velox::HashStringAllocator* /*stringAllocator*/,
         const velox::core::QueryConfig& /*queryConfig*/)
          -> std::unique_ptr<TableFunctionDataProcessor> {
  return std::make_unique<RemoteAnnDataProcessor>(pool);
});
}

} // namespace facebook::presto::tvf
