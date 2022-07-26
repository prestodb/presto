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
#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <random>
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/ContainerRowSerde.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/VectorHasher.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/tests/VectorMaker.h"
#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::exec::test {

class RowContainerTestBase : public testing::Test {
 protected:
  void SetUp() override {
    pool_ = memory::getDefaultScopedMemoryPool();
    mappedMemory_ = memory::MappedMemory::getInstance();
    if (!isRegisteredVectorSerde()) {
      facebook::velox::serializer::presto::PrestoVectorSerde::
          registerVectorSerde();
    }
    filesystems::registerLocalFileSystem();
  }

  RowVectorPtr makeDataset(
      const TypePtr& rowType,
      const size_t size,
      std::function<void(RowVectorPtr)> customizeData) {
    auto batch = std::static_pointer_cast<RowVector>(
        velox::test::BatchMaker::createBatch(rowType, size, *pool_));
    if (customizeData) {
      customizeData(batch);
    }
    return batch;
  }

  std::unique_ptr<RowContainer> makeRowContainer(
      const std::vector<TypePtr>& keyTypes,
      const std::vector<TypePtr>& dependentTypes,
      bool isJoinBuild = true) {
    static const std::vector<std::unique_ptr<Aggregate>> kEmptyAggregates;
    return std::make_unique<RowContainer>(
        keyTypes,
        !isJoinBuild,
        kEmptyAggregates,
        dependentTypes,
        isJoinBuild,
        isJoinBuild,
        true,
        true,
        mappedMemory_,
        ContainerRowSerde::instance());
  }

  std::unique_ptr<memory::MemoryPool> pool_;
  memory::MappedMemory* mappedMemory_;
};
} // namespace facebook::velox::exec::test
