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

#pragma once

#include "velox/common/memory/Memory.h"
#include "velox/external/duckdb/duckdb.hpp"

namespace facebook::velox::duckdb {

struct PrivateVeloxAllocatorData : public ::duckdb::PrivateAllocatorData {
  explicit PrivateVeloxAllocatorData(memory::MemoryPool& pool_) : pool(pool_) {}

  ~PrivateVeloxAllocatorData() override {}

  memory::MemoryPool& pool;
};

::duckdb::data_ptr_t veloxPoolAllocate(
    ::duckdb::PrivateAllocatorData* privateData,
    ::duckdb::idx_t size);

void veloxPoolFree(
    ::duckdb::PrivateAllocatorData* privateData,
    ::duckdb::data_ptr_t pointer,
    ::duckdb::idx_t size);

::duckdb::data_ptr_t veloxPoolReallocate(
    ::duckdb::PrivateAllocatorData* privateData,
    ::duckdb::data_ptr_t pointer,
    ::duckdb::idx_t oldSize,
    ::duckdb::idx_t size);

class VeloxPoolAllocator : public ::duckdb::Allocator {
 public:
  explicit VeloxPoolAllocator(memory::MemoryPool& pool)
      : ::duckdb::Allocator(
            veloxPoolAllocate,
            veloxPoolFree,
            veloxPoolReallocate,
            std::make_unique<PrivateVeloxAllocatorData>(pool)) {}
};

VeloxPoolAllocator& getDefaultAllocator();

} // namespace facebook::velox::duckdb
