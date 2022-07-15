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

#include "velox/duckdb/memory/Allocator.h"

namespace facebook::velox::duckdb {

::duckdb::data_ptr_t veloxPoolAllocate(
    ::duckdb::PrivateAllocatorData* privateData,
    ::duckdb::idx_t size) {
  auto veloxPrivateData = dynamic_cast<PrivateVeloxAllocatorData*>(privateData);
  VELOX_CHECK(veloxPrivateData);
  return static_cast<::duckdb::data_ptr_t>(
      veloxPrivateData->pool.allocate(size));
}

void veloxPoolFree(
    ::duckdb::PrivateAllocatorData* privateData,
    ::duckdb::data_ptr_t pointer,
    ::duckdb::idx_t size) {
  auto veloxPrivateData = dynamic_cast<PrivateVeloxAllocatorData*>(privateData);
  VELOX_CHECK(veloxPrivateData);
  veloxPrivateData->pool.free(pointer, size);
}

::duckdb::data_ptr_t veloxPoolReallocate(
    ::duckdb::PrivateAllocatorData* privateData,
    ::duckdb::data_ptr_t pointer,
    ::duckdb::idx_t oldSize,
    ::duckdb::idx_t size) {
  auto veloxPrivateData = dynamic_cast<PrivateVeloxAllocatorData*>(privateData);
  VELOX_CHECK(veloxPrivateData);
  return static_cast<::duckdb::data_ptr_t>(
      veloxPrivateData->pool.reallocate(pointer, oldSize, size));
}

VeloxPoolAllocator& getDefaultAllocator() {
  static VeloxPoolAllocator allocator{
      memory::getProcessDefaultMemoryManager().getRoot()};
  return allocator;
}

} // namespace facebook::velox::duckdb
