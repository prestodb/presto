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
#pragma once
#include "velox/vector/BaseVector.h"

namespace facebook::presto::protocol {

// Deserializes base64-encoded string created by
// presto-common/src/main/java/com/facebook/presto/common/block/BlockEncodingManager.java
// into vector.
// TODO Refactor to avoid duplicating logic in PrestoSerializer.
velox::VectorPtr readBlock(
    const velox::TypePtr& type,
    const std::string& base64Encoded,
    velox::memory::MemoryPool* pool);
} // namespace facebook::presto::protocol
