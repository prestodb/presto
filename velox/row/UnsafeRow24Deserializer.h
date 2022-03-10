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

#include <memory>
#include <string_view>

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::row {

// Caller is responsible for controlling batching.
class UnsafeRow24Deserializer {
 public:
  virtual ~UnsafeRow24Deserializer() = default;

  static std::unique_ptr<UnsafeRow24Deserializer> Create(RowTypePtr rowType);

  virtual RowVectorPtr DeserializeRows(
      memory::MemoryPool* pool,
      const std::vector<std::string_view>& rows) = 0;

 protected:
  UnsafeRow24Deserializer() = default;
  UnsafeRow24Deserializer(const UnsafeRow24Deserializer&) = delete;
  const UnsafeRow24Deserializer& operator=(const UnsafeRow24Deserializer&) =
      delete;
};

} // namespace facebook::velox::row
