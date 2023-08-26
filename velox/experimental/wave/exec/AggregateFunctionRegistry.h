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

#include <folly/Range.h>
#include "velox/experimental/wave/common/Cuda.h"
#include "velox/experimental/wave/common/Type.h"
#include "velox/experimental/wave/exec/AggregateFunction.h"

namespace facebook::velox::wave::aggregation {

class AggregateFunctionRegistry {
 public:
  explicit AggregateFunctionRegistry(GpuAllocator* allocator);

  void addAllBuiltInFunctions(Stream&);

  using Types = folly::Range<const PhysicalType*>;

  AggregateFunction* getFunction(const std::string& name, const Types& argTypes)
      const;

 private:
  GpuAllocator* allocator_;
  struct Entry {
    std::function<bool(const Types&)> accept;
    GpuAllocator::UniquePtr<AggregateFunction> function;
  };
  std::unordered_map<std::string, std::vector<Entry>> entries_;
};

} // namespace facebook::velox::wave::aggregation
