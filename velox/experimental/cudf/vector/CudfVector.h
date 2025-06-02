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

#include "velox/common/memory/MemoryPool.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/TypeAliases.h"

#include <cudf/table/table.hpp>

#include <rmm/cuda_stream_view.hpp>

#include <memory>
#include <utility>

namespace facebook::velox::cudf_velox {

// Vector class which holds GPU data from cuDF.
class CudfVector : public RowVector {
 public:
  CudfVector(
      velox::memory::MemoryPool* pool,
      TypePtr type,
      vector_size_t size,
      std::unique_ptr<cudf::table>&& table,
      rmm::cuda_stream_view stream);

  rmm::cuda_stream_view stream() const {
    return stream_;
  }

  cudf::table_view getTableView() const {
    return table_->view();
  }

  std::unique_ptr<cudf::table>&& release() {
    flatSize_ = 0;
    return std::move(table_);
  }

  uint64_t estimateFlatSize() const override;

 private:
  std::unique_ptr<cudf::table> table_;
  rmm::cuda_stream_view stream_;
  uint64_t flatSize_;
};

using CudfVectorPtr = std::shared_ptr<CudfVector>;

} // namespace facebook::velox::cudf_velox
