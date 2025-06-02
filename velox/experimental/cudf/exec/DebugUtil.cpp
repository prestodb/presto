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

#include "velox/experimental/cudf/exec/DebugUtil.h"
#include "velox/experimental/cudf/exec/Utilities.h"
#include "velox/experimental/cudf/exec/VeloxCudfInterop.h"

namespace facebook::velox::cudf_velox {

std::string DebugUtil::toString(
    const cudf::table_view& table,
    rmm::cuda_stream_view stream,
    vector_size_t from,
    vector_size_t to) {
  auto rowVector = with_arrow::toVeloxColumn(table, pool_.get(), "", stream);
  stream.synchronize();
  return rowVector->toString(from, to);
}
} // namespace facebook::velox::cudf_velox
