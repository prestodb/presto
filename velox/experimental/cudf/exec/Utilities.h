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

#include "velox/experimental/cudf/vector/CudfVector.h"

#include <cudf/detail/utilities/stream_pool.hpp>
#include <cudf/table/table.hpp>

#include <rmm/mr/device/device_memory_resource.hpp>

#include <memory>
#include <string_view>

namespace facebook::velox::cudf_velox {

/**
 * @brief Creates a memory resource based on the given mode.
 *
 * @param mode rmm::mr::pool_memory_resource mode.
 * @param percent The initial percent of GPU memory to allocate for memory
 * resource.
 */
[[nodiscard]] std::shared_ptr<rmm::mr::device_memory_resource>
createMemoryResource(std::string_view mode, int percent);

/**
 * @brief Returns the global CUDA stream pool used by cudf.
 */
[[nodiscard]] cudf::detail::cuda_stream_pool& cudfGlobalStreamPool();

// Concatenate a vector of cuDF tables into a single table
[[nodiscard]] std::unique_ptr<cudf::table> concatenateTables(
    std::vector<std::unique_ptr<cudf::table>> tables,
    rmm::cuda_stream_view stream);

// Concatenate a vector of cuDF tables into a single table.
// This function joins the streams owned by individual tables on the passed
// stream. Inputs are not safe to use after calling this function.
[[nodiscard]] std::unique_ptr<cudf::table> getConcatenatedTable(
    std::vector<CudfVectorPtr>& tables,
    const TypePtr& tableType,
    rmm::cuda_stream_view stream);

} // namespace facebook::velox::cudf_velox
