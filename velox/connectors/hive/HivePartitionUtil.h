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

#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector::hive {

/// Make the partition name in the Hive style, with the partition values at row.
/// Partition name is in the format of key1=value1/key2=value2/...
std::string makePartitionName(
    const RowVectorPtr& partitionsVector,
    vector_size_t row);

} // namespace facebook::velox::connector::hive
