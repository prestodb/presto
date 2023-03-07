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

#include "velox/vector/BaseVector.h"

namespace facebook::velox {

/// Returns human-friendly text representation of the vector's data.
std::string printVector(const BaseVector& vector);

/// Returns human-friendly text representation of the vector's data in rows
/// [from, from + size).
/// @param from Zero-based row number of the first row to print. Must be
/// non-negative. If greater than vector size, no rows are printed.
/// @param size Number of of rows to print. If 'from' + 'size' is greater than
/// vector size, a subset of rows starting from 'from' to the end of the vector
/// are printed.
std::string printVector(
    const BaseVector& vector,
    vector_size_t from,
    vector_size_t size = 10);

/// Returns human-friendly text representation of the vector's data in 'rows'.
/// @param rows A set of rows to print.
std::string printVector(
    const BaseVector& vector,
    const SelectivityVector& rows);

} // namespace facebook::velox
