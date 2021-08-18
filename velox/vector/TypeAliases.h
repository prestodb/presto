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
#include <cstdint>

namespace facebook {
namespace velox {

/**
 * The reason for centralizing these types is to maintain consistency in the
 * types used to represent e.g. a vector index, and other similar concepts.
 * Before this centralizing there was significant inconsistency in places as to
 * whether e.g. size_t vs int64_t vs uint32_t vs int32_t was used.
 */

using vector_size_t = int32_t;
using ByteCount = int32_t;

// This is used in SequenceVector as the type for sequence lengths. It is
// involved in storage, and should not be changed.
using SequenceLength = uint32_t;

} // namespace velox
} // namespace facebook
