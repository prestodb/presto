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

namespace facebook::velox::dwio::common {

// Converts signed/unsigned int 16/32/64 and float/double to unsigned int of the
// same size. Used to make a dictionary index from a type.
template <typename T>
struct make_index {};

template <>
struct make_index<int16_t> {
  using type = uint16_t;
};

template <>
struct make_index<uint16_t> {
  using type = uint16_t;
};

template <>
struct make_index<int32_t> {
  using type = uint32_t;
};

template <>
struct make_index<uint32_t> {
  using type = uint32_t;
};

template <>
struct make_index<int64_t> {
  using type = uint64_t;
};

template <>
struct make_index<uint64_t> {
  using type = uint64_t;
};

template <>
struct make_index<float> {
  using type = uint32_t;
};

template <>
struct make_index<double> {
  using type = uint64_t;
};

} // namespace facebook::velox::dwio::common
