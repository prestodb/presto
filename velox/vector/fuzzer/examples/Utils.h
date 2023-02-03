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

#include "velox/vector/FlatVector.h"

namespace facebook::velox::generator_spec_examples {

using Rng = std::mt19937;
using Sample = std::map<int32_t, size_t>;

template <typename T>
Sample convertToSample(FlatVector<T>* flatVector) {
  Sample sample;
  for (auto i = 0; i < flatVector->size(); ++i) {
    int32_t val = std::round(flatVector->valueAt(i));
    sample[val]++;
  }
  return sample;
}

template <typename T>
std::string plotVector(FlatVector<T>* flatVector, const size_t norm = 200) {
  auto sample = convertToSample(flatVector);
  std::stringstream sstream;
  for (auto [val, num] : sample) {
    sstream << std::setw(2) << val << ' ' << std::string(num / norm, '*')
            << "\n";
  }
  return sstream.str();
}

} // namespace facebook::velox::generator_spec_examples
