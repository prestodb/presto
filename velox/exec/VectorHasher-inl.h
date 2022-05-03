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
namespace facebook::velox::exec {

template <typename T>
bool VectorHasher::tryMapToRangeSimd(
    const T* values,
    const SelectivityVector& rows,
    uint64_t* result) {
  bool inRange = true;
  auto allLow = xsimd::broadcast<T>(min_);
  auto allHigh = xsimd::broadcast<T>(max_);
  auto allOne = xsimd::broadcast<T>(1);
  vector_size_t row = rows.begin();
  constexpr int kWidth = xsimd::batch<T>::size;
  for (; row + kWidth <= rows.end(); row += kWidth) {
    auto data = xsimd::load_unaligned(values + row);
    int32_t gtMax = simd::toBitMask(data > allHigh);
    int32_t ltMin = simd::toBitMask(data < allLow);
    // value - (low - 1) doesn't work when low is the lowest possible (e.g.
    // std::numeric_limits<int64_t>::min())
    if constexpr (sizeof(T) == sizeof(uint64_t)) {
      (data - allLow + allOne).store_unaligned(result + row);
    }
    if ((gtMax | ltMin) != 0) {
      inRange = false;
      break;
    }
  }

  if (inRange) {
    for (; row < rows.end(); row++) {
      auto value = values[row];
      if (value > max_ || value < min_) {
        inRange = false;
        break;
      }
      if constexpr (sizeof(T) == sizeof(uint64_t)) {
        result[row] = value - min_ + 1;
      }
    }
    if constexpr (sizeof(T) != sizeof(uint64_t)) {
      for (row = rows.begin(); row < rows.end(); row++) {
        result[row] = values[row] - min_ + 1;
      }
    }
  }
  return inRange;
}

} // namespace facebook::velox::exec
