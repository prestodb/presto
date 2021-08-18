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
template <>
inline bool VectorHasher::tryMapToRangeSimd(
    const int64_t* values,
    const SelectivityVector& rows,
    uint64_t* result) {
  bool inRange = true;
  __m256i allLow = _mm256_set1_epi64x(min_);
  __m256i allHigh = _mm256_set1_epi64x(max_);
  __m256i allOne = _mm256_set1_epi64x(1);
  vector_size_t row = rows.begin();
  for (; row + 4 <= rows.end(); row += 4) {
    __m256i data = _mm256_loadu_si256((__m256i*)(values + row));
    // value > max_
    int32_t gtMax = _mm256_movemask_epi8(_mm256_cmpgt_epi64(data, allHigh));
    // value < min_
    int32_t ltMin = _mm256_movemask_epi8(_mm256_cmpgt_epi64(allLow, data));
    // result = (value - low) + 1
    // value - (low - 1) doesn't work when low is the lowest possible (e.g.
    // std::numeric_limits<int64_t>::min())
    _mm256_storeu_si256(
        (__m256i*)(result + row),
        _mm256_add_epi64(_mm256_sub_epi64(data, allLow), allOne));
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
      result[row] = value - min_ + 1;
    }
  }
  return inRange;
}

template <>
inline bool VectorHasher::tryMapToRangeSimd(
    const int32_t* values,
    const SelectivityVector& rows,
    uint64_t* result) {
  bool inRange = true;
  __m256i allLow = _mm256_set1_epi32(min_);
  __m256i allHigh = _mm256_set1_epi32(max_);
  vector_size_t row = rows.begin();
  for (; row + 8 <= rows.end(); row += 8) {
    __m256i data = _mm256_loadu_si256((__m256i*)(values + row));
    // value > max_
    int32_t gtMax = _mm256_movemask_epi8(_mm256_cmpgt_epi32(data, allHigh));
    // value < min_
    int32_t ltMin = _mm256_movemask_epi8(_mm256_cmpgt_epi32(allLow, data));
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
    }
  }

  if (inRange) {
    for (row = rows.begin(); row < rows.end(); row++) {
      result[row] = values[row] - min_ + 1;
    }
  }
  return inRange;
}

template <>
inline bool VectorHasher::tryMapToRangeSimd(
    const int16_t* values,
    const SelectivityVector& rows,
    uint64_t* result) {
  bool inRange = true;
  __m256i allLow = _mm256_set1_epi16(min_);
  __m256i allHigh = _mm256_set1_epi16(max_);
  vector_size_t row = rows.begin();
  for (; row + 16 <= rows.end(); row += 16) {
    __m256i data = _mm256_loadu_si256((__m256i*)(values + row));
    // value > max_
    int32_t gtMax = _mm256_movemask_epi8(_mm256_cmpgt_epi16(data, allHigh));
    // value < min_
    int32_t ltMin = _mm256_movemask_epi8(_mm256_cmpgt_epi16(allLow, data));
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
    }
  }

  if (inRange) {
    for (row = rows.begin(); row < rows.end(); row++) {
      result[row] = values[row] - min_ + 1;
    }
  }
  return inRange;
}
} // namespace facebook::velox::exec
