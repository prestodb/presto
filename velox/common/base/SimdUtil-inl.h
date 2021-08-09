/*
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

namespace facebook::velox::simd {

namespace detail {

// Copies one datum between *from and *to at the width of T. Uses
// unaligned instructions if withd > 64 bits.
template <typename T>
inline void copyWord(void* to, const void* from) {
  if constexpr (sizeof(T) == 32) {
    *(__m256i_u*)to = *(__m256i_u*)from;
  } else if constexpr (sizeof(T) == 16) {
    *(__m128i_u*)to = *(__m128i_u*)from;
  } else {
    *reinterpret_cast<T*>(to) = *reinterpret_cast<const T*>(from);
  }
}

// Copies one element of T and advances 'to', 'from', and 'bytes' by
// sizeof(T). Returns false if 'bytes' went to 0.
template <typename T>
inline bool copyNextWord(void*& to, const void*& from, int32_t& bytes) {
  if (bytes >= sizeof(T)) {
    copyWord<T>(to, from);
    bytes -= sizeof(T);
    if (!bytes) {
      return false;
    }
    from = addBytes(from, sizeof(T));
    to = addBytes(to, sizeof(T));
    return true;
  }
  return true;
}
} // namespace detail

inline void memcpy(void* to, const void* from, int32_t bytes) {
  constexpr int32_t kByteWidth = Vectors<int64_t>::VSize * sizeof(int64_t);
  while (bytes >= kByteWidth) {
    if (!detail::copyNextWord<__m256i_u>(to, from, bytes)) {
      return;
    }
  }
  if (!detail::copyNextWord<__m128i_u>(to, from, bytes)) {
    return;
  }
  if (!detail::copyNextWord<int64_t>(to, from, bytes)) {
    return;
  }
  if (!detail::copyNextWord<int32_t>(to, from, bytes)) {
    return;
  }
  if (!detail::copyNextWord<int16_t>(to, from, bytes)) {
    return;
  }
  detail::copyNextWord<int8_t>(to, from, bytes);
}

namespace detail {
template <typename T>
inline bool setNextWord(void*& to, T data, int32_t& bytes) {
  if (bytes >= sizeof(T)) {
    if constexpr (sizeof(T) == 32) {
      *reinterpret_cast<__m256i_u*>(to) = data;
    } else if constexpr (sizeof(T) == 16) {
      *reinterpret_cast<__m128i_u*>(to) = data;
    } else {
      *reinterpret_cast<T*>(to) = data;
    }
    bytes -= sizeof(T);
    if (!bytes) {
      return false;
    }
    to = addBytes(to, sizeof(T));
    return true;
  }
  return true;
}

} // namespace detail

inline void memset(void* to, char data, int32_t bytes) {
  constexpr int32_t kByteWidth = Vectors<int64_t>::VSize * sizeof(int64_t);
  if (bytes >= kByteWidth) {
    __m256i_u data256 = _mm256_set1_epi8(data);
    while (bytes >= kByteWidth) {
      if (!detail::setNextWord<__m256i_u>(to, data256, bytes)) {
        return;
      }
    }
  }
  __m128i_u data128 = _mm_set1_epi8(data);
  if (!detail::setNextWord<__m128i_u>(to, data128, bytes)) {
    return;
  }
  int64_t data64 = data128[0];
  if (!detail::setNextWord<int64_t>(to, data64, bytes)) {
    return;
  }
  if (!detail::setNextWord<int32_t>(to, data64, bytes)) {
    return;
  }
  if (!detail::setNextWord<int16_t>(to, data64, bytes)) {
    return;
  }
  detail::setNextWord<int8_t>(to, data64, bytes);
}

} // namespace facebook::velox::simd
