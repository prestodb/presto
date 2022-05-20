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

#include <algorithm>
#include <cstddef>
#include <cstdint>

#ifdef __BMI2__
#include <x86intrin.h>
#endif

namespace facebook {
namespace velox {
namespace bits {

template <typename T>
inline bool isBitSet(const T* bits, int32_t idx) {
  return bits[idx / (sizeof(bits[0]) * 8)] &
      (static_cast<T>(1) << (idx & ((sizeof(bits[0]) * 8) - 1)));
}

// Masks which store all `0` except one `1`, with the position of that `1` being
// the index (from the right) of the index in the mask buffer. So e.g.
// kOneBitmasks[0] has index 0th bit (from the right) set to 1.
// So the has the sequence 0b00000001, 0b00000010, ..., 0b10000000
// The reason we do this is that it's 21% faster for setNthBit<Value> in
// benchmarks compared do doing the calculation inline (See code and results at
// P127838775).
static constexpr uint8_t kOneBitmasks[] =
    {1 << 0, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7};

// The inversion of kOneBitmasks - so stores all 1s except for one zero
// being in the sequence 0b11111110, 0b11111101, ..., 0b01111111
static constexpr uint8_t kZeroBitmasks[] = {
    static_cast<uint8_t>(~(1 << 0)),
    static_cast<uint8_t>(~(1 << 1)),
    static_cast<uint8_t>(~(1 << 2)),
    static_cast<uint8_t>(~(1 << 3)),
    static_cast<uint8_t>(~(1 << 4)),
    static_cast<uint8_t>(~(1 << 5)),
    static_cast<uint8_t>(~(1 << 6)),
    static_cast<uint8_t>(~(1 << 7)),
};

template <typename T>
inline void setBit(T* bits, int32_t idx) {
  auto bitsAs8Bit = reinterpret_cast<uint8_t*>(bits);
  bitsAs8Bit[idx / 8] |= kOneBitmasks[idx % 8];
}

template <typename T>
inline void clearBit(T* bits, int32_t idx) {
  auto bitsAs8Bit = reinterpret_cast<uint8_t*>(bits);
  bitsAs8Bit[idx / 8] &= kZeroBitmasks[idx % 8];
}

template <typename T>
inline void setBit(T* bits, int32_t idx, bool value) {
  value ? setBit(bits, idx) : clearBit(bits, idx);
}

inline void negate(char* bits, int32_t size) {
  int32_t i = 0;
  for (; i + 64 <= size; i += 64) {
    auto wordPtr = reinterpret_cast<uint64_t*>(bits + (i / 8));
    *wordPtr = ~*wordPtr;
  }
  for (; i + 8 <= size; i += 8) {
    bits[i / 8] = ~bits[i / 8];
  }
  for (; i < size; ++i) {
    bits::setBit(bits, i, !bits::isBitSet(bits, i));
  }
}

template <typename T, typename U>
constexpr inline T roundUp(T value, U factor) {
  return (value + (factor - 1)) / factor * factor;
}

constexpr inline uint64_t lowMask(int32_t bits) {
  return (1UL << bits) - 1;
}

constexpr inline uint64_t highMask(int32_t bits) {
  return lowMask(bits) << (64 - bits);
}

constexpr inline uint64_t nbytes(int32_t bits) {
  return roundUp(bits, 8) / 8;
}

constexpr inline uint64_t nwords(int32_t bits) {
  return roundUp(bits, 64) / 64;
}

inline int32_t getAndClearLastSetBit(uint16_t& bits) {
  int32_t trailingZeros = __builtin_ctz(bits);
  // erase last non-zero bit
  bits &= bits - 1;
  return trailingZeros;
}

/**
 * Invokes a function for each batch of bits (partial or full words)
 * in a given range.
 *
 * @param begin first bit to check (inclusive)
 * @param end last bit to check (exclusive)
 * @param partialWordFunc function to invoke for a partial word;
 *  takes index of the word and mask; returns a boolean which terminates
 *  the loop if false
 * @param fullWordFunc function to invoke for a full word;
 *  takes index of the word; returns a boolean which terminates
 *  the loop if false
 * @return true if completed full loop, false if terminated early
 */
template <typename PartialWordFunc, typename FullWordFunc>
inline bool testWords(
    int32_t begin,
    int32_t end,
    PartialWordFunc partialWordFunc,
    FullWordFunc fullWordFunc) {
  if (begin >= end) {
    return true;
  }
  int32_t firstWord = roundUp(begin, 64);
  int32_t lastWord = end & ~63L;
  if (lastWord < firstWord) {
    return partialWordFunc(
        lastWord / 64, lowMask(end - lastWord) & highMask(firstWord - begin));
  }
  if (begin != firstWord) {
    if (!partialWordFunc(begin / 64, highMask(firstWord - begin))) {
      return false;
    }
  }
  for (int32_t i = firstWord; i + 64 <= lastWord; i += 64) {
    if (!fullWordFunc(i / 64)) {
      return false;
    }
  }
  if (end != lastWord) {
    return partialWordFunc(lastWord / 64, lowMask(end - lastWord));
  }
  return true;
}

/**
 * Invokes a function for each batch of bits (partial or full words)
 * in a given range.
 *
 * @param begin first bit to check (inclusive)
 * @param end last bit to check (exclusive)
 * @param partialWordFunc function to invoke for a partial word;
 *  takes index of the word and mask
 * @param fullWordFunc function to invoke for a full word;
 *  takes index of the word
 */
template <typename PartialWordFunc, typename FullWordFunc>
inline void forEachWord(
    int32_t begin,
    int32_t end,
    PartialWordFunc partialWordFunc,
    FullWordFunc fullWordFunc) {
  if (begin >= end) {
    return;
  }
  int32_t firstWord = roundUp(begin, 64);
  int32_t lastWord = end & ~63L;
  if (lastWord < firstWord) {
    partialWordFunc(
        lastWord / 64, lowMask(end - lastWord) & highMask(firstWord - begin));
    return;
  }
  if (begin != firstWord) {
    partialWordFunc(begin / 64, highMask(firstWord - begin));
  }
  for (int32_t i = firstWord; i + 64 <= lastWord; i += 64) {
    fullWordFunc(i / 64);
  }
  if (end != lastWord) {
    partialWordFunc(lastWord / 64, lowMask(end - lastWord));
  }
}

/// Invokes a function for each batch of bits (partial or full words)
/// in a given range in descending order of address.
///
/// @param begin first bit to check (inclusive)
/// @param end last bit to check (exclusive)
/// @param partialWordFunc function to invoke for a partial word;
///  takes index of the word and mask; returns a boolean which terminates
///  the loop if false
/// @param fullWordFunc function to invoke for a full word;
///  takes index of the word; returns a boolean which terminates
///  the loop if false
/// @return true if completed full loop, false if terminated early
template <typename PartialWordFunc, typename FullWordFunc>
inline bool testWordsReverse(
    int32_t begin,
    int32_t end,
    PartialWordFunc partialWordFunc,
    FullWordFunc fullWordFunc) {
  if (begin >= end) {
    return true;
  }
  int32_t firstWord = roundUp(begin, 64);
  int32_t lastWord = end & ~63L;
  if (lastWord < firstWord) {
    return partialWordFunc(
        lastWord / 64, lowMask(end - lastWord) & highMask(firstWord - begin));
  }
  if (end != lastWord) {
    if (!partialWordFunc(lastWord / 64, lowMask(end - lastWord))) {
      return false;
    }
  }
  for (int32_t i = lastWord - 64; i >= firstWord; i -= 64) {
    if (!fullWordFunc(i / 64)) {
      return false;
    }
  }
  if (begin != firstWord) {
    return partialWordFunc(begin / 64, highMask(firstWord - begin));
  }
  return true;
}

inline void fillBits(uint64_t* bits, int32_t begin, int32_t end, bool value) {
  forEachWord(
      begin,
      end,
      [bits, value](int32_t idx, uint64_t mask) {
        if (value) {
          bits[idx] |= static_cast<uint64_t>(-1) & mask;
        } else {
          bits[idx] &= ~mask;
        }
      },
      [bits, value](int32_t idx) { bits[idx] = value ? -1 : 0; });
}

inline int32_t countBits(const uint64_t* bits, int32_t begin, int32_t end) {
  int32_t count = 0;
  forEachWord(
      begin,
      end,
      [&count, bits](int32_t idx, uint64_t mask) {
        count += __builtin_popcountll(bits[idx] & mask);
      },
      [&count, bits](int32_t idx) {
        count += __builtin_popcountll(bits[idx]);
      });
  return count;
}

/**
 * Reverses the order of bits for every byte in an array of bytes. The Presto
 * wire format represents null flags with bits in reverse order, i.e. the bit
 * for the first value is the high bit.
 * @param bytes The byte array to be reversed
 * @param numBytes The number of bytes of the byte array
 */
inline void reverseBits(uint8_t* bytes, int numBytes) {
  for (int i = 0; i < numBytes; ++i) {
    auto byte = bytes[i];
    bytes[i] = ((byte & 0x01) << 7) | ((byte & 0x02) << 5) |
        ((byte & 0x4) << 3) | ((byte & 0x08) << 1) | ((byte & 0x10) >> 1) |
        ((byte & 0x20) >> 3) | ((byte & 0x40) >> 5) | ((byte & 0x80) >> 7);
  }
}

inline bool
isAllSet(const uint64_t* bits, int32_t begin, int32_t end, bool value = true) {
  if (begin >= end) {
    return true;
  }
  uint64_t word = value ? -1 : 0;
  return testWords(
      begin,
      end,
      [bits, word](int32_t idx, uint64_t mask) {
        return (word & mask) == (bits[idx] & mask);
      },
      [bits, word](int32_t idx) { return word == bits[idx]; });
}

inline int32_t findFirstBit(const uint64_t* bits, int32_t begin, int32_t end) {
  int32_t found = -1;
  testWords(
      begin,
      end,
      [bits, &found](int32_t idx, uint64_t mask) {
        uint64_t word = bits[idx] & mask;
        if (word) {
          found = idx * 64 + __builtin_ctzll(word);
          return false;
        }
        return true;
      },
      [bits, &found](int32_t idx) {
        uint64_t word = bits[idx];
        if (word) {
          found = idx * 64 + __builtin_ctzll(word);
          return false;
        }
        return true;
      });
  return found;
}

/**
 * Invokes a function for each set or unset bit.
 *
 * @param begin first bit to process (inclusive)
 * @param end last bit to process (exclusive)
 * @param isSet determines whether the function is called for each
 *        set or unset bit
 * @param func function to call; takes the index of the bit
 */
template <typename Callable>
void forEachBit(
    const uint64_t* bits,
    int32_t begin,
    int32_t end,
    bool isSet,
    Callable func) {
  static constexpr uint64_t kAllSet = -1ULL;
  forEachWord(
      begin,
      end,
      [isSet, bits, func](int32_t idx, uint64_t mask) {
        auto word = (isSet ? bits[idx] : ~bits[idx]) & mask;
        if (!word) {
          return;
        }
        while (word) {
          func(idx * 64 + __builtin_ctzll(word));
          word &= word - 1;
        }
      },
      [isSet, bits, func](int32_t idx) {
        auto word = (isSet ? bits[idx] : ~bits[idx]);
        if (kAllSet == word) {
          const size_t start = idx * 64;
          const size_t end = (idx + 1) * 64;
          for (size_t row = start; row < end; ++row) {
            func(row);
          }
        } else {
          while (word) {
            func(idx * 64 + __builtin_ctzll(word));
            word &= word - 1;
          }
        }
      });
}

/// Invokes a function for each set bit.
template <typename Callable>
inline void
forEachSetBit(const uint64_t* bits, int32_t begin, int32_t end, Callable func) {
  forEachBit(bits, begin, end, true, func);
}

/// Invokes a function for each unset bit.
template <typename Callable>
inline void forEachUnsetBit(
    const uint64_t* bits,
    int32_t begin,
    int32_t end,
    Callable func) {
  forEachBit(bits, begin, end, false, func);
}

/**
 * Invokes a function for each set or unset bit.
 *
 * @param begin first bit to check (inclusive)
 * @param end last bit to check (exclusive)
 * @param isSet determines whether the function is called for each
 *        set or unset bit
 * @param func function to call; takes the index of the bit and
 *        returns a boolean which terminates the loop if false
 * @return true if completed full loop, false if terminated early
 */
template <typename Callable>
bool testBits(
    const uint64_t* bits,
    int32_t begin,
    int32_t end,
    bool isSet,
    Callable func) {
  return testWords(
      begin,
      end,
      [isSet, bits, func](int32_t idx, uint64_t mask) {
        auto word = (isSet ? bits[idx] : ~bits[idx]) & mask;
        if (!word) {
          return true;
        }
        while (word) {
          if (!func(idx * 64 + __builtin_ctzll(word))) {
            return false;
          }
          word &= word - 1;
        }
        return true;
      },
      [isSet, bits, func](int32_t idx) {
        auto word = (isSet ? bits[idx] : ~bits[idx]);
        if (!word) {
          return true;
        }
        while (word) {
          if (!func(idx * 64 + __builtin_ctzll(word))) {
            return false;
          }
          word &= word - 1;
        }
        return true;
      });
}

/// Invokes a function for each set bit.
template <typename Callable>
inline bool
testSetBits(const uint64_t* bits, int32_t begin, int32_t end, Callable func) {
  return testBits(bits, begin, end, true, func);
}

/// Invokes a function for each unset bit.
template <typename Callable>
inline bool
testUnsetBits(const uint64_t* bits, int32_t begin, int32_t end, Callable func) {
  return testBits(bits, begin, end, false, func);
}

inline int32_t findLastBit(
    const uint64_t* bits,
    int32_t begin,
    int32_t end,
    bool value = true) {
  int32_t found = -1;
  testWordsReverse(
      begin,
      end,
      [bits, &found, value](int32_t idx, uint64_t mask) {
        uint64_t word = (value ? bits[idx] : ~bits[idx]) & mask;
        if (word) {
          found = idx * 64 + 63 - __builtin_clzll(word);
          return false;
        }
        return true;
      },
      [bits, &found, value](int32_t idx) {
        uint64_t word = value ? bits[idx] : ~bits[idx];
        if (word) {
          found = idx * 64 + 63 - __builtin_clzll(word);
          return false;
        }
        return true;
      });
  return found;
}

inline int32_t
findLastUnsetBit(const uint64_t* bits, int32_t begin, int32_t end) {
  return findLastBit(bits, begin, end, false);
}

template <bool negate>
inline void andRange(
    uint64_t* target,
    const uint64_t* left,
    const uint64_t* right,
    int32_t begin,
    int32_t end) {
  forEachWord(
      begin,
      end,
      [target, left, right](int32_t idx, uint64_t mask) {
        target[idx] = (target[idx] & ~mask) |
            (mask & left[idx] & (negate ? ~right[idx] : right[idx]));
      },
      [target, left, right](int32_t idx) {
        target[idx] = left[idx] & (negate ? ~right[idx] : right[idx]);
      });
}

template <bool negate>
inline void orRange(
    uint64_t* target,
    const uint64_t* left,
    const uint64_t* right,
    int32_t begin,
    int32_t end) {
  forEachWord(
      begin,
      end,
      [target, left, right](int32_t idx, uint64_t mask) {
        target[idx] = (target[idx] & ~mask) |
            (mask & (left[idx] | (negate ? ~right[idx] : right[idx])));
      },
      [target, left, right](int32_t idx) {
        target[idx] = left[idx] | (negate ? ~right[idx] : right[idx]);
      });
}

// Bit-wise AND: target = left AND right
inline void andBits(
    uint64_t* target,
    const uint64_t* left,
    const uint64_t* right,
    int32_t begin,
    int32_t end) {
  andRange<false>(target, left, right, begin, end);
}

// Bit-wise in-place AND: target = target AND right
inline void
andBits(uint64_t* target, const uint64_t* right, int32_t begin, int32_t end) {
  andRange<false>(target, target, right, begin, end);
}

// Bit-wise AND NOT: target = left AND !right
inline void andWithNegatedBits(
    uint64_t* target,
    const uint64_t* left,
    const uint64_t* right,
    int32_t begin,
    int32_t end) {
  andRange<true>(target, left, right, begin, end);
}

// Bit-wise in-place AND NOT: target = target AND !right
inline void andWithNegatedBits(
    uint64_t* target,
    const uint64_t* right,
    int32_t begin,
    int32_t end) {
  andRange<true>(target, target, right, begin, end);
}

// Bit-wise OR: target = left OR right
inline void orBits(
    uint64_t* target,
    const uint64_t* left,
    const uint64_t* right,
    int32_t begin,
    int32_t end) {
  orRange<false>(target, left, right, begin, end);
}

// Bit-wise in-place OR: target = target OR right
inline void
orBits(uint64_t* target, const uint64_t* right, int32_t begin, int32_t end) {
  orRange<false>(target, target, right, begin, end);
}

// Bit-wise OR NOT: target = left OR !right
inline void orWithNegatedBits(
    uint64_t* target,
    const uint64_t* left,
    const uint64_t* right,
    int32_t begin,
    int32_t end) {
  orRange<true>(target, left, right, begin, end);
}

// Bit-wise in-place OR NOT: target = target OR !right
inline void orWithNegatedBits(
    uint64_t* target,
    const uint64_t* right,
    int32_t begin,
    int32_t end) {
  orRange<true>(target, target, right, begin, end);
}

inline bool isSubset(
    const uint64_t* sub,
    const uint64_t* super,
    int32_t begin,
    int32_t end) {
  return testWords(
      begin,
      end,
      [sub, super](int32_t idx, uint64_t mask) {
        auto subWord = sub[idx] & mask;
        return (super[idx] & subWord) == subWord;
      },
      [sub, super](int32_t idx) {
        auto subWord = sub[idx];
        return (super[idx] & subWord) == subWord;
      });
}

bool inline hasIntersection(
    const uint64_t* left,
    const uint64_t* right,
    int32_t begin,
    int32_t end) {
  if (begin >= end) {
    return false;
  }
  return !testWords(
      begin,
      end,
      [left, right](int32_t idx, uint64_t mask) {
        auto leftWord = left[idx] & mask;
        return (right[idx] & leftWord) == 0;
      },
      [left, right](int32_t idx) {
        auto leftWord = left[idx];
        return (right[idx] & leftWord) == 0;
      });
}

inline int32_t countLeadingZeros(uint64_t word) {
  return __builtin_clzll(word);
}

inline uint64_t nextPowerOfTwo(uint64_t size) {
  if (size == 0) {
    return 0;
  }
  uint32_t bits = 63 - countLeadingZeros(size);
  uint64_t lower = 1U << bits;
  // Size is a power of 2.
  if (lower == size) {
    return size;
  }
  return 2 * lower;
}

inline bool isPowerOfTwo(uint64_t size) {
  return bits::countBits(&size, 0, sizeof(uint64_t) * 8) <= 1;
}

// This is the Hash128to64 function from Google's cityhash (available
// under the MIT License).  We use it to reduce multiple 64 bit hashes
// into a single hash.
#if defined(FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER)
FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER("unsigned-integer-overflow")
#endif
inline uint64_t hashMix(const uint64_t upper, const uint64_t lower) noexcept {
  // Murmur-inspired hashing.
  const uint64_t kMul = 0x9ddfea08eb382d69ULL;
  uint64_t a = (lower ^ upper) * kMul;
  a ^= (a >> 47);
  uint64_t b = (upper ^ a) * kMul;
  b ^= (b >> 47);
  b *= kMul;
  return b;
}

// Order-independent way to reduce multiple 64 bit hashes into a
// single hash. Copied from folly/hash/Hash.h because this is not
// defined in some versions of folly.
#if defined(FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER)
FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER("unsigned-integer-overflow")
#endif
inline uint64_t commutativeHashMix(
    const uint64_t upper,
    const uint64_t lower) noexcept {
  // Commutative accumulator taken from this paper:
  // https://www.preprints.org/manuscript/201710.0192/v1/download
  return 3860031 + (upper + lower) * 2779 + (upper * lower * 2);
}

inline uint64_t loadPartialWord(const uint8_t* data, int32_t size) {
  // Must be declared volatile, else gcc misses aliasing in optimized mode.
  volatile uint64_t result = 0;
  auto resultPtr = reinterpret_cast<volatile uint8_t*>(&result);
  auto begin = data;
  auto toGo = size;
  if (toGo >= 4) {
    *reinterpret_cast<volatile uint32_t*>(resultPtr) =
        *reinterpret_cast<const uint32_t*>(begin);
    begin += 4;
    resultPtr += 4;
    toGo -= 4;
  }
  if (toGo >= 2) {
    *reinterpret_cast<volatile uint16_t*>(resultPtr) =
        *reinterpret_cast<const uint16_t*>(begin);
    begin += 2;
    resultPtr += 2;
    toGo -= 2;
  }
  if (toGo == 1) {
    *reinterpret_cast<volatile uint8_t*>(resultPtr) =
        *reinterpret_cast<const uint8_t*>(begin);
  }
  return result;
}

inline size_t hashBytes(size_t seed, const char* data, size_t size) {
  auto begin = reinterpret_cast<const uint8_t*>(data);
  if (size < 8) {
    return hashMix(seed, loadPartialWord(begin, size));
  }
  auto result = seed;
  auto end = begin + size;
  while (begin + 8 <= end) {
    result = hashMix(result, *reinterpret_cast<const uint64_t*>(begin));
    begin += 8;
  }
  if (end != begin) {
    // Accesses the last 64 bits. Some bytes may get processed twice but the
    // access is safe.
    result = hashMix(result, *reinterpret_cast<const uint64_t*>(end - 8));
  }
  return result;
}

namespace detail {
// Returns at least 'numBits' bits of data starting at bit 'bitOffset'
// from 'source'. T must be at least 'numBits' wide. If 'numBits' bits
// from 'bitIffset' do not in T, loads the next byte to get the extra
// bits.
template <typename T>
inline T loadBits(const uint64_t* source, uint64_t bitOffset, uint8_t numBits) {
  constexpr int32_t kBitSize = 8 * sizeof(T);
  auto address = reinterpret_cast<uint64_t>(source) + bitOffset / 8;
  T word = *reinterpret_cast<const T*>(address);
  auto bit = bitOffset & 7;
  if (!bit) {
    return word;
  }
  if (numBits + bit <= kBitSize) {
    return word >> bit;
  }
  uint8_t lastByte = reinterpret_cast<const uint8_t*>(address)[sizeof(T)];
  uint64_t lastBits = static_cast<T>(lastByte) << (kBitSize - bit);
  return (word >> bit) | lastBits;
}

// Stores the 'numBits' low bits of 'word' into bits starting at the
// 'bitOffset'th bit from target. T must be at least 'numBits'
// wide. If the bit field that is stored overflows a word of T, writes
// the trailing bits in the low bits of the next byte. Preserves all
// bits below and above the written bits.
template <typename T>
inline void
storeBits(uint64_t* target, uint64_t offset, uint64_t word, uint8_t numBits) {
  constexpr int32_t kBitSize = 8 * sizeof(T);
  T* address =
      reinterpret_cast<T*>(reinterpret_cast<uint64_t>(target) + (offset / 8));
  auto bitOffset = offset & 7;
  uint64_t mask = (numBits == 64 ? ~0UL : ((1UL << numBits) - 1)) << bitOffset;
  *address = (*address & ~mask) | (mask & (word << bitOffset));
  if (numBits + bitOffset > kBitSize) {
    uint8_t* lastByteAddress = reinterpret_cast<uint8_t*>(address) + sizeof(T);
    uint8_t lastByteBits = bitOffset + numBits - kBitSize;
    uint8_t lastByteMask = (1 << lastByteBits) - 1;
    *lastByteAddress =
        (*lastByteAddress & ~lastByteMask) | (word >> (kBitSize - bitOffset));
  }
}
} // namespace detail

// Copies a string of bits between locations in memory given by an
// address and a bit offset for source and destination.
inline void copyBits(
    const uint64_t* source,
    uint64_t sourceOffset,
    uint64_t* target,
    uint64_t targetOffset,
    uint64_t numBits) {
  uint64_t i = 0;
  for (; i + 64 <= numBits; i += 64) {
    uint64_t word = detail::loadBits<uint64_t>(source, i + sourceOffset, 64);
    detail::storeBits<uint64_t>(target, targetOffset + i, word, 64);
  }
  if (i + 32 <= numBits) {
    auto lastWord = detail::loadBits<uint32_t>(source, sourceOffset + i, 32);
    detail::storeBits<uint32_t>(target, targetOffset + i, lastWord, 32);
    i += 32;
  }
  if (i + 16 <= numBits) {
    auto lastWord = detail::loadBits<uint16_t>(source, sourceOffset + i, 16);
    detail::storeBits<uint16_t>(target, targetOffset + i, lastWord, 16);
    i += 16;
  }
  for (; i < numBits; i += 8) {
    auto copyBits = std::min<uint64_t>(numBits - i, 8);
    auto lastWord =
        detail::loadBits<uint8_t>(source, sourceOffset + i, copyBits);
    detail::storeBits<uint8_t>(target, targetOffset + i, lastWord, copyBits);
  }
}

// Copies consecutive bits from 'source' to positions in 'target'
// where 'targetMask' has a 1. 'source' may be a prefix of 'target',
// so that contiguous bits of source are scattered in place. The
// positions of 'target' where 'targetMask' is 0 are 0. A sample use
// case is reading a column of boolean with nulls. The booleans
// from the column get inserted into the places given by ones in the
// present bitmap.
void scatterBits(
    int32_t numSource,
    int32_t numTarget,
    const char* source,
    const uint64_t* targetMask,
    char* target);

// Extract bits from integer 'a' at the corresponding bit locations
// specified by 'mask' to contiguous low bits in return value; the
// remaining upper bits in return value are set to zero.
template <typename T>
inline T extractBits(T a, T mask);

#ifdef __BMI2__
template <>
inline uint32_t extractBits(uint32_t a, uint32_t mask) {
  return _pext_u32(a, mask);
}
template <>
inline uint64_t extractBits(uint64_t a, uint64_t mask) {
  return _pext_u64(a, mask);
}
#else
template <typename T>
T extractBits(T a, T mask) {
  constexpr int kBitsCount = 8 * sizeof(T);
  T dst = 0;
  for (int i = 0, k = 0; i < kBitsCount; ++i) {
    if (mask & 1) {
      dst |= ((a & 1) << k);
      ++k;
    }
    a >>= 1;
    mask >>= 1;
  }
  return dst;
}
#endif

// Shift the bits of unsigned 32-bit integer a left by the number of
// bits specified in shift, rotating the most-significant bit to the
// least-significant bit location, and return the unsigned result.
inline uint32_t rotateLeft(uint32_t a, int shift) {
#ifdef __BMI2__
  return _rotl(a, shift);
#else
  return (a << shift) | (a >> (32 - shift));
#endif
}

} // namespace bits
} // namespace velox
} // namespace facebook
