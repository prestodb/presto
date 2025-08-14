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
/**
 * Simple data structure for representing 128-bit numbers as 2 64-bit integers.
 */

#ifndef COMMON_ENCODE_UINT128_H__
#define COMMON_ENCODE_UINT128_H__

#include <stdint.h>
#include <utility>

namespace facebook {

/**
 * Simple data structure for representing 128-bit numbers as 2 64-bit integers.
 *
 * Only logical operations are included; arithmetic operations are not,
 * but they could be easily added.
 *
 * This class should not have a user-defined copy constructor, assignment
 * operator, or destructor; this way, on x86_64, arguments of type UInt128
 * (passed by value) are passed around in registers instead of on the stack.
 */
class UInt128 {
 public:
  UInt128(std::pair<uint64_t, uint64_t> p) : hi_(p.first), lo_(p.second) {}
  UInt128(uint64_t hi, uint64_t lo) : hi_(hi), lo_(lo) {}
  UInt128(uint64_t lo) : hi_(0), lo_(lo) {}

  uint64_t hi() const {
    return hi_;
  }
  void setHi(uint64_t hi) {
    hi_ = hi;
  }
  uint64_t lo() const {
    return lo_;
  }
  void setLo(uint64_t lo) {
    lo_ = lo;
  }

  UInt128& operator|=(UInt128 other) {
    hi_ |= other.hi_;
    lo_ |= other.lo_;
    return *this;
  }
  UInt128 operator|(UInt128 other) const {
    UInt128 a(*this);
    return (a |= other);
  }

  UInt128& operator&=(UInt128 other) {
    hi_ &= other.hi_;
    lo_ &= other.lo_;
    return *this;
  }
  UInt128 operator&(UInt128 other) const {
    UInt128 a(*this);
    return (a &= other);
  }

  UInt128& operator<<=(uint32_t n) {
    if (n >= 64) {
      hi_ = lo_ << (n - 64);
      lo_ = 0;
    } else if (n > 0) {
      hi_ = (hi_ << n) | (lo_ >> (64 - n));
      lo_ <<= n;
    }
    return *this;
  }
  UInt128 operator<<(uint32_t n) const {
    UInt128 a(*this);
    return (a <<= n);
  }

  UInt128& operator>>=(uint32_t n) {
    if (n >= 64) {
      lo_ = hi_ >> (n - 64);
      hi_ = 0;
    } else if (n > 0) {
      lo_ = (lo_ >> n) | (hi_ << (64 - n));
      hi_ >>= n;
    }
    return *this;
  }
  UInt128 operator>>(uint32_t n) const {
    UInt128 a(*this);
    return (a >>= n);
  }

  UInt128 operator~() const {
    return UInt128(~hi_, ~lo_);
  }

  bool operator==(const UInt128& other) const = default;

 private:
  uint64_t hi_;
  uint64_t lo_;
};

} // namespace facebook

#endif /* COMMON_ENCODE_UINT128_H__ */
