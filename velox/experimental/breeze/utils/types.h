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

/*
 * Copyright (c) 2024 by Rivos Inc.
 * Licensed under the Apache License, Version 2.0, see LICENSE for details.
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "platforms/platform.h"

#ifdef __EXCEPTIONS
#include <exception>
#include <string>
#endif

namespace breeze {
namespace utils {

using size_type = int;

template <int N, int M>
struct Min {
  enum { VALUE = ((N < M) ? N : M) };
};

template <int N, int M>
struct Max {
  enum { VALUE = ((N > M) ? N : M) };
};

template <int N>
struct PowerOfTwo {
  enum { VALUE = ((N & (N - 1)) == 0) };
};

template <int N, int D>
struct DivideAndRoundUp {
  enum { VALUE = N / D + (N % D != 0 ? 1 : 0) };
};

template <int N, int D>
struct RoundUp {
  enum {
    DIVIDE_AND_ROUND_UP_VALUE = DivideAndRoundUp<N, D>::VALUE,
    VALUE = DIVIDE_AND_ROUND_UP_VALUE * D,
  };
};

template <typename T>
struct NumericLimits {
  static ATTR T min();
  static ATTR T max();
};

// specialization for T=int
template <>
struct NumericLimits<int> {
  static ATTR int min() { return -2147483648; }
  static ATTR int max() { return 2147483647; }
};

// specialization for T=unsigned
template <>
struct NumericLimits<unsigned> {
  static ATTR unsigned min() { return 0u; }
  static ATTR unsigned max() { return 4294967295u; }
};

// specialization for T=float
template <>
struct NumericLimits<float> {
  static ATTR float min() { return 1.17549435e-38f; }
  static ATTR float max() { return 3.40282347e+38f; }
};

#if !defined(PLATFORM_METAL)

// specialization for T=long long
template <>
struct NumericLimits<long long> {
  static ATTR long long min() { return -9223372036854775807ll; }
  static ATTR long long max() { return 9223372036854775807ll; }
};

// specialization for T=unsigned long long
template <>
struct NumericLimits<unsigned long long> {
  static ATTR unsigned long long min() { return 0; }
  static ATTR unsigned long long max() { return 18446744073709551615llu; }
};

// specialization for T=double
template <>
struct NumericLimits<double> {
  static ATTR double min() { return 2.22507385850720138309e-308L; }
  static ATTR double max() { return 1.79769313486231570815e+308L; }
};

#endif  // !defined(PLATFORM_METAL)

template <typename T>
struct Msb {
  enum { VALUE = sizeof(T) * /*CHAR_BIT=*/8 - 1 };
};

template <typename T>
T NextPowerOfTwo(T value);

// specialization for T=int
template <>
inline int NextPowerOfTwo(int value) {
  return value == 1 ? 1 : 1 << (32 - __builtin_clz(value - 1));
}

template <typename T, typename U>
struct IsSame {
  enum { VALUE = 0 };
};

template <typename T>
struct IsSame<T, T> {
  enum { VALUE = 1 };
};

template <typename T, typename U>
struct IsDifferent {
  enum { VALUE = !IsSame<T, U>::VALUE };
};

template <typename T>
struct RemoveConstT {
  using Type = T;
};

template <typename T>
struct RemoveConstT<const T> {
  using Type = T;
};

template <typename T>
using RemoveConst = typename RemoveConstT<T>::Type;

#ifdef __EXCEPTIONS

// custom exception used for device allocation failures
class BadDeviceAlloc : public std::exception {
 public:
  BadDeviceAlloc(size_t size, size_t free, size_t total)
      : message_("BadDeviceAlloc(size=" + std::to_string(size) +
                 ",free=" + std::to_string(free) +
                 ",total=" + std::to_string(total) + ")") {}

  virtual const char *what() const throw() { return message_.c_str(); }

 private:
  std::string message_;
};

#endif  // __EXCEPTIONS

enum AddressSpace {
  THREAD,
  SHARED,
  GLOBAL,
};

enum DataArrangement {
  BLOCKED,
  STRIPED,
  WARP_STRIPED,
};

class EmptySlice {};

ATTR EmptySlice constexpr make_empty_slice() { return EmptySlice{}; }

// metal platform requires specialization as it has native address space
// qualifiers
#if defined(PLATFORM_METAL)

template <AddressSpace A, DataArrangement B, typename T>
class Slice {};

// partial specialization for ADDRESS_SPACE=THREAD
template <DataArrangement A, typename T>
class Slice<THREAD, A, T> {
 public:
  static constant AddressSpace ADDRESS_SPACE = THREAD;
  static constant DataArrangement ARRANGEMENT = A;
  using data_type = T;

  ATTR explicit Slice(thread T *data) : data_(data) {}
  ATTR T const thread &operator[](int index) const { return data_[index]; }
  ATTR T thread &operator[](int index) { return data_[index]; }
  ATTR T const thread &operator*() const { return *data_; }
  ATTR T thread &operator*() { return *data_; }
  ATTR T const thread *operator->() const { return data_; }
  ATTR T thread *operator->() { return data_; }
  ATTR Slice<THREAD, A, T> subslice(int offset) {
    return Slice<THREAD, A, T>(data_ + offset);
  }
  ATTR const Slice<THREAD, A, T> subslice(int offset) const {
    return Slice<THREAD, A, T>(data_ + offset);
  }
  ATTR operator bool() const { return data_ != nullptr; }
  ATTR T thread *data() { return data_; }
  ATTR T const thread *data() const { return data_; }

 private:
  thread T *data_;
};

template <AddressSpace A = THREAD, DataArrangement B = STRIPED, typename T>
ATTR Slice<THREAD, B, T> constexpr make_slice(thread T *data) {
  return Slice<THREAD, B, T>(data);
}

// partial specialization for ADDRESS_SPACE=SHARED
template <DataArrangement A, typename T>
class Slice<SHARED, A, T> {
 public:
  static constant AddressSpace ADDRESS_SPACE = SHARED;
  static constant DataArrangement ARRANGEMENT = A;
  using data_type = T;

  ATTR explicit Slice(threadgroup T *data) : data_(data) {}
  ATTR T const threadgroup &operator[](int index) const { return data_[index]; }
  ATTR T threadgroup &operator[](int index) { return data_[index]; }
  ATTR T const threadgroup &operator*() const { return *data_; }
  ATTR T threadgroup &operator*() { return *data_; }
  ATTR T const threadgroup *operator->() const { return data_; }
  ATTR T threadgroup *operator->() { return data_; }
  ATTR Slice<SHARED, A, T> subslice(int offset) {
    return Slice<SHARED, A, T>(data_ + offset);
  }
  ATTR const Slice<SHARED, A, T> subslice(int offset) const {
    return Slice<SHARED, A, T>(data_ + offset);
  }
  ATTR operator bool() const { return data_ != nullptr; }
  ATTR T threadgroup *data() { return data_; }
  ATTR T const threadgroup *data() const { return data_; }

  template <AddressSpace OTHER>
  ATTR Slice<OTHER, A, T> reinterpret();
  // This is a no-op, but is needed for consistency with other platforms
  template <>
  ATTR Slice<SHARED, A, T> reinterpret<SHARED>() {
    return *this;
  }

 private:
  threadgroup T *data_;
};

template <AddressSpace A = SHARED, DataArrangement B = BLOCKED, typename T>
ATTR Slice<SHARED, B, T> constexpr make_slice(threadgroup T *data) {
  return Slice<SHARED, B, T>(data);
}

// partial specialization for ADDRESS_SPACE=GLOBAL
template <DataArrangement A, typename T>
class Slice<GLOBAL, A, T> {
 public:
  static constant AddressSpace ADDRESS_SPACE = GLOBAL;
  static constant DataArrangement ARRANGEMENT = A;
  using data_type = T;

  ATTR explicit Slice(device T *data) : data_(data) {}
  ATTR T const device &operator[](int index) const { return data_[index]; }
  ATTR T device &operator[](int index) { return data_[index]; }
  ATTR T const device &operator*() const { return *data_; }
  ATTR T device &operator*() { return *data_; }
  ATTR Slice<GLOBAL, A, T> subslice(int offset) {
    return Slice<GLOBAL, A, T>(data_ + offset);
  }
  ATTR const Slice<GLOBAL, A, T> subslice(int offset) const {
    return Slice<GLOBAL, A, T>(data_ + offset);
  }
  ATTR operator bool() const { return data_ != nullptr; }
  ATTR T device *data() { return data_; }
  ATTR T const device *data() const { return data_; }

 private:
  device T *data_;
};

template <AddressSpace A = GLOBAL, DataArrangement B = BLOCKED, typename T>
ATTR Slice<GLOBAL, B, T> constexpr make_slice(device T *data) {
  return Slice<GLOBAL, B, T>(data);
}

#elif defined(PLATFORM_OPENCL)

template <AddressSpace A, DataArrangement B, typename T>
class Slice {};

// partial specialization for ADDRESS_SPACE=THREAD
template <DataArrangement A, typename T>
class Slice<THREAD, A, T> {
 public:
  static constexpr AddressSpace ADDRESS_SPACE = THREAD;
  static constexpr DataArrangement ARRANGEMENT = A;
  using data_type = T;

  ATTR explicit Slice(__private T *data) : data_(data) {}
  ATTR T const __private &operator[](int index) const { return data_[index]; }
  ATTR T __private &operator[](int index) { return data_[index]; }
  ATTR T const __private &operator*() const { return *data_; }
  ATTR T __private &operator*() { return *data_; }
  ATTR T const __private *operator->() const { return data_; }
  ATTR T __private *operator->() { return data_; }
  ATTR Slice<THREAD, A, T> subslice(int offset) {
    return Slice<THREAD, A, T>(data_ + offset);
  }
  ATTR const Slice<THREAD, A, T> subslice(int offset) const {
    return Slice<THREAD, A, T>(data_ + offset);
  }
  ATTR operator bool() const { return data_ != nullptr; }
  ATTR T __private *data() { return data_; }
  ATTR T const __private *data() const { return data_; }

  template <AddressSpace OTHER>
  ATTR Slice<OTHER, A, T> reinterpret();
  // This is a no-op, but is needed for consistency with other platforms
  template <>
  ATTR Slice<THREAD, A, T> reinterpret<THREAD>() {
    return *this;
  }

 private:
  __private T *data_;
};

template <AddressSpace A = THREAD, DataArrangement B = STRIPED, typename T>
ATTR Slice<THREAD, B, T> constexpr make_slice(private T *data) {
  return Slice<THREAD, B, T>(data);
}

// partial specialization for ADDRESS_SPACE=SHARED
template <DataArrangement A, typename T>
class Slice<SHARED, A, T> {
 public:
  static constexpr AddressSpace ADDRESS_SPACE = SHARED;
  static constexpr DataArrangement ARRANGEMENT = A;
  using data_type = T;

  ATTR explicit Slice(local T *data) : data_(data) {}
  ATTR T const local &operator[](int index) const { return data_[index]; }
  ATTR T local &operator[](int index) { return data_[index]; }
  ATTR T const local &operator*() const { return *data_; }
  ATTR T local &operator*() { return *data_; }
  ATTR T const local *operator->() const { return data_; }
  ATTR T local *operator->() { return data_; }
  ATTR Slice<SHARED, A, T> subslice(int offset) {
    return Slice<SHARED, A, T>(data_ + offset);
  }
  ATTR const Slice<SHARED, A, T> subslice(int offset) const {
    return Slice<SHARED, A, T>(data_ + offset);
  }
  ATTR operator bool() const { return data_ != nullptr; }
  ATTR T local *data() { return data_; }
  ATTR T const local *data() const { return data_; }

  template <AddressSpace OTHER>
  ATTR Slice<OTHER, A, T> reinterpret();
  // This is a no-op, but is needed for consistency with other platforms
  template <>
  ATTR Slice<SHARED, A, T> reinterpret<SHARED>() {
    return *this;
  }

 private:
  local T *data_;
};

template <AddressSpace A = SHARED, DataArrangement B = BLOCKED, typename T>
ATTR Slice<SHARED, B, T> constexpr make_slice(local T *data) {
  return Slice<SHARED, B, T>(data);
}

// partial specialization for ADDRESS_SPACE=GLOBAL
template <DataArrangement A, typename T>
class Slice<GLOBAL, A, T> {
 public:
  static constexpr AddressSpace ADDRESS_SPACE = GLOBAL;
  static constexpr DataArrangement ARRANGEMENT = A;
  using data_type = T;

  ATTR explicit Slice(global T *data) : data_(data) {}
  ATTR T const global &operator[](int index) const { return data_[index]; }
  ATTR T global &operator[](int index) { return data_[index]; }
  ATTR T const global &operator*() const { return *data_; }
  ATTR T global &operator*() { return *data_; }
  ATTR Slice<GLOBAL, A, T> subslice(int offset) {
    return Slice<GLOBAL, A, T>(data_ + offset);
  }
  ATTR const Slice<GLOBAL, A, T> subslice(int offset) const {
    return Slice<GLOBAL, A, T>(data_ + offset);
  }
  ATTR operator bool() const { return data_ != nullptr; }
  ATTR T global *data() { return data_; }
  ATTR T const global *data() const { return data_; }

 private:
  global T *data_;
};

template <AddressSpace A = GLOBAL, DataArrangement B = BLOCKED, typename T>
ATTR Slice<GLOBAL, B, T> constexpr make_slice(global T *data) {
  return Slice<GLOBAL, B, T>(data);
}

#else  // defined(PLATFORM_OPENCL)

template <AddressSpace A, DataArrangement B, typename T>
class Slice {
 public:
  static constexpr AddressSpace ADDRESS_SPACE = A;
  static constexpr DataArrangement ARRANGEMENT = B;
  using data_type = T;
  using pointer_type = T *;

  ATTR explicit Slice(pointer_type data) : data_(data) {}
  ATTR T const &operator[](int index) const { return data_[index]; }
  ATTR T &operator[](int index) { return data_[index]; }
  ATTR T const &operator*() const { return *data_; }
  ATTR T &operator*() { return *data_; }
  ATTR T const *operator->() const { return data_; }
  ATTR T *operator->() { return data_; }
  ATTR Slice<A, B, T> subslice(int offset) {
    return Slice<A, B, T>(data_ + offset);
  }
  ATTR const Slice<A, B, T> subslice(int offset) const {
    return Slice<A, B, T>(data_ + offset);
  }
  ATTR operator bool() const { return data_ != nullptr; }
  ATTR T *data() { return data_; }
  ATTR T const *data() const { return data_; }

  template <AddressSpace OTHER, DataArrangement OTHER_ARRANGEMENT =
                                    OTHER == THREAD ? STRIPED : BLOCKED>
  ATTR Slice<OTHER, OTHER_ARRANGEMENT, T> reinterpret() {
    using other_slice_type = Slice<OTHER, OTHER_ARRANGEMENT, T>;
    return other_slice_type((typename other_slice_type::pointer_type)data_);
  }

 private:
  pointer_type data_;
};

// STRIPED arrangement by default for THREAD address space and BLOCKED
// arrangement by default for other address spaces
template <AddressSpace A = THREAD,
          DataArrangement B = A == THREAD ? STRIPED : BLOCKED, typename T>
ATTR Slice<A, B, T> constexpr make_slice(T *data) {
  return Slice<A, B, T>(data);
}

#endif  // !defined(PLATFORM_METAL) && !defined(PLATFORM_OPENCL)

}  // namespace utils
}  // namespace breeze
