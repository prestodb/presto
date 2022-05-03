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

#include <folly/dynamic.h>

#include "velox/common/base/SimdUtil.h"
#include "velox/common/base/VeloxException.h"
#include "velox/vector/BuilderTypeUtils.h"
#include "velox/vector/SimpleVector.h"

namespace facebook::velox {

/**
 * Provides a vector implementation for number types where the difference
 * between the maximum value and the minimum value is in a range that allows for
 * a smaller delta value to be encoded along with a base, bias value.  The
 * result is that we can store what might be logically a 64bit number in a space
 * that is a base 64bit value (the bias) and then smaller delta values, which
 * might be 32, 16 or 8 bit values.
 *
 * For example, we can encode the sequence of 64bit numbers:
 *   -1023, -963, -774
 * as 8bit numbers (since -774 - (-1023) = 249 which is < 255):
 *   bias = -898, values = -125, -65, 124
 *
 * or another example:
 *   1024, 1124, 900
 * as:
 *   bias = 1012, values = -12, 112, -112
 *
 * The original values at an index are retrieved via bias + value[i], e.g.
 * logical_value[1] = -898 + (-65) = -963
 *
 * To calculate the bias value we first find the delta value, which is the
 * difference between the max and min value in the vector. Since the final
 * biased values are stored in signed integer space, the bias needs to be
 * in between the max and min value of the vector. The biased values are
 * calculated by subtracting the bias value from input values. With the bias
 * in between min and max, the final values (input value - bias) will be
 * both positive and negative, utilizing the entire signed integer range.
 *
 * The bias is calculating using the formula -
 *  Delta  = maxValue - minValue
 *  Bias   = minValue + ceil(delta/2)
 *  Output = input - bias
 *
 * Example: [200, 250, 300, 420]
 *  Delta = 220 (420 - 200)
 *  Bias = 310 (200 + 220/2).
 *  Output = [-110, -60, -10, 110]
 *
 * The output values can be encoded as int8_t since they fit in the
 * range [-128, 127].
 *
 * The ceiling operation in the calculation is very important. We need
 * the delta to be higher since the absolute value of the smallest negative
 * value is higher than the largest positive value for any integer.
 * For instance, int8_t has the range [-128, 127] where abs(-128) > 127.
 *
 * Example: [500, 550, 400, 655]
 *  Delta  = 255 (655 - 400)
 *  Bias   = 528 (400 + ceil (255/2))
 *  Output = [-28,  22,  -128, 127]

 * If the bias value had been 527 instead of 528, 400 would be encoded
 * as -127 and 655 would be 128. This value (128) would not fit within int8_t
 * range.
 */
template <typename T>
class BiasVector : public SimpleVector<T> {
  // TODO(T54638002): Add support for unsigned types in biased vector
  // FIXME(venkatra): Commented out since this assertion is always true due
  // to a typo. If we fix it by changing 'admitsBias<T>' to 'admitsBias<T>()',
  // compilation fails due to lack of support for unsigned types.
  // static_assert(
  //     admitsBias<T>,
  //    "Only vectors that are stored as 64, 32 and 16-bit numbers can use
  //    bias");

 public:
  static constexpr bool can_simd =
      (std::is_same<T, int64_t>::value || std::is_same<T, int32_t>::value ||
       std::is_same<T, int16_t>::value);

  BiasVector(
      velox::memory::MemoryPool* pool,
      BufferPtr nulls,
      size_t length,
      TypeKind valueType,
      BufferPtr values,
      T bias,
      const SimpleVectorStats<T>& stats = {},
      std::optional<int32_t> distinctCount = std::nullopt,
      std::optional<vector_size_t> nullCount = std::nullopt,
      std::optional<bool> sorted = std::nullopt,
      std::optional<ByteCount> representedBytes = std::nullopt,
      std::optional<ByteCount> storageByteCount = std::nullopt);

  ~BiasVector() override {}

  inline VectorEncoding::Simple encoding() const override {
    return VectorEncoding::Simple::BIASED;
  }

  const T valueAtFast(vector_size_t idx) const;

  const T valueAt(vector_size_t idx) const override {
    SimpleVector<T>::checkElementSize();
    return valueAtFast(idx);
  }

  /**
   * Loads a SIMD vector of data at the virtual byteOffset given
   * Note this method is implemented on each vector type, but is intentionally
   * not virtual for performance reasons
   *
   * @param byteOffset - the byte offset to laod from
   */
  xsimd::batch<T> loadSIMDValueBufferAt(size_t index) const;

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  inline T bias() const {
    return bias_;
  }

  TypeKind valueType() const {
    return valueType_;
  }

  uint64_t retainedSize() const override {
    return BaseVector::retainedSize() + values_->capacity();
  }

  /**
   * Returns a shared_ptr to the underlying arrow array holding the values for
   * this vector. This is used during execution to process over the subset of
   * values when possible.
   */
  inline const BufferPtr& values() const override {
    return values_;
  }

  bool isScalar() const override {
    return true;
  }

  bool isNullsWritable() const override {
    return true;
  }

 private:
  template <typename U>
  inline xsimd::batch<T> loadSIMDInternal(size_t byteOffset) const {
    auto mem = reinterpret_cast<const U*>(
        rawValues_ + byteOffset / sizeof(T) * sizeof(U));
    return xsimd::batch<T>::load_unaligned(mem);
  }

  TypeKind valueType_;
  BufferPtr values_;
  const uint8_t* rawValues_;

  // Note: there is no 64 bit internal array as the largest number type we
  // support is 64 bit and all biasing requires a smaller internal type.
  T bias_;

  // Used to debias several values at a time.
  std::conditional_t<can_simd, xsimd::batch<T>, char> biasBuffer_;
};

template <typename T>
using BiasVectorPtr = std::shared_ptr<BiasVector<T>>;

} // namespace facebook::velox

#include "velox/vector/BiasVector-inl.h"
