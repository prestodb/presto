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

#include <folly/container/F14Map.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/SimdUtil.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox {

namespace {
struct DummyReleaser {
  void addRef() const {}

  void release() const {}
};
} // namespace

template <typename T>
class ConstantVector final : public SimpleVector<T> {
 public:
  static constexpr bool can_simd =
      (std::is_same<T, int64_t>::value || std::is_same<T, int32_t>::value ||
       std::is_same<T, int16_t>::value || std::is_same<T, int8_t>::value ||
       std::is_same<T, bool>::value || std::is_same<T, size_t>::value);

  ConstantVector(
      velox::memory::MemoryPool* pool,
      size_t length,
      bool isNull,
      T&& val,
      const SimpleVectorStats<T>& stats = {},
      std::optional<ByteCount> representedBytes = std::nullopt,
      std::optional<ByteCount> storageByteCount = std::nullopt)
      : ConstantVector(
            pool,
            length,
            isNull,
            CppToType<T>::create(),
            std::move(val),
            stats,
            representedBytes,
            storageByteCount) {}

  ConstantVector(
      velox::memory::MemoryPool* pool,
      size_t length,
      bool isNull,
      std::shared_ptr<const Type> type,
      T&& val,
      const SimpleVectorStats<T>& stats = {},
      std::optional<ByteCount> representedBytes = std::nullopt,
      std::optional<ByteCount> storageByteCount = std::nullopt)
      : SimpleVector<T>(
            pool,
            type,
            BufferPtr(nullptr),
            length,
            stats,
            isNull ? 0 : 1 /* distinctValueCount */,
            isNull ? length : 0 /* nullCount */,
            true /*isSorted*/,
            representedBytes,
            storageByteCount),
        value_(std::move(val)),
        isNull_(isNull),
        initialized_(true) {
    makeNullsBuffer();
    // Special handling for complex types
    if (type->size() > 0) {
      // Only allow null constants to be created through this interface.
      VELOX_CHECK(isNull_);
      valueVector_ = BaseVector::create(type, 1, pool);
      valueVector_->setNull(0, true);
    }
    if (!isNull_ && std::is_same<T, StringView>::value) {
      // Copy string value.
      StringView* valuePtr = reinterpret_cast<StringView*>(&value_);
      setValue(std::string(valuePtr->data(), valuePtr->size()));
    }
    // If this is not encoded integer, or string, set value buffer
    if constexpr (can_simd) {
      valueBuffer_ = simd::setAll(value_);
    }
  }

  // Creates constant vector with value coming from 'index' element of the
  // 'base' vector. Base vector can be flat or lazy vector. Base vector cannot
  // be a constant or dictionary vector. Use BaseVector::wrapInConstant to
  // automatically peel off encodings of the base vector.
  //
  // If base vector is lazy and has not been loaded yet, loading will be delayed
  // until loadedVector() is called.
  ConstantVector(
      velox::memory::MemoryPool* pool,
      vector_size_t length,
      vector_size_t index,
      VectorPtr base,
      const SimpleVectorStats<T>& stats = {})
      : SimpleVector<T>(
            pool,
            base->type(),
            BufferPtr(nullptr),
            length,
            stats,
            std::nullopt,
            std::nullopt,
            true /*isSorted*/,
            base->representedBytes().has_value()
                ? std::optional<ByteCount>(
                      (base->representedBytes().value() / base->size()) *
                      length)
                : std::nullopt /* representedBytes */),
        valueVector_(base),
        index_(index) {
    VELOX_CHECK_NE(
        base->encoding(),
        VectorEncoding::Simple::CONSTANT,
        "Constant vector cannot wrap Constant vector");
    VELOX_CHECK_NE(
        base->encoding(),
        VectorEncoding::Simple::DICTIONARY,
        "Constant vector cannot wrap Dictionary vector");
    setInternalState();
  }

  virtual ~ConstantVector() override = default;

  inline VectorEncoding::Simple encoding() const override {
    return VectorEncoding::Simple::CONSTANT;
  }

  bool isNullAt(vector_size_t /*idx*/) const override {
    VELOX_DCHECK(initialized_);
    return isNull_;
  }

  bool mayHaveNulls() const override {
    VELOX_DCHECK(initialized_);
    return isNull_;
  }

  bool mayHaveNullsRecursive() const override {
    VELOX_DCHECK(initialized_);
    return isNull_ || (valueVector_ && valueVector_->mayHaveNullsRecursive());
  }

  void setNull(vector_size_t /*idx*/, bool /*value*/) override {
    VELOX_FAIL("setNull not supported on ConstantVector");
  }

  const uint64_t* flatRawNulls(const SelectivityVector& rows) override {
    VELOX_DCHECK(initialized_);
    if (isNull_) {
      if (BaseVector::nulls_ &&
          BaseVector::nulls_->capacity() / 8 >= rows.size()) {
        return BaseVector::rawNulls_;
      }
      BaseVector::nulls_ = AlignedBuffer::allocate<bool>(
          rows.size(), BaseVector::pool(), bits::kNull);
      BaseVector::rawNulls_ = BaseVector::nulls_->as<uint64_t>();
      return BaseVector::rawNulls_;
    }
    return nullptr;
  }

  const T valueAtFast(vector_size_t /*idx*/) const {
    VELOX_DCHECK(initialized_);
    return value_;
  }

  virtual const T valueAt(vector_size_t idx) const override {
    VELOX_DCHECK(initialized_);
    SimpleVector<T>::checkElementSize();
    return valueAtFast(idx);
  }

  BufferPtr getStringBuffer() const {
    VELOX_DCHECK(initialized_);
    return stringBuffer_;
  }

  const T* rawValues() const {
    VELOX_DCHECK(initialized_);
    return &value_;
  }

  const void* valuesAsVoid() const override {
    VELOX_DCHECK(initialized_);
    return &value_;
  }

  /**
   * Loads a 256bit vector of data at the virtual byteOffset given
   * Note this method is implemented on each vector type, but is intentionally
   * not virtual for performance reasons
   *
   * @param byteOffset - the byte offset to laod from
   */
  xsimd::batch<T> loadSIMDValueBufferAt(size_t /* byteOffset */) const {
    VELOX_DCHECK(initialized_);
    return valueBuffer_;
  }

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  uint64_t retainedSize() const override {
    VELOX_DCHECK(initialized_);
    if (valueVector_) {
      return valueVector_->retainedSize();
    }
    if (stringBuffer_) {
      return stringBuffer_->capacity();
    }
    return sizeof(T);
  }

  BaseVector* loadedVector() override {
    if (!valueVector_) {
      return this;
    }
    auto loaded = BaseVector::loadedVectorShared(valueVector_);
    if (loaded == valueVector_) {
      return this;
    }
    valueVector_ = loaded;
    setInternalState();
    return this;
  }

  const BaseVector* loadedVector() const override {
    return const_cast<ConstantVector<T>*>(this)->loadedVector();
  }

  // Fast Check to optimize for constant vectors
  bool isConstant(const SelectivityVector& rows) const override {
    return true;
  }

  bool isConstantEncoding() const override {
    return true;
  }

  bool isScalar() const override {
    return valueVector_ ? valueVector_->isScalar() : true;
  }

  const BaseVector* wrappedVector() const override {
    return valueVector_ ? valueVector_->wrappedVector() : this;
  }

  vector_size_t wrappedIndex(vector_size_t /*index*/) const override {
    return valueVector_ ? valueVector_->wrappedIndex(index_) : 0;
  }

  BufferPtr wrapInfo() const override {
    static const DummyReleaser kDummy;
    return BufferView<DummyReleaser>::create(
        reinterpret_cast<const uint8_t*>(&index_),
        sizeof(vector_size_t),
        kDummy);
  }

  // Base vector if isScalar() is false (e.g. complex type vector) or if base
  // vector is a lazy vector that hasn't been loaded yet.
  VectorPtr valueVector() const override {
    return valueVector_;
  }

  // Index of the element of the base vector that determines the value of this
  // constant vector.
  vector_size_t index() const {
    return index_;
  }

  void resize(vector_size_t size, bool setNotNull = true) override {
    BaseVector::length_ = size;
  }

  void addNulls(const uint64_t* /*bits*/, const SelectivityVector& /*rows*/)
      override {
    VELOX_FAIL("addNulls not supported");
  }

  void move(vector_size_t /*source*/, vector_size_t target) override {
    VELOX_CHECK_LT(target, BaseVector::length_);
    // nothing to do
  }

  std::optional<int32_t> compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override {
    if constexpr (!std::is_same_v<T, ComplexType>) {
      if (other->isConstantEncoding()) {
        auto otherConstant = other->asUnchecked<ConstantVector<T>>();
        if (isNull_ || otherConstant->isNull_) {
          return BaseVector::compareNulls(
              isNull_, otherConstant->isNull_, flags);
        }

        auto result =
            SimpleVector<T>::comparePrimitiveAsc(value_, otherConstant->value_);
        return flags.ascending ? result : result * -1;
      }
    }

    return SimpleVector<T>::compare(other, index, otherIndex, flags);
  }

  std::string toString() const override {
    std::stringstream out;
    out << "[" << encoding() << " " << this->type()->toString() << ": "
        << toString(index_) << " value, " << this->size() << " size]";
    return out.str();
  }

  std::string toString(vector_size_t index) const override {
    if (isScalar()) {
      return SimpleVector<T>::toString(index);
    }

    return valueVector_->toString(index_);
  }

 private:
  void setInternalState() {
    if (isLazyNotLoaded(*valueVector_)) {
      // Do not load Lazy vector
      return;
    }

    isNull_ = valueVector_->isNullAt(index_);
    BaseVector::distinctValueCount_ = isNull_ ? 0 : 1;
    BaseVector::nullCount_ = isNull_ ? BaseVector::length_ : 0;
    if (valueVector_->isScalar()) {
      auto simple = valueVector_->loadedVector()->as<SimpleVector<T>>();
      isNull_ = simple->isNullAt(index_);
      if (!isNull_) {
        value_ = simple->valueAt(index_);
        if constexpr (std::is_same<T, StringView>::value) {
          // Copy string value.
          StringView* valuePtr = reinterpret_cast<StringView*>(&value_);
          setValue(std::string(valuePtr->data(), valuePtr->size()));

          auto stringVector = simple->template as<SimpleVector<StringView>>();
          if (auto ascii = stringVector->isAscii(index_)) {
            SimpleVector<T>::setAllIsAscii(ascii.value());
          }
        }
      }
      valueVector_ = nullptr;
    }
    makeNullsBuffer();
    initialized_ = true;
  }

  void setValue(const std::string& string) {
    BaseVector::inMemoryBytes_ += string.size();
    value_ = velox::to<decltype(value_)>(string);
    if constexpr (can_simd) {
      valueBuffer_ = simd::setAll(value_);
    }
  }

  void makeNullsBuffer() {
    if (!isNull_) {
      return;
    }
    BaseVector::nulls_ =
        AlignedBuffer::allocate<uint64_t>(1, BaseVector::pool());
    BaseVector::nulls_->setSize(1);
    BaseVector::rawNulls_ = BaseVector::nulls_->as<uint64_t>();
    *BaseVector::nulls_->asMutable<uint64_t>() = bits::kNull64;
  }

  // 'valueVector_' element 'index_' represents a complex constant
  // value. 'valueVector_' is nullptr if the constant is scalar.
  VectorPtr valueVector_;
  // The index of the represented value in 'valueVector_'.
  vector_size_t index_ = 0;
  // Holds the memory for backing non-inlined values represented by StringView.
  BufferPtr stringBuffer_;
  T value_;
  bool isNull_ = false;
  bool initialized_{false};

  // This must be at end to avoid memory corruption.
  std::conditional_t<can_simd, xsimd::batch<T>, char> valueBuffer_;
};

template <>
void ConstantVector<StringView>::setValue(const std::string& string);

template <>
void ConstantVector<std::shared_ptr<void>>::setValue(const std::string& string);

template <>
void ConstantVector<ComplexType>::setValue(const std::string& string);

template <typename T>
using ConstantVectorPtr = std::shared_ptr<ConstantVector<T>>;

} // namespace facebook::velox

#include "velox/vector/ConstantVector-inl.h"
