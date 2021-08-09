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

#pragma once

#include <type_traits>

#include <folly/container/F14Map.h>
#include <folly/hash/Hash.h>
#include <glog/logging.h>

#include <folly/Optional.h>
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/LazyVector.h"
#include "velox/vector/TypeAliases.h"

namespace facebook::velox {

using ChannelIndex = uint32_t;

class RowVector : public BaseVector {
 public:
  RowVector(
      velox::memory::MemoryPool* pool,
      std::shared_ptr<const Type> type,
      BufferPtr nulls,
      size_t length,
      std::vector<VectorPtr> children,
      folly::Optional<vector_size_t> nullCount = folly::none)
      : BaseVector(pool, type, nulls, length, folly::none, nullCount, 1),
        childrenSize_(children.size()),
        children_(std::move(children)) {
    // Some columns may not be projected out
    VELOX_CHECK(children_.size() <= type->size());

    // Check child vector types.
    for (auto i = 0; i < children_.size(); i++) {
      const auto& child = children_[i];
      if (child) {
        VELOX_CHECK(
            child->type()->kindEquals(type->childAt(i)),
            "Unexpected child type: {}. Expected: {}",
            child->type()->toString(),
            type->childAt(i)->toString());
      }
    }
  }

  static std::shared_ptr<RowVector> createEmpty(
      std::shared_ptr<const Type> type,
      velox::memory::MemoryPool* pool);

  virtual ~RowVector() override {}

  VectorEncoding::Simple encoding() const override {
    return VectorEncoding::Simple::ROW;
  }

  bool equalValueAt(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex) const override;

  int32_t compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override;

  uint64_t hashValueAt(vector_size_t index) const override;

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  /// Return the number of child vectors.
  /// This will exactly match the number of fields.
  size_t childrenSize() const {
    return childrenSize_;
  }

  /// Get the child vector at a given offset.
  VectorPtr& childAt(ChannelIndex index) {
    VELOX_USER_CHECK_LT(index, childrenSize_);
    return children_[index];
  }

  const VectorPtr& childAt(ChannelIndex index) const {
    VELOX_USER_CHECK_LT(index, childrenSize_);
    return children_[index];
  }
  const VectorPtr& loadedChildAt(ChannelIndex index) const {
    VELOX_USER_CHECK_LT(index, childrenSize_);
    auto& child = children_[index];
    if (child->encoding() == VectorEncoding::Simple::LAZY) {
      child = child->as<LazyVector>()->loadedVectorShared();
    }
    return child;
  }

  std::vector<VectorPtr>& children() {
    return children_;
  }

  const std::vector<VectorPtr>& children() const {
    return children_;
  }

  void copy(
      const BaseVector* source,
      vector_size_t targetIndex,
      vector_size_t sourceIndex,
      vector_size_t count) override;

  void move(vector_size_t source, vector_size_t target) override;

  uint64_t retainedSize() const override {
    auto size = BaseVector::retainedSize();
    for (auto& child : children_) {
      if (child) {
        size += child->retainedSize();
      }
    }
    return size;
  }

  std::string toString(vector_size_t index) const override;

  void ensureWritable(const SelectivityVector& rows) override;

 private:
  vector_size_t childSize() const {
    bool allConstant = false;
    for (auto& child : children_) {
      if (child) {
        if (!child->isConstantEncoding()) {
          return child->size();
        }
        allConstant = true;
      }
    }
    if (!allConstant) {
      // All are nullptr.
      return 0;
    }
    // If all children are constants which do not have a meaningful
    // size, the size is one past the last referenced child index.
    return BaseVector::length_;
  }

  void appendToChildren(
      const RowVector* source,
      vector_size_t sourceIndex,
      vector_size_t count,
      vector_size_t childSize);

  const size_t childrenSize_;
  mutable std::vector<VectorPtr> children_;
};

class ArrayVector : public BaseVector {
 public:
  ArrayVector(
      velox::memory::MemoryPool* pool,
      std::shared_ptr<const Type> type,
      BufferPtr nulls,
      size_t length,
      BufferPtr offsets,
      BufferPtr lengths,
      VectorPtr elements,
      folly::Optional<vector_size_t> nullCount = folly::none)
      : BaseVector(
            pool,
            type,
            nulls,
            length,
            folly::none /*distinctValueCount*/,
            nullCount,
            folly::none /*representedByteCount*/),
        offsets_(std::move(offsets)),
        rawOffsets_(offsets_->as<vector_size_t>()),
        sizes_(std::move(lengths)),
        rawSizes_(sizes_->as<vector_size_t>()),
        elements_(BaseVector::getOrCreateEmpty(
            std::move(elements),
            type->childAt(0),
            pool)) {
    VELOX_CHECK_EQ(type->kind(), TypeKind::ARRAY);
    VELOX_CHECK(
        elements_->type()->kindEquals(type->childAt(0)),
        "Unexpected element type: {}. Expected: {}",
        elements_->type()->toString(),
        type->childAt(0)->toString());
  }

  virtual ~ArrayVector() override {}

  VectorEncoding::Simple encoding() const override {
    return VectorEncoding::Simple::ARRAY;
  }

  bool equalValueAt(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex) const override;

  int32_t compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override;

  uint64_t hashValueAt(vector_size_t index) const override;

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  void resize(vector_size_t size) override {
    if (BaseVector::length_ < size) {
      resizeIndices(size, 0, &offsets_, &rawOffsets_);
      resizeIndices(size, 0, &sizes_, &rawSizes_);
    }
    BaseVector::resize(size);
  }

  void
  setOffsetAndSize(vector_size_t i, vector_size_t offset, vector_size_t size) {
    offsets_->asMutable<vector_size_t>()[i] = offset;
    sizes_->asMutable<vector_size_t>()[i] = size;
  }

  const VectorPtr& elements() const {
    return elements_;
  }

  void setElements(VectorPtr elements) {
    elements_ = BaseVector::getOrCreateEmpty(
        std::move(elements), type()->childAt(0), pool_);
  }

  const BufferPtr& offsets() const {
    return offsets_;
  }

  const BufferPtr& sizes() const {
    return sizes_;
  }

  const vector_size_t* rawOffsets() const {
    return rawOffsets_;
  }

  const vector_size_t* rawSizes() const {
    return rawSizes_;
  }

  vector_size_t offsetAt(vector_size_t index) const {
    return rawOffsets_[index];
  }

  vector_size_t sizeAt(vector_size_t index) const {
    return rawSizes_[index];
  }

  BufferPtr mutableOffsets(size_t size) {
    if (offsets_ && offsets_->capacity() >= size * sizeof(vector_size_t)) {
      return offsets_;
    }
    resizeIndices(size, 0, &offsets_, &rawOffsets_);
    return offsets_;
  }

  BufferPtr mutableSizes(size_t size) {
    if (sizes_ && sizes_->capacity() >= size * sizeof(vector_size_t)) {
      return sizes_;
    }
    resizeIndices(size, 0, &sizes_, &rawSizes_);
    return sizes_;
  }

  void copy(
      const BaseVector* source,
      vector_size_t targetIndex,
      vector_size_t sourceIndex,
      vector_size_t count) override;

  void move(vector_size_t source, vector_size_t target) override;

  uint64_t retainedSize() const override {
    return BaseVector::retainedSize() + offsets_->capacity() +
        sizes_->capacity() + elements_->retainedSize();
  }

  std::string toString(vector_size_t index) const override;

  void ensureWritable(const SelectivityVector& rows) override;

 private:
  BufferPtr offsets_;
  const vector_size_t* rawOffsets_;
  BufferPtr sizes_;
  const vector_size_t* rawSizes_;
  VectorPtr elements_;
};

class MapVector : public BaseVector {
 public:
  MapVector(
      velox::memory::MemoryPool* pool,
      std::shared_ptr<const Type> type,
      BufferPtr nulls,
      size_t length,
      BufferPtr offsets,
      BufferPtr sizes,
      VectorPtr keys,
      VectorPtr values,
      folly::Optional<vector_size_t> nullCount = folly::none)
      : BaseVector(
            pool,
            type,
            nulls,
            length,
            folly::none /*distinctValueCount*/,
            nullCount,
            folly::none /*representedByteCount*/),
        offsets_(std::move(offsets)),
        rawOffsets_(offsets_->as<vector_size_t>()),
        sizes_(std::move(sizes)),
        rawSizes_(sizes_->as<vector_size_t>()),
        keys_(BaseVector::getOrCreateEmpty(
            std::move(keys),
            type->childAt(0),
            pool)),
        values_(BaseVector::getOrCreateEmpty(
            std::move(values),
            type->childAt(1),
            pool)) {
    VELOX_CHECK_EQ(type->kind(), TypeKind::MAP);

    VELOX_CHECK(
        keys_->type()->kindEquals(type->childAt(0)),
        "Unexpected key type: {}. Expected: {}",
        keys_->type()->toString(),
        type->childAt(0)->toString());
    VELOX_CHECK(
        values_->type()->kindEquals(type->childAt(1)),
        "Unexpected value type: {}. Expected: {}",
        values_->type()->toString(),
        type->childAt(1)->toString());
  }

  virtual ~MapVector() override {}

  VectorEncoding::Simple encoding() const override {
    return VectorEncoding::Simple::MAP;
  }

  bool equalValueAt(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex) const override;

  int32_t compare(
      const BaseVector* other,
      vector_size_t index,
      vector_size_t otherIndex,
      CompareFlags flags) const override;

  uint64_t hashValueAt(vector_size_t index) const override;

  std::unique_ptr<SimpleVector<uint64_t>> hashAll() const override;

  void resize(vector_size_t size) override {
    if (BaseVector::length_ < size) {
      resizeIndices(size, 0, &offsets_, &rawOffsets_);
      resizeIndices(size, 0, &sizes_, &rawSizes_);
    }
    BaseVector::resize(size);
  }

  const VectorPtr& mapKeys() const {
    return keys_;
  }

  const VectorPtr& mapValues() const {
    return values_;
  }

  vector_size_t reserveMap(vector_size_t offset, vector_size_t size);

  void
  setOffsetAndSize(vector_size_t i, vector_size_t offset, vector_size_t size) {
    offsets_->asMutable<vector_size_t>()[i] = offset;
    sizes_->asMutable<vector_size_t>()[i] = size;
  }

  const BufferPtr& offsets() {
    return offsets_;
  }

  const BufferPtr& sizes() {
    return sizes_;
  }

  const vector_size_t* rawOffsets() const {
    return rawOffsets_;
  }

  const vector_size_t* rawSizes() const {
    return rawSizes_;
  }

  vector_size_t offsetAt(vector_size_t index) const {
    return rawOffsets_[index];
  }

  vector_size_t sizeAt(vector_size_t index) const {
    return rawSizes_[index];
  }

  void setKeysAndValues(VectorPtr keys, VectorPtr values) {
    keys_ = BaseVector::getOrCreateEmpty(
        std::move(keys), type()->childAt(0), pool_);
    values_ = BaseVector::getOrCreateEmpty(
        std::move(values), type()->childAt(1), pool_);
  }

  BufferPtr mutableOffsets(size_t size) {
    if (offsets_ && offsets_->capacity() >= size * sizeof(vector_size_t)) {
      return offsets_;
    }
    resizeIndices(size, 0, &offsets_, &rawOffsets_);
    return offsets_;
  }

  BufferPtr mutableSizes(size_t size) {
    if (sizes_ && sizes_->capacity() >= size * sizeof(vector_size_t)) {
      return sizes_;
    }
    resizeIndices(size, 0, &sizes_, &rawSizes_);
    return sizes_;
  }

  void copy(
      const BaseVector* source,
      vector_size_t targetIndex,
      vector_size_t sourceIndex,
      vector_size_t count) override;

  void move(vector_size_t source, vector_size_t target) override;

  uint64_t retainedSize() const override {
    return BaseVector::retainedSize() + offsets_->capacity() +
        sizes_->capacity() + keys_->retainedSize() + values_->retainedSize();
  }

  std::string toString(vector_size_t index) const override;

  // Sorts all maps smallest key first. This enables linear time
  // comparison and log time lookup.
  void canonicalize(bool useStableSort = false) const;

  void ensureWritable(const SelectivityVector& rows) override;

 private:
  // Returns true if the keys for map at 'index' are sorted from first
  // to last in the type's collation order.
  bool isSorted(vector_size_t index) const;

  // makes a Buffer with 0, 1, 2,... size-1. This is later sorted to
  // get elements in key order in each map.
  BufferPtr elementIndices() const;

  BufferPtr offsets_;
  const vector_size_t* rawOffsets_;
  BufferPtr sizes_;
  const vector_size_t* rawSizes_;
  // Canonicalization, which is logically const may set the 'keys'_, 'values'_,
  // 'sortedKeys_'.
  mutable VectorPtr keys_;
  mutable VectorPtr values_;
  mutable bool sortedKeys_ = false;
};

using RowVectorPtr = std::shared_ptr<RowVector>;
using ArrayVectorPtr = std::shared_ptr<ArrayVector>;
using MapVectorPtr = std::shared_ptr<MapVector>;

} // namespace facebook::velox
