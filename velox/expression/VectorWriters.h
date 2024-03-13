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
#include <array>
#include <cstring>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

#include "velox/expression/ComplexWriterTypes.h"
#include "velox/expression/StringWriter.h"
#include "velox/expression/UdfTypeResolver.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

// This default is for scalar types.
template <typename T, typename = void>
struct VectorWriter : public VectorWriterBase {
  using exec_out_t = typename VectorExec::template resolver<T>::out_type;
  using vector_t = typename TypeToFlatVector<T>::type;

  void init(vector_t& vector, bool uniqueAndMutable = false) {
    vector_ = &vector;
    if (!uniqueAndMutable || vector.rawValues() == nullptr) {
      data_ = vector.mutableRawValues();
    } else {
      data_ = const_cast<exec_out_t*>(vector.rawValues());
    }
  }

  void ensureSize(size_t size) override {
    if (size > vector_->size()) {
      vector_->resize(size, /*setNotNull*/ false);
      data_ = vector_->mutableRawValues();
    }
  }

  VectorWriter() {}

  FOLLY_ALWAYS_INLINE exec_out_t& current() {
    return data_[offset_];
  }

  void copyCommit(const exec_out_t& data) {
    // this code path is called for copying nested structures to slices
    // in future, we want to eliminate this so all writes go directly to their
    // slice.
    data_[offset_] = data;
    vector_->setNull(offset_, false);
  }

  void commitNull() {
    vector_->setNull(offset_, true);
  }

  void commit(bool isSet) override {
    // this code path is called when the slice is top-level
    if (!isSet) {
      vector_->setNull(offset_, true);
    } else {
      vector_->setNull(offset_, false);
    }
  }

  vector_t& vector() {
    return *vector_;
  }

  vector_t* vector_;
  exec_out_t* data_;
};

template <typename V>
struct VectorWriter<Array<V>> : public VectorWriterBase {
  using vector_t = typename TypeToFlatVector<Array<V>>::type;
  using child_vector_t = typename TypeToFlatVector<V>::type;
  using exec_out_t = ArrayWriter<V>;

  void init(vector_t& vector) {
    arrayVector_ = &vector;
    childWriter_.init(static_cast<child_vector_t&>(*vector.elements().get()));
    writer_.initialize(this);
  }

  // This should be called once all rows are processed.
  void finish() override {
    writer_.elementsVector()->resize(writer_.valuesOffset_);
    arrayVector_ = nullptr;
    childWriter_.finish();
  }

  VectorWriter() = default;

  exec_out_t& current() {
    return writer_;
  }

  vector_t& vector() {
    return *arrayVector_;
  }

  void ensureSize(size_t size) override {
    if (size > arrayVector_->size()) {
      arrayVector_->resize(size);
    }
  }

  // Commit a not null value.
  void commit() {
    arrayVector_->setOffsetAndSize(
        offset_, writer_.valuesOffset_, writer_.length_);
    arrayVector_->setNull(offset_, false);
    // Will reset length to 0 and prepare elementWriter_ for the next item.
    writer_.finalize();
  }

  void finalizeNull() override {
    writer_.resetLength();
    // Call it on the children
    childWriter_.finalizeNull();
  }

  // Commit a null value.
  void commitNull() {
    finalizeNull();
    arrayVector_->setNull(offset_, true);
  }

  void commit(bool isSet) override {
    if (LIKELY(isSet)) {
      commit();
    } else {
      commitNull();
    }
  }

  void reset() {
    writer_.valuesOffset_ = 0;
  }

  vector_t* arrayVector_ = nullptr;

  exec_out_t writer_;

  VectorWriter<V> childWriter_;
};

template <typename K, typename V>
struct VectorWriter<Map<K, V>> : public VectorWriterBase {
  using vector_t = typename TypeToFlatVector<Map<K, V>>::type;
  using key_vector_t = typename TypeToFlatVector<K>::type;
  using val_vector_t = typename TypeToFlatVector<V>::type;
  using exec_out_t = MapWriter<K, V>;

  void init(vector_t& vector) {
    mapVector_ = &vector;
    keyWriter_.init(static_cast<key_vector_t&>(*vector.mapKeys()));
    valWriter_.init(static_cast<val_vector_t&>(*vector.mapValues()));
    writer_.initialize(this);
  }

  // This should be called once all rows are processed.
  void finish() override {
    // Downsize to actual used size.
    writer_.keysVector_->resize(writer_.innerOffset_);
    writer_.valuesVector_->resize(writer_.innerOffset_);
    mapVector_ = nullptr;
    keyWriter_.finish();
    valWriter_.finish();
  }

  VectorWriter() = default;

  exec_out_t& current() {
    return writer_;
  }

  vector_t& vector() {
    return *mapVector_;
  }

  void ensureSize(size_t size) override {
    if (size > mapVector_->size()) {
      mapVector_->resize(size);
    }
  }

  // Commit a not null value.
  void commit() {
    mapVector_->setOffsetAndSize(
        offset_, writer_.innerOffset_, writer_.length_);
    mapVector_->setNull(offset_, false);
    // Will reset length to 0 and prepare proxy_.valuesOffset_ for the next
    // item.
    writer_.finalize();
  }

  void finalizeNull() override {
    writer_.resetLength();
    // Call it on the children
    keyWriter_.finalizeNull();
    valWriter_.finalizeNull();
  }

  // Commit a null value.
  void commitNull() {
    finalizeNull();
    mapVector_->setNull(offset_, true);
  }

  void commit(bool isSet) override {
    if (LIKELY(isSet)) {
      commit();
    } else {
      commitNull();
    }
  }

  void reset() {
    writer_.innerOffset_ = 0;
  }

  exec_out_t writer_;

  vector_t* mapVector_;
  VectorWriter<K> keyWriter_;
  VectorWriter<V> valWriter_;
};

template <typename... T>
struct VectorWriter<Row<T...>> : public VectorWriterBase {
  using children_types = std::tuple<T...>;
  using vector_t = typename TypeToFlatVector<Row<T...>>::type;
  using exec_out_t = typename VectorExec::resolver<Row<T...>>::out_type;

  VectorWriter() {}

  void init(vector_t& vector) {
    rowVector_ = &vector;
    // Ensure that children vectors are at least of same size as top row vector.
    initVectorWriters<0>();
    resizeVectorWriters<0>(rowVector_->size());
    writer_.initialize();
  }

  // This should be called once all rows are processed.
  void finish() override {
    finishChildren(std::index_sequence_for<T...>());
  }

  exec_out_t& current() {
    return writer_;
  }

  vector_t& vector() {
    return *rowVector_;
  }

  void ensureSize(size_t size) override {
    if (size > rowVector_->size()) {
      // Note order is important here, we resize children first to ensure
      // data_ is cached and are not reset when rowVector resizes them.
      resizeVectorWriters<0>(size);
      rowVector_->resize(size, /*setNotNull*/ false);
    }
  }

  void finalizeNull() override {
    // TODO: we could pull the logic out to here also.
    writer_.finalizeNullOnChildren();
  }

  void commitNull() {
    rowVector_->setNull(writer_.offset_, true);
    finalizeNull();
  }

  void commit(bool isSet = true) override {
    VELOX_DCHECK(rowVector_->size() > writer_.offset_);

    if (LIKELY(isSet)) {
      rowVector_->setNull(writer_.offset_, false);
      writer_.finalize();
    } else {
      commitNull();
    }
  }

  void setOffset(vector_size_t offset) override {
    writer_.offset_ = offset;
  }

  void reset() {
    writer_.offset_ = 0;
  }

 private:
  template <size_t... Is>
  void finishChildren(std::index_sequence<Is...>) {
    (std::get<Is>(writer_.childrenWriters_).finish(), ...);
  }

  template <size_t I>
  void resizeVectorWriters(size_t size) {
    if constexpr (I == sizeof...(T)) {
      return;
    } else {
      std::get<I>(writer_.childrenWriters_).ensureSize(size);
      resizeVectorWriters<I + 1>(size);
    }
  }

  template <size_t I>
  void initVectorWriters() {
    if constexpr (I == sizeof...(T)) {
      return;
    } else {
      using type = std::tuple_element_t<I, children_types>;
      std::get<I>(writer_.childrenWriters_)
          .init(static_cast<typename TypeToFlatVector<type>::type&>(
              *rowVector_->childAt(I).get()));

      initVectorWriters<I + 1>();
    }
  }

  RowWriter<T...> writer_{};
  vector_t* rowVector_ = nullptr;
};

template <typename T>
struct VectorWriter<
    T,
    std::enable_if_t<std::is_same_v<T, Varchar> | std::is_same_v<T, Varbinary>>>
    : public VectorWriterBase {
  using vector_t = typename TypeToFlatVector<T>::type;
  using exec_out_t = StringWriter<>;

  void init(vector_t& vector, bool uniqueAndMutable = false) {
    proxy_.vector_ = &vector;
  }

  void ensureSize(size_t size) override {
    if (size > proxy_.vector_->size()) {
      proxy_.vector_->resize(size, /*setNotNull*/ false);
    }
  }

  VectorWriter() {}

  exec_out_t& current() {
    return proxy_;
  }

  void commitNull() {
    proxy_.vector_->setNull(proxy_.offset_, true);
  }

  void commit(bool isSet) override {
    // this code path is called when the slice is top-level
    if (isSet) {
      proxy_.finalize();
    } else {
      commitNull();
    }
    proxy_.prepareForReuse(isSet);
  }

  void setOffset(vector_size_t offset) override {
    proxy_.offset_ = offset;
  }

  vector_t& vector() {
    return *proxy_.vector_;
  }
  exec_out_t proxy_;
};

template <typename T>
struct VectorWriter<T, std::enable_if_t<std::is_same_v<T, bool>>>
    : public VectorWriterBase {
  using vector_t = typename TypeToFlatVector<T>::type;
  using exec_out_t = bool;

  void init(vector_t& vector, bool uniqueAndMutable = false) {
    vector_ = &vector;
  }

  void ensureSize(size_t size) override {
    if (size > vector_->size()) {
      vector_->resize(size, /*setNotNull*/ false);
    }
  }

  VectorWriter() {}

  bool& current() {
    return proxy_;
  }

  void copyCommit(const bool& data) {
    vector_->set(offset_, data);
  }

  void commitNull() {
    vector_->setNull(offset_, true);
  }

  void commit(bool isSet) override {
    // this code path is called when the slice is top-level
    if (isSet) {
      copyCommit(proxy_);
    } else {
      commitNull();
    }
  }

  vector_t& vector() {
    return *vector_;
  }

  bool proxy_;
  vector_t* vector_;
};

template <typename T>
struct VectorWriter<std::shared_ptr<T>> : public VectorWriterBase {
  using exec_out_t =
      typename VectorExec::template resolver<std::shared_ptr<T>>::out_type;
  using vector_t = typename TypeToFlatVector<std::shared_ptr<void>>::type;

  void init(vector_t& vector) {
    vector_ = &vector;
  }

  void ensureSize(size_t size) override {
    if (size > vector_->size()) {
      vector_->resize(size, /*setNotNull*/ false);
    }
  }

  vector_t& vector() {
    return *vector_;
  }

  VectorWriter() {}

  exec_out_t& current() {
    return proxy_;
  }

  void copyCommit(const exec_out_t& data) {
    vector_->set(offset_, data);
  }

  void commitNull() {
    vector_->setNull(offset_, true);
  }

  void commit(bool isSet) override {
    // this code path is called when the slice is top-level
    if (isSet) {
      copyCommit(proxy_);
    } else {
      commitNull();
    }
  }

  exec_out_t proxy_;
  vector_t* vector_;
};

template <typename T, bool comparable, bool orderable>
struct VectorWriter<Generic<T, comparable, orderable>>
    : public VectorWriterBase {
  using exec_out_t = GenericWriter;
  using vector_t = BaseVector;

  VectorWriter() : writer_{castWriter_, castType_, offset_} {}

  void setOffset(vector_size_t offset) override {
    offset_ = offset;
    if (castWriter_) {
      castWriter_->setOffset(offset);
    }
  }

  template <typename F>
  struct isRowWriter : public std::false_type {};

  template <typename... F>
  struct isRowWriter<writer_ptr_t<Row<F...>>> : public std::true_type {};

  void finalizeNull() override {
    if (castType_) {
      castWriter_->finalizeNull();
    }
  }

  // This should be called once all rows are processed to resize the vectors to
  // the actual used size. No need to call finish() if the generic writer is
  // never casted.
  void finish() override {
    if (castType_) {
      castWriter_->finish();
    }
  }

  template <TypeKind kind>
  void ensureCastedWriter() {
    if constexpr (TypeTraits<kind>::isPrimitiveType) {
      writer_.ensureWriter<typename KindToSimpleType<kind>::type>();
    }
  }

  void init(vector_t& vector) {
    vector_ = &vector;
    writer_.initialize(vector_);
    if (vector.type()->isPrimitiveType()) {
      TypeKind kind = vector.typeKind();
      VELOX_DYNAMIC_TYPE_DISPATCH_ALL(ensureCastedWriter, kind);
    }
  }

  void ensureSize(size_t size) override {
    if (castType_) {
      castWriter_->ensureSize(size);
    } else {
      if (vector_->size() < size) {
        vector_->resize(size, false);
      }
    }
  }

  FOLLY_ALWAYS_INLINE exec_out_t& current() {
    return writer_;
  }

  // Commit a null value.
  void commitNull() {
    if (castType_) {
      castWriter_->commit(false);
    } else {
      vector_->setNull(offset_, true);
    }
    // No need to call finalizeNull here since commitNull will call it in
    // castType_ is true.
    // otherwse there is nothing to do.
  }

  // User can only add values after casting a generic writer to an actual type.
  // If the writer has not been casted, commit should do nothing.
  void commit(bool isSet) override {
    if (!isSet) {
      commitNull();
    } else {
      // It is possible that the writer hasn't been casted when commit(true) is
      // called. This can happen when this generic writer is a child writer of a
      // complex-type writer, i.e., array, map, or row writers, and the previous
      // writing threw right after the parent's add_item was called. In this
      // case, when the next writing calls the parent's add_item(), add_item()
      // calls commitMostRecentChildItem() with the needsCommit_ (or
      // keyNeedsCommit_ and valueNeedsCommit_) flag being true. So it will call
      // commit(true) on the child writer even if the child writer is generic
      // and hasn't been casted yet. In this situation, commitNull() should have
      // been called on the parent writer at the row of the exception before
      // add_item() is called for the next row. commitNull() of the parent
      // writer resets length_ to 0, so we don't need to commit anything here
      // from the last writing.
      if (castType_) {
        castWriter_->commit(isSet);
      }
    }
  }

  FOLLY_ALWAYS_INLINE vector_t& vector() {
    return *vector_;
  }

  vector_t* vector_;
  exec_out_t writer_;
  vector_size_t offset_ = 0;

  std::shared_ptr<VectorWriterBase> castWriter_;
  TypePtr castType_;
};

class DynamicRowWriter {
 public:
  using child_writer_t = GenericWriter;
  using writers_t = std::vector<std::shared_ptr<VectorWriter<Any, void>>>;

  column_index_t size() const {
    return childrenCount_;
  }

  void set_null_at(column_index_t index) {
    VELOX_USER_CHECK_LT(
        index,
        childrenVectors_->size(),
        "Failed to access the child vector at index {}. Row vector has only {} children.",
        index,
        childrenVectors_->size());

    (*childrenVectors_)[index]->setNull(offset_, true);
    needCommit_[index] = false;
  }

  child_writer_t& get_writer_at(column_index_t index) {
    VELOX_USER_CHECK_LT(
        index,
        childrenVectors_->size(),
        "Failed to access the child vector at index {}. Row vector has only {} children.",
        index,
        childrenVectors_->size());

    needCommit_[index] = true;
    childrenWriters_[index]->setOffset(offset_);
    return childrenWriters_[index]->current();
  }

 private:
  // Make sure user do not use those.
  DynamicRowWriter() = default;

  DynamicRowWriter(const DynamicRowWriter&) = default;

  DynamicRowWriter& operator=(const DynamicRowWriter&) = default;

  void initialize(BaseVector* vector) {
    childrenVectors_ = &vector->as<RowVector>()->children();
    childrenCount_ = childrenVectors_->size();

    for (int i = 0; i < childrenCount_; ++i) {
      childrenWriters_.push_back(std::make_shared<VectorWriter<Any, void>>());
      childrenWriters_[i]->init(*(*childrenVectors_)[i]);
      childrenWriters_[i]->ensureSize(vector->size());

      needCommit_.push_back(false);
    }
  }

  void finalize() {
    for (int i = 0; i < childrenCount_; ++i) {
      if (needCommit_[i]) {
        childrenWriters_[i]->commit(true);
        needCommit_[i] = false;
      }
    }
  }

  void finalizeNullOnChildren() {
    for (int i = 0; i < childrenCount_; ++i) {
      childrenWriters_[i]->finalizeNull();
      needCommit_[i] = false;
    }
  }

  writers_t childrenWriters_;

  column_index_t childrenCount_;

  std::vector<VectorPtr>* childrenVectors_;

  std::vector<bool> needCommit_;

  vector_size_t offset_;

  template <typename A, typename B>
  friend struct VectorWriter;

  friend class GenericWriter;
};

template <>
struct VectorWriter<DynamicRow, void> : public VectorWriterBase {
  using vector_t = RowVector;
  using exec_out_t = DynamicRowWriter;

  VectorWriter() {}

  void init(vector_t& vector) {
    rowVector_ = &vector;
    writer_.initialize(rowVector_);
  }

  // This should be called once all rows are processed.
  void finish() override {
    for (int i = 0; i < writer_.childrenCount_; ++i) {
      writer_.childrenWriters_[i]->finish();
    }
  }

  exec_out_t& current() {
    return writer_;
  }

  vector_t& vector() {
    return *rowVector_;
  }

  void ensureSize(size_t size) override {
    if (size > rowVector_->size()) {
      for (int i = 0; i < writer_.childrenCount_; ++i) {
        writer_.childrenWriters_[i]->ensureSize(size);
      }
      rowVector_->resize(size, /*setNotNull*/ false);
    }
  }

  void finalizeNull() override {
    writer_.finalizeNullOnChildren();
  }

  void commitNull() {
    finalizeNull();
    rowVector_->setNull(writer_.offset_, true);
  }

  void commit(bool isSet = true) override {
    VELOX_DCHECK(rowVector_->size() > writer_.offset_);

    if (LIKELY(isSet)) {
      rowVector_->setNull(writer_.offset_, false);
      writer_.finalize();
    } else {
      commitNull();
    }
  }

  void setOffset(vector_size_t offset) override {
    writer_.offset_ = offset;
  }

  void reset() {
    writer_.offset_ = 0;
  }

 private:
  DynamicRowWriter writer_;
  vector_t* rowVector_ = nullptr;
};

template <typename T>
struct VectorWriter<CustomType<T>> : public VectorWriter<typename T::type> {};

} // namespace facebook::velox::exec
