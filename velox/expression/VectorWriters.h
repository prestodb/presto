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

#include "velox/expression/ComplexWriterTypes.h"
#include "velox/expression/StringWriter.h"
#include "velox/expression/UdfTypeResolver.h"
#include "velox/type/Type.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::exec {

// This default is for scalar types.
template <typename T, typename = void>
struct VectorWriter {
  using exec_out_t = typename VectorExec::template resolver<T>::out_type;
  using vector_t = typename TypeToFlatVector<T>::type;

  void init(vector_t& vector) {
    vector_ = &vector;
    data_ = vector.mutableRawValues();
  }

  void finish() {}

  void ensureSize(size_t size) {
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

  void commit(bool isSet) {
    // this code path is called when the slice is top-level
    if (!isSet) {
      vector_->setNull(offset_, true);
    } else {
      vector_->setNull(offset_, false);
    }
  }

  FOLLY_ALWAYS_INLINE void setOffset(int32_t offset) {
    offset_ = offset;
  }

  FOLLY_ALWAYS_INLINE vector_t& vector() {
    return *vector_;
  }

  vector_t* vector_;
  exec_out_t* data_;
  size_t offset_ = 0;
};

template <typename V>
struct VectorWriter<Array<V>> {
  using vector_t = typename TypeToFlatVector<Array<V>>::type;
  using child_vector_t = typename TypeToFlatVector<V>::type;
  using exec_out_t = ArrayWriter<V>;

  void init(vector_t& vector) {
    arrayVector_ = &vector;
    childWriter_.init(static_cast<child_vector_t&>(*vector.elements().get()));
    writer_.initialize(this);
  }

  // This should be called once all rows are processed.
  void finish() {
    writer_.elementsVector_->resize(writer_.valuesOffset_);
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

  void ensureSize(size_t size) {
    if (size > arrayVector_->size()) {
      arrayVector_->resize(size);
      childWriter_.init(
          static_cast<child_vector_t&>(*arrayVector_->elements()));
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

  // Commit a null value.
  void commitNull() {
    writer_.finalizeNull();
    arrayVector_->setNull(offset_, true);
  }

  void commit(bool isSet) {
    if (LIKELY(isSet)) {
      commit();
    } else {
      commitNull();
    }
  }

  // Set the index being written.
  void setOffset(vector_size_t offset) {
    offset_ = offset;
  }

  void reset() {
    writer_.valuesOffset_ = 0;
  }

  vector_t* arrayVector_ = nullptr;

  exec_out_t writer_;

  VectorWriter<V> childWriter_;

  // The index being written in the array vector.
  vector_size_t offset_ = 0;
};

template <typename K, typename V>
struct VectorWriter<Map<K, V>> {
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
  void finish() {
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

  void ensureSize(size_t size) {
    if (size > mapVector_->size()) {
      mapVector_->resize(size);
      init(vector());
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

  // Commit a null value.
  void commitNull() {
    writer_.finalizeNull();
    mapVector_->setNull(offset_, true);
  }

  void commit(bool isSet) {
    if (LIKELY(isSet)) {
      commit();
    } else {
      commitNull();
    }
  }

  // Set the index being written.
  void setOffset(vector_size_t offset) {
    offset_ = offset;
  }

  void reset() {
    writer_.innerOffset_ = 0;
  }

  exec_out_t writer_;

  vector_t* mapVector_;
  VectorWriter<K> keyWriter_;
  VectorWriter<V> valWriter_;
  size_t offset_ = 0;
};

template <typename... T>
struct VectorWriter<Row<T...>> {
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
  void finish() {
    finishChildren(std::index_sequence_for<T...>());
  }

  exec_out_t& current() {
    return writer_;
  }

  vector_t& vector() {
    return *rowVector_;
  }

  void ensureSize(size_t size) {
    if (size > rowVector_->size()) {
      rowVector_->resize(size, /*setNotNull*/ false);
      resizeVectorWriters<0>(rowVector_->size());
    }
  }

  void commitNull() {
    rowVector_->setNull(writer_.offset_, true);
  }

  void commit(bool isSet = true) {
    VELOX_DCHECK(rowVector_->size() > writer_.offset_);

    if (LIKELY(isSet)) {
      rowVector_->setNull(writer_.offset_, false);
      writer_.finalize();
    } else {
      commitNull();
    }
  }

  void setOffset(size_t offset) {
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
    std::enable_if_t<
        std::is_same_v<T, Varchar> | std::is_same_v<T, Varbinary>>> {
  using vector_t = typename TypeToFlatVector<T>::type;
  using exec_out_t = StringWriter<>;

  void init(vector_t& vector) {
    proxy_.vector_ = &vector;
  }

  void finish() {}

  void ensureSize(size_t size) {
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

  void commit(bool isSet) {
    // this code path is called when the slice is top-level
    if (isSet) {
      proxy_.finalize();
    } else {
      commitNull();
    }
    proxy_.prepareForReuse(isSet);
  }

  void setOffset(int32_t offset) {
    proxy_.offset_ = offset;
  }

  vector_t& vector() {
    return *proxy_.vector_;
  }
  exec_out_t proxy_;
};

template <typename T>
struct VectorWriter<T, std::enable_if_t<std::is_same_v<T, bool>>> {
  using vector_t = typename TypeToFlatVector<T>::type;
  using exec_out_t = bool;

  void init(vector_t& vector) {
    vector_ = &vector;
  }

  void finish() {}

  void ensureSize(size_t size) {
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

  void commit(bool isSet) {
    // this code path is called when the slice is top-level
    if (isSet) {
      copyCommit(proxy_);
    } else {
      commitNull();
    }
  }

  void setOffset(int32_t offset) {
    offset_ = offset;
  }

  vector_t& vector() {
    return *vector_;
  }

  bool proxy_;
  vector_t* vector_;
  size_t offset_ = 0;
};

template <typename T>
struct VectorWriter<std::shared_ptr<T>> {
  using exec_out_t =
      typename VectorExec::template resolver<std::shared_ptr<T>>::out_type;
  using vector_t = typename TypeToFlatVector<std::shared_ptr<void>>::type;

  void init(vector_t& vector) {
    vector_ = &vector;
  }

  void finish() {}

  void ensureSize(size_t size) {
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

  void commit(bool isSet) {
    // this code path is called when the slice is top-level
    if (isSet) {
      copyCommit(proxy_);
    } else {
      commitNull();
    }
  }

  void setOffset(int32_t offset) {
    offset_ = offset;
  }

  exec_out_t proxy_;
  vector_t* vector_;
  size_t offset_ = 0;
};

} // namespace facebook::velox::exec
