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

#include "velox/vector/fuzzer/ConstrainedVectorGenerator.h"

#include "velox/expression/VectorWriters.h"

namespace facebook::velox::fuzzer {

using exec::GenericWriter;
using exec::VectorWriter;

// static
VectorPtr ConstrainedVectorGenerator::generateConstant(
    const AbstractInputGeneratorPtr& customGenerator,
    vector_size_t size,
    memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(customGenerator);
  VELOX_CHECK(customGenerator->type()->isPrimitiveType());

  const auto& type = customGenerator->type();
  const auto variant = customGenerator->generate();

  return BaseVector::createConstant(type, variant, size, pool);
}

template <TypeKind KIND>
void writeOne(const variant& v, GenericWriter& writer);
template <>
void writeOne<TypeKind::VARCHAR>(const variant& v, GenericWriter& writer);
template <>
void writeOne<TypeKind::VARBINARY>(const variant& v, GenericWriter& writer);
template <>
void writeOne<TypeKind::ARRAY>(const variant& v, GenericWriter& writer);
template <>
void writeOne<TypeKind::MAP>(const variant& v, GenericWriter& writer);
template <>
void writeOne<TypeKind::ROW>(const variant& v, GenericWriter& writer);

template <TypeKind KIND>
void writeOne(const variant& v, GenericWriter& writer) {
  using T = typename TypeTraits<KIND>::NativeType;
  writer.template castTo<T>() = v.value<KIND>();
}

template <>
void writeOne<TypeKind::VARCHAR>(const variant& v, GenericWriter& writer) {
  writer.template castTo<Varchar>() = v.value<TypeKind::VARCHAR>();
}

template <>
void writeOne<TypeKind::VARBINARY>(const variant& v, GenericWriter& writer) {
  writer.template castTo<Varbinary>() = v.value<TypeKind::VARBINARY>();
}

template <>
void writeOne<TypeKind::ARRAY>(const variant& v, GenericWriter& writer) {
  auto& writerTyped = writer.template castTo<Array<Any>>();
  const auto& elements = v.array();
  for (const auto& element : elements) {
    if (element.isNull()) {
      writerTyped.add_null();
    } else {
      VELOX_DYNAMIC_TYPE_DISPATCH(
          writeOne, element.kind(), element, writerTyped.add_item());
    }
  }
}

template <>
void writeOne<TypeKind::MAP>(const variant& v, GenericWriter& writer) {
  auto& writerTyped = writer.template castTo<Map<Any, Any>>();
  const auto& map = v.map();
  for (const auto& pair : map) {
    const auto& key = pair.first;
    const auto& value = pair.second;
    VELOX_CHECK(!key.isNull());
    if (value.isNull()) {
      VELOX_DYNAMIC_TYPE_DISPATCH(
          writeOne, key.kind(), key, writerTyped.add_null());
    } else {
      auto writers = writerTyped.add_item();
      VELOX_DYNAMIC_TYPE_DISPATCH(
          writeOne, key.kind(), key, std::get<0>(writers));
      VELOX_DYNAMIC_TYPE_DISPATCH(
          writeOne, value.kind(), value, std::get<1>(writers));
    }
  }
}

template <>
void writeOne<TypeKind::ROW>(const variant& v, GenericWriter& writer) {
  auto& writerTyped = writer.template castTo<DynamicRow>();
  const auto& elements = v.row();
  column_index_t i = 0;
  for (const auto& element : elements) {
    if (element.isNull()) {
      writerTyped.set_null_at(i);
    } else {
      VELOX_DYNAMIC_TYPE_DISPATCH(
          writeOne, element.kind(), element, writerTyped.get_writer_at(i));
    }
    i++;
  }
}

// static
VectorPtr ConstrainedVectorGenerator::generateFlat(
    const AbstractInputGeneratorPtr& customGenerator,
    vector_size_t size,
    memory::MemoryPool* pool) {
  VELOX_CHECK_NOT_NULL(customGenerator);

  VectorPtr result;
  const auto& type = customGenerator->type();
  BaseVector::ensureWritable(SelectivityVector(size), type, pool, result);
  VectorWriter<Any> writer;
  writer.init(*result);

  for (auto i = 0; i < size; ++i) {
    writer.setOffset(i);
    const auto variant = customGenerator->generate();
    if (variant.isNull()) {
      writer.commitNull();
    } else {
      VELOX_DYNAMIC_TYPE_DISPATCH(
          writeOne, type->kind(), variant, writer.current());
      writer.commit(true);
    }
  }
  return result;
}

} // namespace facebook::velox::fuzzer
