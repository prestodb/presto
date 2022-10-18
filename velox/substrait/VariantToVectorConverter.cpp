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

#include "velox/substrait/VariantToVectorConverter.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::substrait {

namespace {
template <TypeKind KIND>
VectorPtr setVectorFromVariantsByKind(
    const std::vector<velox::variant>& values,
    memory::MemoryPool* pool) {
  using T = typename TypeTraits<KIND>::NativeType;

  auto flatVector = std::dynamic_pointer_cast<FlatVector<T>>(
      BaseVector::create(CppToType<T>::create(), values.size(), pool));

  for (vector_size_t i = 0; i < values.size(); i++) {
    if (values[i].isNull()) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, values[i].value<T>());
    }
  }
  return flatVector;
}

template <>
VectorPtr setVectorFromVariantsByKind<TypeKind::VARBINARY>(
    const std::vector<velox::variant>& /* values */,
    memory::MemoryPool* /* pool */) {
  throw std::invalid_argument("Return of VARBINARY data is not supported");
}

template <>
VectorPtr setVectorFromVariantsByKind<TypeKind::VARCHAR>(
    const std::vector<velox::variant>& values,
    memory::MemoryPool* pool) {
  auto flatVector = std::dynamic_pointer_cast<FlatVector<StringView>>(
      BaseVector::create(VARCHAR(), values.size(), pool));

  for (vector_size_t i = 0; i < values.size(); i++) {
    if (values[i].isNull()) {
      flatVector->setNull(i, true);
    } else {
      flatVector->set(i, StringView(values[i].value<Varchar>()));
    }
  }
  return flatVector;
}
} // namespace

VectorPtr setVectorFromVariants(
    const TypePtr& type,
    const std::vector<velox::variant>& values,
    memory::MemoryPool* pool) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      setVectorFromVariantsByKind, type->kind(), values, pool);
}
} // namespace facebook::velox::substrait
