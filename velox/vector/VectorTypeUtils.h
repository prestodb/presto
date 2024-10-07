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

#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook {
namespace velox {

// Maps TypeKind to the corresponding writable vector.
template <TypeKind K>
struct KindToFlatVector {
  using type = FlatVector<typename TypeTraits<K>::NativeType>;
  using WrapperType = typename TypeTraits<K>::NativeType;
  using HashRowType = typename TypeTraits<K>::NativeType;
};

template <>
struct KindToFlatVector<TypeKind::TIMESTAMP> {
  using type = FlatVector<Timestamp>;
  using WrapperType = Timestamp;
  using HashRowType = Timestamp;
};

template <>
struct KindToFlatVector<TypeKind::MAP> {
  using type = MapVector;
  using WrapperType = ComplexType;
  using HashRowType = StringView;
};

template <>
struct KindToFlatVector<TypeKind::ARRAY> {
  using type = ArrayVector;
  using WrapperType = ComplexType;
  using HashRowType = StringView;
};

template <>
struct KindToFlatVector<TypeKind::ROW> {
  using type = RowVector;
  using WrapperType = ComplexType;
  using HashRowType = StringView;
};

template <>
struct KindToFlatVector<TypeKind::VARCHAR> {
  using type = FlatVector<StringView>;
  using WrapperType = StringView;
  using HashRowType = StringView;
};

template <>
struct KindToFlatVector<TypeKind::VARBINARY> {
  using type = FlatVector<StringView>;
  using WrapperType = StringView;
  using HashRowType = StringView;
};

template <>
struct KindToFlatVector<TypeKind::OPAQUE> {
  using type = FlatVector<std::shared_ptr<void>>;
  using WrapperType = std::shared_ptr<void>;
  using HashRowType = void;
};

template <typename T>
struct TypeToFlatVector {
  using type = typename KindToFlatVector<SimpleTypeTrait<T>::typeKind>::type;
};

// Generic's, by design, do not have any compile time type information, so it is
// impossible to determine what sort of Vector would hold values for this type.
// To work around this, we just return BaseVector, since any Vector class can be
// safely casted to BaseVector, and it is consistent with classes specialized
// for the Generic type, like the VectorWriter.
template <typename T, bool comparable, bool orderable>
struct TypeToFlatVector<Generic<T, comparable, orderable>> {
  using type = BaseVector;
};
} // namespace velox
} // namespace facebook
