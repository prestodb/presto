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
struct KindToFlatVector<TypeKind::DATE> {
  using type = FlatVector<Date>;
  using WrapperType = Date;
  using HashRowType = Date;
};

template <>
struct KindToFlatVector<TypeKind::INTERVAL_DAY_TIME> {
  using type = FlatVector<IntervalDayTime>;
  using WrapperType = IntervalDayTime;
  using HashRowType = IntervalDayTime;
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
  using type = typename KindToFlatVector<CppToType<T>::typeKind>::type;
};

} // namespace velox
} // namespace facebook
