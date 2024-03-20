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

namespace facebook::velox {

// TODO: move cppToType testing utilities.
template <typename T>
struct CppToType {};

template <TypeKind KIND>
struct CppToTypeBase : public TypeTraits<KIND> {
  static auto create() {
    return TypeFactory<KIND>::create();
  }
};

template <>
struct CppToType<int128_t> : public CppToTypeBase<TypeKind::HUGEINT> {};

template <>
struct CppToType<__uint128_t> : public CppToTypeBase<TypeKind::HUGEINT> {};

template <>
struct CppToType<int64_t> : public CppToTypeBase<TypeKind::BIGINT> {};

template <>
struct CppToType<uint64_t> : public CppToTypeBase<TypeKind::BIGINT> {};

template <>
struct CppToType<int32_t> : public CppToTypeBase<TypeKind::INTEGER> {};

template <>
struct CppToType<uint32_t> : public CppToTypeBase<TypeKind::INTEGER> {};

template <>
struct CppToType<int16_t> : public CppToTypeBase<TypeKind::SMALLINT> {};

template <>
struct CppToType<uint16_t> : public CppToTypeBase<TypeKind::SMALLINT> {};

template <>
struct CppToType<int8_t> : public CppToTypeBase<TypeKind::TINYINT> {};

template <>
struct CppToType<uint8_t> : public CppToTypeBase<TypeKind::TINYINT> {};

template <>
struct CppToType<bool> : public CppToTypeBase<TypeKind::BOOLEAN> {};

template <>
struct CppToType<Varchar> : public CppToTypeBase<TypeKind::VARCHAR> {};

template <>
struct CppToType<folly::StringPiece> : public CppToTypeBase<TypeKind::VARCHAR> {
};

template <>
struct CppToType<velox::StringView> : public CppToTypeBase<TypeKind::VARCHAR> {
};

template <>
struct CppToType<std::string_view> : public CppToTypeBase<TypeKind::VARCHAR> {};

template <>
struct CppToType<std::string> : public CppToTypeBase<TypeKind::VARCHAR> {};

template <>
struct CppToType<const char*> : public CppToTypeBase<TypeKind::VARCHAR> {};

template <>
struct CppToType<Varbinary> : public CppToTypeBase<TypeKind::VARBINARY> {};

template <>
struct CppToType<folly::ByteRange> : public CppToTypeBase<TypeKind::VARBINARY> {
};

template <>
struct CppToType<float> : public CppToTypeBase<TypeKind::REAL> {};

template <>
struct CppToType<double> : public CppToTypeBase<TypeKind::DOUBLE> {};

template <>
struct CppToType<Timestamp> : public CppToTypeBase<TypeKind::TIMESTAMP> {};

template <>
struct CppToType<Date> : public CppToTypeBase<TypeKind::INTEGER> {};

template <typename T>
struct CppToType<Generic<T>> : public CppToTypeBase<TypeKind::UNKNOWN> {};

// TODO: maybe do something smarter than just matching any shared_ptr, e.g. we
// can declare "registered" types explicitly
template <typename T>
struct CppToType<std::shared_ptr<T>> : public CppToTypeBase<TypeKind::OPAQUE> {
  // We override the type with the concrete specialization here!
  // using NativeType = std::shared_ptr<T>;
  static auto create() {
    return OpaqueType::create<T>();
  }
};

template <typename KEY, typename VAL>
struct CppToType<Map<KEY, VAL>> : public TypeTraits<TypeKind::MAP> {
  static auto create() {
    return MAP(CppToType<KEY>::create(), CppToType<VAL>::create());
  }
};

template <typename ELEMENT>
struct CppToType<Array<ELEMENT>> : public TypeTraits<TypeKind::ARRAY> {
  static auto create() {
    return ARRAY(CppToType<ELEMENT>::create());
  }
};

template <typename... T>
struct CppToType<Row<T...>> : public TypeTraits<TypeKind::ROW> {
  static auto create() {
    return ROW({CppToType<T>::create()...});
  }
};

template <>
struct CppToType<DynamicRow> : public TypeTraits<TypeKind::ROW> {
  static std::shared_ptr<const Type> create() {
    throw std::logic_error{"can't determine exact type for DynamicRow"};
  }
};

template <>
struct CppToType<UnknownValue> : public CppToTypeBase<TypeKind::UNKNOWN> {};

template <typename T>
struct CppToType<CustomType<T>> : public CppToType<typename T::type> {
  static auto create() {
    return CppToType<typename T::type>::create();
  }
};

// todo: remaining cpp2type

} // namespace facebook::velox
