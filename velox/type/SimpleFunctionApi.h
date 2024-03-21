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

#include "velox/type/CppToType.h"

namespace facebook::velox {

template <typename UNDERLYING_TYPE>
struct Variadic {
  using underlying_type = UNDERLYING_TYPE;

  Variadic() = delete;
};

// A type that can be used in simple function to represent any type.
// Two Generics with the same type variables should bound to the same type.
template <size_t id>
struct TypeVariable {
  static size_t getId() {
    return id;
  }
};

using T1 = TypeVariable<1>;
using T2 = TypeVariable<2>;
using T3 = TypeVariable<3>;
using T4 = TypeVariable<4>;
using T5 = TypeVariable<5>;
using T6 = TypeVariable<6>;
using T7 = TypeVariable<7>;
using T8 = TypeVariable<8>;

template <size_t id>
struct IntegerVariable {
  static size_t getId() {
    return id;
  }

  static std::string name() {
    return fmt::format("i{}", id);
  }
};

using P1 = IntegerVariable<1>;
using P2 = IntegerVariable<2>;
using P3 = IntegerVariable<3>;
using P4 = IntegerVariable<4>;
using S1 = IntegerVariable<5>;
using S2 = IntegerVariable<6>;
using S3 = IntegerVariable<7>;
using S4 = IntegerVariable<8>;

template <typename P, typename S>
struct ShortDecimal {
 private:
  ShortDecimal() {}
};

template <typename P, typename S>
struct LongDecimal {
 private:
  LongDecimal() {}
};

struct AnyType {};

template <typename T = AnyType, bool comparable = false, bool orderable = false>
struct Generic {
  Generic() = delete;
  static_assert(!(orderable && !comparable), "Orderable implies comparable.");
};

using Any = Generic<>;

template <typename T>
using Comparable = Generic<T, true, false>;

// Orderable implies comparable.
template <typename T>
using Orderable = Generic<T, true, true>;

template <typename>
struct isVariadicType : public std::false_type {};

template <typename T>
struct isVariadicType<Variadic<T>> : public std::true_type {};

template <typename>
struct isGenericType : public std::false_type {};

template <typename T, bool comparable, bool orderable>
struct isGenericType<Generic<T, comparable, orderable>>
    : public std::true_type {};

template <typename>
struct isOpaqueType : public std::false_type {};

template <typename T>
struct isOpaqueType<std::shared_ptr<T>> : public std::true_type {};

template <typename KEY, typename VALUE>
struct Map {
  using key_type = KEY;
  using value_type = VALUE;

  static_assert(
      !isVariadicType<key_type>::value,
      "Map keys cannot be Variadic");
  static_assert(
      !isVariadicType<value_type>::value,
      "Map values cannot be Variadic");

 private:
  Map() {}
};

template <typename ELEMENT>
struct Array {
  using element_type = ELEMENT;

  static_assert(
      !isVariadicType<element_type>::value,
      "Array elements cannot be Variadic");

 private:
  Array() {}
};

template <typename ELEMENT>
using ArrayWriterT = Array<ELEMENT>;

template <typename... T>
struct Row {
  template <size_t idx>
  using type_at = typename std::tuple_element<idx, std::tuple<T...>>::type;

  static const size_t size_ = sizeof...(T);

  static_assert(
      std::conjunction<std::bool_constant<!isVariadicType<T>::value>...>::value,
      "Struct fields cannot be Variadic");

 private:
  Row() {}
};

struct DynamicRow {
 private:
  DynamicRow() {}
};

// T must be a struct with T::type being a built-in type and T::typeName
// type name to use in FunctionSignature.
template <typename T>
struct CustomType {
 private:
  CustomType() {}
};

template <typename T>
struct UnwrapCustomType {
  using type = T;
};

template <typename T>
struct UnwrapCustomType<CustomType<T>> {
  using type = typename T::type;
};

struct IntervalDayTime {
 private:
  IntervalDayTime() {}
};

struct IntervalYearMonth {
 private:
  IntervalYearMonth() {}
};

struct Date {
 private:
  Date() {}
};

struct Varbinary {
 private:
  Varbinary() {}
};

struct Varchar {
 private:
  Varchar() {}
};

template <typename T>
struct Constant {};

template <typename T>
struct UnwrapConstantType {
  using type = T;
};

template <typename T>
struct UnwrapConstantType<Constant<T>> {
  using type = T;
};

template <typename T>
struct isConstantType {
  static constexpr bool value = false;
};

template <typename T>
struct isConstantType<Constant<T>> {
  static constexpr bool value = true;
};

template <typename... TArgs>
struct ConstantChecker {
  static constexpr bool isConstant[sizeof...(TArgs)] = {
      isConstantType<TArgs>::value...};
};

/// CppToType templates for types introduced above.

template <>
struct CppToType<Varchar> : public CppToTypeBase<TypeKind::VARCHAR> {};

template <>
struct CppToType<Varbinary> : public CppToTypeBase<TypeKind::VARBINARY> {};

template <>
struct CppToType<Date> : public CppToTypeBase<TypeKind::INTEGER> {};

template <typename T>
struct CppToType<Generic<T>> : public CppToTypeBase<TypeKind::UNKNOWN> {};

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

template <typename T>
struct CppToType<CustomType<T>> : public CppToType<typename T::type> {
  static auto create() {
    return CppToType<typename T::type>::create();
  }
};

/// SimpleTypeTrait template.

template <typename P, typename S>
struct SimpleTypeTrait<ShortDecimal<P, S>>
    : public TypeTraits<TypeKind::BIGINT> {};

template <typename P, typename S>
struct SimpleTypeTrait<LongDecimal<P, S>>
    : public TypeTraits<TypeKind::HUGEINT> {};

template <>
struct SimpleTypeTrait<Varchar> : public TypeTraits<TypeKind::VARCHAR> {};

template <>
struct SimpleTypeTrait<Varbinary> : public TypeTraits<TypeKind::VARBINARY> {};

template <>
struct SimpleTypeTrait<Date> : public SimpleTypeTrait<int32_t> {
  static constexpr const char* name = "DATE";
};

template <>
struct SimpleTypeTrait<IntervalDayTime> : public SimpleTypeTrait<int64_t> {
  static constexpr const char* name = "INTERVAL DAY TO SECOND";
};

template <>
struct SimpleTypeTrait<IntervalYearMonth> : public SimpleTypeTrait<int32_t> {
  static constexpr const char* name = "INTERVAL YEAR TO MONTH";
};

template <typename T, bool comparable, bool orderable>
struct SimpleTypeTrait<Generic<T, comparable, orderable>> {
  static constexpr TypeKind typeKind = TypeKind::UNKNOWN;
  static constexpr bool isPrimitiveType = false;
  static constexpr bool isFixedWidth = false;
};

template <typename T>
struct SimpleTypeTrait<std::shared_ptr<T>>
    : public TypeTraits<TypeKind::OPAQUE> {};

template <typename KEY, typename VAL>
struct SimpleTypeTrait<Map<KEY, VAL>> : public TypeTraits<TypeKind::MAP> {};

template <typename ELEMENT>
struct SimpleTypeTrait<Array<ELEMENT>> : public TypeTraits<TypeKind::ARRAY> {};

template <typename... T>
struct SimpleTypeTrait<Row<T...>> : public TypeTraits<TypeKind::ROW> {};

template <>
struct SimpleTypeTrait<DynamicRow> : public TypeTraits<TypeKind::ROW> {};

// T is also a simple type that represent the physical type of the custom type.
template <typename T>
struct SimpleTypeTrait<CustomType<T>>
    : public SimpleTypeTrait<typename T::type> {
  using physical_t = SimpleTypeTrait<typename T::type>;
  static constexpr TypeKind typeKind = physical_t::typeKind;
  static constexpr bool isPrimitiveType = physical_t::isPrimitiveType;
  static constexpr bool isFixedWidth = physical_t::isFixedWidth;

  // This is different than the physical type name.
  static constexpr char* name = T::typeName;
};

/// MaterializeType template.

template <typename T>
struct MaterializeType {
  using null_free_t = T;
  using nullable_t = T;
  static constexpr bool requiresMaterialization = false;
};

template <typename V>
struct MaterializeType<Array<V>> {
  using null_free_t = std::vector<typename MaterializeType<V>::null_free_t>;
  using nullable_t =
      std::vector<std::optional<typename MaterializeType<V>::nullable_t>>;
  static constexpr bool requiresMaterialization = true;
};

template <typename K, typename V>
struct MaterializeType<Map<K, V>> {
  using key_t = typename MaterializeType<K>::null_free_t;

  using nullable_t = folly::
      F14FastMap<key_t, std::optional<typename MaterializeType<V>::nullable_t>>;

  using null_free_t =
      folly::F14FastMap<key_t, typename MaterializeType<V>::null_free_t>;
  static constexpr bool requiresMaterialization = true;
};

template <typename... T>
struct MaterializeType<Row<T...>> {
  using nullable_t =
      std::tuple<std::optional<typename MaterializeType<T>::nullable_t>...>;

  using null_free_t = std::tuple<typename MaterializeType<T>::null_free_t...>;
  static constexpr bool requiresMaterialization = true;
};

template <typename T>
struct MaterializeType<std::shared_ptr<T>> {
  using nullable_t = T;
  using null_free_t = T;
  static constexpr bool requiresMaterialization = false;
};

template <typename T>
struct MaterializeType<CustomType<T>> {
  using inner_materialize_t = MaterializeType<typename T::type>;
  using nullable_t = typename inner_materialize_t::nullable_t;
  using null_free_t = typename inner_materialize_t::null_free_t;
  static constexpr bool requiresMaterialization =
      inner_materialize_t::requiresMaterialization;
};

template <>
struct MaterializeType<Varchar> {
  using nullable_t = std::string;
  using null_free_t = std::string;
  static constexpr bool requiresMaterialization = false;
};

template <>
struct MaterializeType<Varbinary> {
  using nullable_t = std::string;
  using null_free_t = std::string;
  static constexpr bool requiresMaterialization = false;
};

} // namespace facebook::velox
