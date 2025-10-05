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

#include <folly/CPortability.h>
#include <folly/Hash.h>
#include <folly/Random.h>
#include <folly/Range.h>
#include <folly/container/F14Set.h>
#include <folly/dynamic.h>

#include <cstdint>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <typeindex>
#include <vector>

#include <velox/common/Enums.h>
#include "velox/common/base/BitUtil.h"
#include "velox/common/base/ClassName.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Macros.h"
#include "velox/common/serialization/Serializable.h"
#include "velox/type/HugeInt.h"
#include "velox/type/StringView.h"
#include "velox/type/Timestamp.h"
#include "velox/type/Tree.h"

namespace facebook::velox {

using int128_t = __int128_t;

using column_index_t = uint32_t;

constexpr column_index_t kConstantChannel =
    std::numeric_limits<column_index_t>::max();

/// Velox type system supports a small set of SQL-compatible composeable types:
/// BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, HUGEINT, REAL, DOUBLE, VARCHAR,
/// VARBINARY, TIMESTAMP, ARRAY, MAP, ROW
///
/// This file has multiple C++ type definitions for each of these logical types.
/// These logical definitions each serve slightly different purposes.
/// These type sets are:
/// - TypeKind
/// - Type (RowType, BigIntType, ect.)
/// - Templated Types (Row<T...>, Map<K, V>, ...)
///     C++ templated classes. Never instantiated, used to pass limited type
///     information into template parameters.

/// Simple enum with type category.
enum class TypeKind : int8_t {
  BOOLEAN = 0,
  TINYINT = 1,
  SMALLINT = 2,
  INTEGER = 3,
  BIGINT = 4,
  REAL = 5,
  DOUBLE = 6,
  VARCHAR = 7,
  VARBINARY = 8,
  TIMESTAMP = 9,
  HUGEINT = 10,
  // Enum values for ComplexTypes start after 30 to leave
  // some values space to accommodate adding new scalar/native
  // types above.
  ARRAY = 30,
  MAP = 31,
  ROW = 32,
  UNKNOWN = 33,
  FUNCTION = 34,
  OPAQUE = 35,
  INVALID = 36
};

VELOX_DECLARE_ENUM_NAME(TypeKind);

/// Deprecated.
inline TypeKind mapNameToTypeKind(const std::string& name) {
  return TypeKindName::toTypeKind(name);
}

[[deprecated("Use TypeKindName::tryToTypeKind")]]
inline std::optional<TypeKind> tryMapNameToTypeKind(const std::string& name) {
  return TypeKindName::tryToTypeKind(name);
}

/// Deprecated.
inline std::string mapTypeKindToName(const TypeKind& typeKind) {
  return std::string(TypeKindName::toName(typeKind));
}

template <TypeKind KIND>
class ScalarType;
class ShortDecimalType;
class LongDecimalType;
class ArrayType;
class MapType;
class RowType;
class FunctionType;
class OpaqueType;
class UnknownType;

struct UnknownValue {
  bool operator==(const UnknownValue& /* b */) const {
    return true;
  }

  auto operator<=>(const UnknownValue& /* b */) const {
    return std::strong_ordering::equal;
  }
};

template <typename T>
void toAppend(
    const ::facebook::velox::UnknownValue& /* value */,
    T* /* result */) {
  // TODO Implement
}

template <TypeKind KIND>
struct TypeTraits {};

template <>
struct TypeTraits<TypeKind::BOOLEAN> {
  using ImplType = ScalarType<TypeKind::BOOLEAN>;
  using NativeType = bool;
  using DeepCopiedType = NativeType;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::BOOLEAN;
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = true;
  static constexpr const char* name = "BOOLEAN";
};

template <>
struct TypeTraits<TypeKind::TINYINT> {
  using ImplType = ScalarType<TypeKind::TINYINT>;
  using NativeType = int8_t;
  using DeepCopiedType = NativeType;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::TINYINT;
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = true;
  static constexpr const char* name = "TINYINT";
};

template <>
struct TypeTraits<TypeKind::SMALLINT> {
  using ImplType = ScalarType<TypeKind::SMALLINT>;
  using NativeType = int16_t;
  using DeepCopiedType = NativeType;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::SMALLINT;
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = true;
  static constexpr const char* name = "SMALLINT";
};

template <>
struct TypeTraits<TypeKind::INTEGER> {
  using ImplType = ScalarType<TypeKind::INTEGER>;
  using NativeType = int32_t;
  using DeepCopiedType = NativeType;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::INTEGER;
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = true;
  static constexpr const char* name = "INTEGER";
};

template <>
struct TypeTraits<TypeKind::BIGINT> {
  using ImplType = ScalarType<TypeKind::BIGINT>;
  using NativeType = int64_t;
  using DeepCopiedType = NativeType;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::BIGINT;
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = true;
  static constexpr const char* name = "BIGINT";
};

template <>
struct TypeTraits<TypeKind::REAL> {
  using ImplType = ScalarType<TypeKind::REAL>;
  using NativeType = float;
  using DeepCopiedType = NativeType;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::REAL;
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = true;
  static constexpr const char* name = "REAL";
};

template <>
struct TypeTraits<TypeKind::DOUBLE> {
  using ImplType = ScalarType<TypeKind::DOUBLE>;
  using NativeType = double;
  using DeepCopiedType = NativeType;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::DOUBLE;
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = true;
  static constexpr const char* name = "DOUBLE";
};

template <>
struct TypeTraits<TypeKind::VARCHAR> {
  using ImplType = ScalarType<TypeKind::VARCHAR>;
  using NativeType = velox::StringView;
  using DeepCopiedType = std::string;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::VARCHAR;
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = false;
  static constexpr const char* name = "VARCHAR";
};

template <>
struct TypeTraits<TypeKind::TIMESTAMP> {
  using ImplType = ScalarType<TypeKind::TIMESTAMP>;
  using NativeType = Timestamp;
  using DeepCopiedType = Timestamp;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::TIMESTAMP;
  // isPrimitiveType in the type traits indicate whether it is a leaf type.
  // So only types which have other sub types, should be set to false.
  // Timestamp does not contain other types, so it is set to true.
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = true;
  static constexpr const char* name = "TIMESTAMP";
};

template <>
struct TypeTraits<TypeKind::HUGEINT> {
  using ImplType = ScalarType<TypeKind::HUGEINT>;
  using NativeType = int128_t;
  using DeepCopiedType = NativeType;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::HUGEINT;
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = true;
  static constexpr const char* name = "HUGEINT";
};

template <>
struct TypeTraits<TypeKind::VARBINARY> {
  using ImplType = ScalarType<TypeKind::VARBINARY>;
  using NativeType = velox::StringView;
  using DeepCopiedType = std::string;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::VARBINARY;
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = false;
  static constexpr const char* name = "VARBINARY";
};

template <>
struct TypeTraits<TypeKind::ARRAY> {
  using ImplType = ArrayType;
  using NativeType = void;
  using DeepCopiedType = void;
  static constexpr uint32_t minSubTypes = 1;
  static constexpr uint32_t maxSubTypes = 1;
  static constexpr TypeKind typeKind = TypeKind::ARRAY;
  static constexpr bool isPrimitiveType = false;
  static constexpr bool isFixedWidth = false;
  static constexpr const char* name = "ARRAY";
};

template <>
struct TypeTraits<TypeKind::MAP> {
  using ImplType = MapType;
  using NativeType = void;
  using DeepCopiedType = void;
  static constexpr uint32_t minSubTypes = 2;
  static constexpr uint32_t maxSubTypes = 2;
  static constexpr TypeKind typeKind = TypeKind::MAP;
  static constexpr bool isPrimitiveType = false;
  static constexpr bool isFixedWidth = false;
  static constexpr const char* name = "MAP";
};

template <>
struct TypeTraits<TypeKind::ROW> {
  using ImplType = RowType;
  using NativeType = void;
  using DeepCopiedType = void;
  static constexpr uint32_t minSubTypes = 1;
  static constexpr uint32_t maxSubTypes = std::numeric_limits<char16_t>::max();
  static constexpr TypeKind typeKind = TypeKind::ROW;
  static constexpr bool isPrimitiveType = false;
  static constexpr bool isFixedWidth = false;
  static constexpr const char* name = "ROW";
};

template <>
struct TypeTraits<TypeKind::UNKNOWN> {
  using ImplType = UnknownType;
  using NativeType = UnknownValue;
  using DeepCopiedType = UnknownValue;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::UNKNOWN;
  static constexpr bool isPrimitiveType = true;
  static constexpr bool isFixedWidth = true;
  static constexpr const char* name = "UNKNOWN";
};

template <>
struct TypeTraits<TypeKind::INVALID> {
  using ImplType = void;
  using NativeType = void;
  using DeepCopiedType = void;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::INVALID;
  static constexpr bool isPrimitiveType = false;
  static constexpr bool isFixedWidth = false;
  static constexpr const char* name = "INVALID";
};

template <>
struct TypeTraits<TypeKind::FUNCTION> {
  using ImplType = FunctionType;
  using NativeType = void;
  using DeepCopiedType = void;
  static constexpr uint32_t minSubTypes = 1;
  static constexpr uint32_t maxSubTypes = std::numeric_limits<char16_t>::max();
  static constexpr TypeKind typeKind = TypeKind::FUNCTION;
  static constexpr bool isPrimitiveType = false;
  static constexpr bool isFixedWidth = false;
  static constexpr const char* name = "FUNCTION";
};

template <>
struct TypeTraits<TypeKind::OPAQUE> {
  using ImplType = OpaqueType;
  using NativeType = std::shared_ptr<void>;
  using DeepCopiedType = std::shared_ptr<void>;
  static constexpr uint32_t minSubTypes = 0;
  static constexpr uint32_t maxSubTypes = 0;
  static constexpr TypeKind typeKind = TypeKind::OPAQUE;
  static constexpr bool isPrimitiveType = false;
  static constexpr bool isFixedWidth = false;
  static constexpr const char* name = "OPAQUE";
};

template <TypeKind KIND>
struct TypeFactory;

#define VELOX_FLUENT_CAST(NAME, KIND)                                     \
  const typename TypeTraits<TypeKind::KIND>::ImplType& as##NAME() const { \
    return this->as<TypeKind::KIND>();                                    \
  }                                                                       \
  bool is##NAME() const {                                                 \
    return this->kind() == TypeKind::KIND;                                \
  }

class Type;
using TypePtr = std::shared_ptr<const Type>;

/// Represents the parameters for a BigintEnumType.
/// Consists of the name of the enum and a map of string keys to bigint values.
struct LongEnumParameter {
  LongEnumParameter() = default;

  LongEnumParameter(
      std::string enumName,
      std::unordered_map<std::string, int64_t> enumValuesMap)
      : name(std::move(enumName)), valuesMap(std::move(enumValuesMap)) {}

  bool operator==(const LongEnumParameter& other) const {
    return name == other.name && valuesMap == other.valuesMap;
  }

  folly::dynamic serialize() const;

  struct Hash {
    size_t operator()(const LongEnumParameter& param) const;
  };

  std::string name;
  std::unordered_map<std::string, int64_t> valuesMap;
};

/// Represents the parameters for a VarcharEnumType.
/// Consists of the name of the enum and a map of string keys to string values.
struct VarcharEnumParameter {
  VarcharEnumParameter() = default;

  VarcharEnumParameter(
      std::string enumName,
      std::unordered_map<std::string, std::string> enumValuesMap)
      : name(std::move(enumName)), valuesMap(std::move(enumValuesMap)) {}

  bool operator==(const VarcharEnumParameter& other) const {
    return name == other.name && valuesMap == other.valuesMap;
  }

  folly::dynamic serialize() const;

  struct Hash {
    size_t operator()(const VarcharEnumParameter& param) const;
  };

  std::string name;
  std::unordered_map<std::string, std::string> valuesMap;
};

enum class TypeParameterKind {
  /// Type. For example, element type in the array, map, or row children type.
  kType,
  /// Integer. For example, precision in a decimal type.
  kLongLiteral,
  /// LongEnumLiteral. Used for BigintEnum types.
  kLongEnumLiteral,
  /// VarcharEnumLiteral. Used for VarcharEnum types.
  kVarcharEnumLiteral,
};

struct TypeParameter {
  const TypeParameterKind kind;

  /// Must be not not null when kind is kType. All other properties should be
  /// null or unset (other than rowFieldName).
  const TypePtr type;

  /// Must be set when kind is kLongLiteral. All other properties should be null
  /// or unset.
  const std::optional<int64_t> longLiteral;

  /// Must be set when kind is kLongEnumLiteral. All other properties should
  /// be null or unset.
  const std::optional<LongEnumParameter> longEnumLiteral;

  /// Must be set when kind is kVarcharEnumLiteral. All other properties should
  /// be null or unset.
  const std::optional<VarcharEnumParameter> varcharEnumLiteral;

  /// If this parameter is a child of another parent row type, it can optionally
  /// have a name, e.g, "id" for `row(id bigint)`. Only set when kind is kType
  const std::optional<std::string> rowFieldName;

  /// Creates kType parameter.
  explicit TypeParameter(
      TypePtr _type,
      std::optional<std::string> _rowFieldName = std::nullopt)
      : kind{TypeParameterKind::kType},
        type{std::move(_type)},
        longLiteral{std::nullopt},
        longEnumLiteral{std::nullopt},
        varcharEnumLiteral(std::nullopt),
        rowFieldName(std::move(_rowFieldName)) {}

  /// Creates kLongLiteral parameter.
  explicit TypeParameter(int64_t _longLiteral)
      : kind{TypeParameterKind::kLongLiteral},
        type{nullptr},
        longLiteral{_longLiteral},
        longEnumLiteral{std::nullopt},
        varcharEnumLiteral(std::nullopt),
        rowFieldName{std::nullopt} {}

  /// Creates kLongEnumLiteral parameter.
  explicit TypeParameter(LongEnumParameter _longEnumParameter)
      : kind{TypeParameterKind::kLongEnumLiteral},
        type{nullptr},
        longLiteral{std::nullopt},
        longEnumLiteral{std::move(_longEnumParameter)},
        varcharEnumLiteral(std::nullopt),
        rowFieldName{std::nullopt} {}

  /// Creates kVarcharEnumLiteral parameter.
  explicit TypeParameter(VarcharEnumParameter _varcharEnumParameter)
      : kind{TypeParameterKind::kVarcharEnumLiteral},
        type{nullptr},
        longLiteral{std::nullopt},
        longEnumLiteral{std::nullopt},
        varcharEnumLiteral{std::move(_varcharEnumParameter)},
        rowFieldName{std::nullopt} {}
};

/// Abstract class hierarchy. Instances of these classes carry full
/// information about types, including for example field names.
/// Can be instantiated by factory methods, like INTEGER()
/// or MAP(INTEGER(), BIGINT()).
/// Instances of these classes form a tree, and are immutable.
/// For example, MAP<INTEGER, ARRAY<BIGINT>> will form a tree like:
///
///             MapType
///           /         \
///   IntegerType    ArrayType
///                     |
///                   BigintType
class Type : public Tree<const TypePtr>, public velox::ISerializable {
 public:
  constexpr explicit Type(TypeKind kind, bool providesCustomComparison = false)
      : kind_{kind}, providesCustomComparison_{providesCustomComparison} {}

  TypeKind kind() const {
    return kind_;
  }

  ~Type() override = default;

  /// This convenience method makes pattern matching easier. Rather than having
  /// to know the implementation type up front, just use as<TypeKind::MAP> (for
  /// example) to dynamically cast to the appropriate type.
  template <TypeKind KIND>
  const typename TypeTraits<KIND>::ImplType& as() const {
    return dynamic_cast<const typename TypeTraits<KIND>::ImplType&>(*this);
  }

  virtual bool isPrimitiveType() const = 0;

  /// Returns true if equality relationship is defined for the values of this
  /// type, i.e. a == b is defined and returns true, false or null. For example,
  /// scalar types are usually comparable and complex types are comparable if
  /// their nested types are.
  virtual bool isComparable() const = 0;

  /// Returns true if less than relationship is defined for the values of this
  /// type, i.e. a <= b returns true or false. For example, scalar types are
  /// usually orderable, arrays and structs are orderable if their nested types
  /// are, while map types are not orderable.
  virtual bool isOrderable() const = 0;

  /// Returns true if values of this type implements custom comparison and hash
  /// functions. If this returns true the compare and hash functions in TypeBase
  /// should be used instead of native implementations, e.g. ==, <, >, etc.
  bool providesCustomComparison() const {
    return providesCustomComparison_;
  }

  /// Returns unique logical type name. It can be
  /// different from the physical type name returned by 'kindName()'.
  virtual const char* name() const = 0;

  /// Returns a possibly empty list of type parameters.
  virtual const std::vector<TypeParameter>& parameters() const = 0;

  /// Returns physical type name. Multiple logical types may share the same
  /// physical type backing and therefore return the same physical type name.
  /// The logical type name returned by 'name()' must be unique though.
  virtual const char* kindName() const = 0;

  virtual std::string toString() const = 0;

  /// Options to control the output of toSummaryString().
  struct TypeSummaryOptions {
    /// Maximum number of child types to include in the summary.
    size_type maxChildren{0};
  };

  /// Returns human-readable summary of the type. Useful when full output of
  /// toString() is too large.
  std::string toSummaryString(
      TypeSummaryOptions options = {.maxChildren = 0}) const;

  /// Types are weakly matched.
  /// Examples: Two RowTypes are equivalent if the children types are
  /// equivalent, but the children names could be different. Two OpaqueTypes are
  /// equivalent if the typeKind matches, but the typeIndex could be different.
  virtual bool equivalent(const Type& other) const = 0;

  /// For Complex types (Row, Array, Map, Opaque): types are strongly matched.
  /// For primitive types: same as equivalent.
  virtual bool operator==(const Type& other) const {
    return this->equals(other);
  }

  // todo(youknowjack): avoid expensive virtual function calls for these
  // simple functions
  virtual size_t cppSizeInBytes() const {
    // Must be a std::invalid_argument instead of VeloxException in order to
    // generate python ValueError in python bindings.
    throw std::invalid_argument{"Not a fixed width type: " + toString()};
  }

  virtual bool isFixedWidth() const = 0;

  static TypePtr create(const folly::dynamic& obj);

  static void registerSerDe();

  /// Recursive kind hashing (uses only TypeKind).
  virtual size_t hashKind() const;

  /// Recursive kind match (uses only TypeKind).
  bool kindEquals(const TypePtr& other) const;

  template <TypeKind KIND, typename... CHILDREN>
  static std::shared_ptr<const typename TypeTraits<KIND>::ImplType> create(
      CHILDREN... children) {
    return TypeFactory<KIND>::create(std::forward(children)...);
  }

  VELOX_FLUENT_CAST(Boolean, BOOLEAN)
  VELOX_FLUENT_CAST(Tinyint, TINYINT)
  VELOX_FLUENT_CAST(Smallint, SMALLINT)
  VELOX_FLUENT_CAST(Integer, INTEGER)
  VELOX_FLUENT_CAST(Bigint, BIGINT)
  VELOX_FLUENT_CAST(Hugeint, HUGEINT)
  VELOX_FLUENT_CAST(Real, REAL)
  VELOX_FLUENT_CAST(Double, DOUBLE)
  VELOX_FLUENT_CAST(Varchar, VARCHAR)
  VELOX_FLUENT_CAST(Varbinary, VARBINARY)
  VELOX_FLUENT_CAST(Timestamp, TIMESTAMP)
  VELOX_FLUENT_CAST(Array, ARRAY)
  VELOX_FLUENT_CAST(Map, MAP)
  VELOX_FLUENT_CAST(Row, ROW)
  VELOX_FLUENT_CAST(Opaque, OPAQUE)
  VELOX_FLUENT_CAST(UnKnown, UNKNOWN)
  VELOX_FLUENT_CAST(Function, FUNCTION)

  const ShortDecimalType& asShortDecimal() const;
  const LongDecimalType& asLongDecimal() const;
  bool isShortDecimal() const;
  bool isLongDecimal() const;
  bool isDecimal() const;
  bool isIntervalYearMonth() const;

  bool isIntervalDayTime() const;

  bool isTime() const;

  bool isDate() const;

  bool containsUnknown() const;

  template <typename T>
  std::string valueToString(T value) const;

  VELOX_DEFINE_CLASS_NAME(Type)

 protected:
  FOLLY_ALWAYS_INLINE bool hasSameTypeId(const Type& other) const {
    return typeid(*this) == typeid(other);
  }

  /// For Complex types (Row, Array, Map, Opaque): types are strongly matched.
  /// Examples: Two RowTypes are == if the children types and the children names
  /// are same. Two OpaqueTypes are == if the typeKind and the typeIndex are
  /// same.
  /// For primitive types: same as equivalent.
  virtual bool equals(const Type& other) const {
    VELOX_CHECK(this->isPrimitiveType());
    return this->equivalent(other);
  }

 private:
  const TypeKind kind_;
  const bool providesCustomComparison_;
};

#undef VELOX_FLUENT_CAST

template <TypeKind KIND, typename = void>
struct kindCanProvideCustomComparison : std::false_type {};

template <TypeKind KIND>
struct kindCanProvideCustomComparison<
    KIND,
    std::enable_if_t<
        TypeTraits<KIND>::isPrimitiveType && TypeTraits<KIND>::isFixedWidth>>
    : std::true_type {};

struct ProvideCustomComparison {
  explicit ProvideCustomComparison() = default;
};

template <TypeKind KIND>
class TypeBase : public Type {
 public:
  using NativeType = typename TypeTraits<KIND>::NativeType;

  constexpr explicit TypeBase() : Type{KIND, false} {}

  constexpr explicit TypeBase(ProvideCustomComparison tag) : Type{KIND, true} {
    static_assert(
        kindCanProvideCustomComparison<KIND>::value,
        "Custom comparisons are only supported for primitive types that are fixed width.");
  }

  bool isPrimitiveType() const override {
    return TypeTraits<KIND>::isPrimitiveType;
  }

  bool isFixedWidth() const override {
    return TypeTraits<KIND>::isFixedWidth;
  }

  bool isOrderable() const override {
    return false;
  }

  bool isComparable() const override {
    return false;
  }

  const char* kindName() const override {
    return TypeTraits<KIND>::name;
  }

  const char* name() const override {
    return TypeTraits<KIND>::name;
  }

  const std::vector<TypeParameter>& parameters() const override {
    static const std::vector<TypeParameter> kEmpty = {};
    return kEmpty;
  }
};

template <TypeKind KIND>
class CanProvideCustomComparisonType : public TypeBase<KIND> {
 public:
  using TypeBase<KIND>::TypeBase;

  virtual int32_t compare(
      const typename TypeBase<KIND>::NativeType& /*left*/,
      const typename TypeBase<KIND>::NativeType& /*right*/) const {
    VELOX_CHECK(
        !this->providesCustomComparison(),
        "Type {} is marked as providesCustomComparison but did not implement compare.");
    VELOX_FAIL("Type {} does not provide custom comparison", this->name());
  }

  virtual uint64_t hash(
      const typename TypeBase<KIND>::NativeType& /*value*/) const {
    VELOX_CHECK(
        !this->providesCustomComparison(),
        "Type {} is marked as providesCustomComparison but did not implement hash.");
    VELOX_FAIL("Type {} does not provide custom hash", this->name());
  }
};

template <TypeKind KIND>
class ScalarType : public CanProvideCustomComparisonType<KIND> {
 public:
  using CanProvideCustomComparisonType<KIND>::CanProvideCustomComparisonType;

  uint32_t size() const override {
    return 0;
  }

  const TypePtr& childAt(uint32_t) const override {
    VELOX_FAIL("scalar type has no children");
  }

  std::string toString() const override {
    return TypeTraits<KIND>::name;
  }

  bool isOrderable() const override {
    return true;
  }

  bool isComparable() const override {
    return true;
  }

  size_t cppSizeInBytes() const override {
    if (TypeTraits<KIND>::isFixedWidth) {
      return sizeof(typename TypeTraits<KIND>::NativeType);
    }
    // TODO: velox throws here for non fixed width types.
    return Type::cppSizeInBytes();
  }

  // TODO: This and all similar functions should be constexpr starting from
  // C++23. In such case similar places but with type parameters can be
  // constexpr too.
  static std::shared_ptr<const ScalarType<KIND>> create() {
    static constexpr ScalarType<KIND> kInstance;
    return {std::shared_ptr<const ScalarType<KIND>>{}, &kInstance};
  }

  bool equivalent(const Type& other) const override {
    return Type::hasSameTypeId(other);
  }

  // TODO: velox implementation is in cpp
  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = TypeTraits<KIND>::name;
    return obj;
  }
};

/// This class represents the fixed-point numbers.
/// The parameter "precision" represents the number of digits the
/// Decimal Type can support and "scale" represents the number of digits to
/// the right of the decimal point.
template <TypeKind KIND>
class DecimalType : public ScalarType<KIND> {
 public:
  static_assert(KIND == TypeKind::BIGINT || KIND == TypeKind::HUGEINT);
  static constexpr uint8_t kMaxPrecision = KIND == TypeKind::BIGINT ? 18 : 38;
  static constexpr uint8_t kMinPrecision = KIND == TypeKind::BIGINT ? 1 : 19;

  inline bool equivalent(const Type& other) const override {
    if (!Type::hasSameTypeId(other)) {
      return false;
    }
    const auto& otherDecimal = static_cast<const DecimalType<KIND>&>(other);
    return (
        otherDecimal.precision() == precision() &&
        otherDecimal.scale() == scale());
  }

  inline uint8_t precision() const {
    return parameters_[0].longLiteral.value();
  }

  inline uint8_t scale() const {
    return parameters_[1].longLiteral.value();
  }

  const char* name() const override {
    return "DECIMAL";
  }

  std::string toString() const override {
    return fmt::format("DECIMAL({}, {})", precision(), scale());
  }

  folly::dynamic serialize() const override {
    auto obj = ScalarType<KIND>::serialize();
    obj["type"] = name();
    obj["precision"] = precision();
    obj["scale"] = scale();
    return obj;
  }

  const std::vector<TypeParameter>& parameters() const override {
    return parameters_;
  }

 protected:
  DecimalType(const uint8_t precision, const uint8_t scale)
      : parameters_{TypeParameter(precision), TypeParameter(scale)} {
    VELOX_CHECK_LE(
        scale,
        precision,
        "Scale of decimal type must not exceed its precision");
    VELOX_CHECK_LE(
        precision,
        kMaxPrecision,
        "Precision of decimal type must not exceed {}",
        kMaxPrecision);
    VELOX_CHECK_GE(
        precision,
        kMinPrecision,
        "Precision of decimal type must be at least {}",
        kMinPrecision);
  }

 private:
  const std::vector<TypeParameter> parameters_;
};

class ShortDecimalType final : public DecimalType<TypeKind::BIGINT> {
 public:
  ShortDecimalType(int precision, int scale)
      : DecimalType<TypeKind::BIGINT>(precision, scale) {}
};

class LongDecimalType final : public DecimalType<TypeKind::HUGEINT> {
 public:
  // Ensure toString from the parent is not hidden.
  using DecimalType<TypeKind::HUGEINT>::toString;

  LongDecimalType(int precision, int scale)
      : DecimalType<TypeKind::HUGEINT>(precision, scale) {}

  static std::string toString(int128_t value, const Type& type);
};

TypePtr DECIMAL(uint8_t precision, uint8_t scale);

FOLLY_ALWAYS_INLINE const ShortDecimalType& Type::asShortDecimal() const {
  return dynamic_cast<const ShortDecimalType&>(*this);
}

FOLLY_ALWAYS_INLINE const LongDecimalType& Type::asLongDecimal() const {
  return dynamic_cast<const LongDecimalType&>(*this);
}

FOLLY_ALWAYS_INLINE bool Type::isShortDecimal() const {
  return dynamic_cast<const ShortDecimalType*>(this) != nullptr;
}

FOLLY_ALWAYS_INLINE bool Type::isLongDecimal() const {
  return dynamic_cast<const LongDecimalType*>(this) != nullptr;
}

FOLLY_ALWAYS_INLINE bool Type::isDecimal() const {
  return isShortDecimal() || isLongDecimal();
}

FOLLY_ALWAYS_INLINE bool isDecimalName(const std::string& name) {
  return (name == "DECIMAL");
}

std::pair<uint8_t, uint8_t> getDecimalPrecisionScale(const Type& type);

class UnknownType : public CanProvideCustomComparisonType<TypeKind::UNKNOWN> {
 public:
  using CanProvideCustomComparisonType<
      TypeKind::UNKNOWN>::CanProvideCustomComparisonType;

  uint32_t size() const override {
    return 0;
  }

  const TypePtr& childAt(uint32_t) const override {
    throw std::invalid_argument{"UnknownType type has no children"};
  }

  std::string toString() const override {
    return TypeTraits<TypeKind::UNKNOWN>::name;
  }

  size_t cppSizeInBytes() const override {
    return 0;
  }

  bool isOrderable() const override {
    return true;
  }

  bool isComparable() const override {
    return true;
  }

  bool equivalent(const Type& other) const override {
    return Type::hasSameTypeId(other);
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = TypeTraits<TypeKind::UNKNOWN>::name;
    return obj;
  }
};

class ArrayType : public TypeBase<TypeKind::ARRAY> {
 public:
  explicit ArrayType(TypePtr child);

  explicit ArrayType(
      std::vector<std::string>&& /*names*/,
      std::vector<TypePtr>&& types)
      : ArrayType(std::move(types[0])) {}

  const TypePtr& elementType() const {
    return child_;
  }

  uint32_t size() const override {
    return 1;
  }

  std::vector<TypePtr> children() const {
    return {child_};
  }

  std::vector<std::string> names() const {
    return {"element"};
  }

  bool isOrderable() const override {
    return child_->isOrderable();
  }

  bool isComparable() const override {
    return child_->isComparable();
  }

  const TypePtr& childAt(uint32_t idx) const override;

  const char* nameOf(uint32_t idx) const {
    VELOX_USER_CHECK_EQ(idx, 0, "Array type should have only one child");
    return "element";
  }

  std::string toString() const override;

  bool equivalent(const Type& other) const override;

  folly::dynamic serialize() const override;

  const std::vector<TypeParameter>& parameters() const override {
    return parameters_;
  }

 protected:
  bool equals(const Type& other) const override;

  const TypePtr child_;
  const std::vector<TypeParameter> parameters_;
};

using ArrayTypePtr = std::shared_ptr<const ArrayType>;

class MapType : public TypeBase<TypeKind::MAP> {
 public:
  MapType(TypePtr keyType, TypePtr valueType);

  explicit MapType(
      std::vector<std::string>&& /*names*/,
      std::vector<TypePtr>&& types)
      : MapType(std::move(types[0]), std::move(types[1])) {}

  const TypePtr& keyType() const {
    return keyType_;
  }

  const TypePtr& valueType() const {
    return valueType_;
  }

  uint32_t size() const override {
    return 2;
  }

  std::vector<TypePtr> children() const {
    return {keyType_, valueType_};
  }

  std::vector<std::string> names() const {
    return {"key", "value"};
  }

  bool isComparable() const override {
    return keyType_->isComparable() && valueType_->isComparable();
  }

  std::string toString() const override;

  const TypePtr& childAt(uint32_t idx) const override;

  const char* nameOf(uint32_t idx) const;

  bool equivalent(const Type& other) const override;

  folly::dynamic serialize() const override;

  const std::vector<TypeParameter>& parameters() const override {
    return parameters_;
  }

 protected:
  bool equals(const Type& other) const override;

 private:
  TypePtr keyType_;
  TypePtr valueType_;
  const std::vector<TypeParameter> parameters_;
};

using MapTypePtr = std::shared_ptr<const MapType>;

class RowType : public TypeBase<TypeKind::ROW> {
  // This Set<NameIndex> written only to decrease memory footprint.
  // In general it can be replaced with Map<string_view, size_t>
  struct NameIndex {
    explicit NameIndex(std::string_view name, uint32_t index)
        : data{name.data()},
          size{static_cast<uint32_t>(name.size())},
          index{index} {}

    const char* data = nullptr;
    uint32_t size = 0;

    bool operator==(const NameIndex& other) const {
      return size == other.size && std::memcmp(data, other.data, size) == 0;
    }

    uint32_t index = 0;
  };

  struct NameIndexHasher {
    size_t operator()(const NameIndex& nameIndex) const {
      folly::f14::DefaultHasher<std::string_view> hasher;
      return hasher(std::string_view{nameIndex.data, nameIndex.size});
    }
  };

  // TODO: Consider using absl::flat_hash_set instead.
  using NameToIndex = folly::F14ValueSet<NameIndex, NameIndexHasher>;

 public:
  /// @param names Child names. Case sensitive. Can be empty. May contain
  /// duplicates.
  /// @param types List of child types aligned with 'names'.
  RowType(std::vector<std::string>&& names, std::vector<TypePtr>&& types);

  ~RowType() override;

  uint32_t size() const final {
    return children_.size();
  }

  const TypePtr& childAt(uint32_t idx) const final {
    VELOX_CHECK_LT(idx, children_.size());
    return children_[idx];
  }

//  void updateChildAt(uint32_t idx, std::string& newName, TypePtr newType) {
//    VELOX_CHECK_LT(idx, children_.size());
//    names_[idx] = newName;
//    children_[idx] = newType;
//  }

  const std::vector<TypePtr>& children() const {
    return children_;
  }

  /// Returns true if all child types are orderable.
  bool isOrderable() const override;

  /// Returns true if all child types are comparable.
  bool isComparable() const override;

  /// Returns type of the first child with matching name. Throws if child with
  /// this name doesn't exist.
  const TypePtr& findChild(folly::StringPiece name) const;

  /// Returns true if child with specified name exists.
  bool containsChild(std::string_view name) const;

  /// Returns zero-based index of the first child with matching name. Throws if
  /// child with this name doesn't exist.
  uint32_t getChildIdx(std::string_view name) const;

  /// Returns an optional zero-based index of the first child with matching
  /// name. Return std::nullopt if child with this name doesn't exist.
  std::optional<uint32_t> getChildIdxIfExists(std::string_view name) const;

  /// Returns the name of the child at specified index.
  const std::string& nameOf(uint32_t idx) const {
    VELOX_CHECK_LT(idx, names_.size());
    return names_[idx];
  }

  /// Returns true if the 'other' type is the same except for child names.
  bool equivalent(const Type& other) const override;

  std::string toString() const override;

  /// Print child names and types separated by 'delimiter'.
  void printChildren(std::stringstream& ss, std::string_view delimiter = ",")
      const;

  /// Concatenates child names and types of 'this' with 'other'.
  /// {a, b, c}->unionWith({d, e, f}) => {a, b, c, d, e, f}.
  std::shared_ptr<const RowType> unionWith(
      const std::shared_ptr<const RowType>& other) const;

  folly::dynamic serialize() const override;

  const std::vector<std::string>& names() const {
    return names_;
  }

  const std::vector<TypeParameter>& parameters() const override {
    const auto* parameters = parameters_.load(std::memory_order_acquire);
    if (parameters) [[likely]] {
      return *parameters;
    }
    return *ensureParameters();
  }

  const NameToIndex& nameToIndex() const {
    const auto* nameToIndex = nameToIndex_.load(std::memory_order_acquire);
    if (nameToIndex) [[likely]] {
      return *nameToIndex;
    }
    return *ensureNameToIndex();
  }

  size_t hashKind() const override;

 protected:
  bool equals(const Type& other) const override;

 private:
  const std::vector<TypeParameter>* ensureParameters() const;
  const NameToIndex* ensureNameToIndex() const;

  const std::vector<std::string> names_;
  const std::vector<TypePtr> children_;
  mutable std::atomic<std::vector<TypeParameter>*> parameters_{nullptr};
  mutable std::atomic<NameToIndex*> nameToIndex_{nullptr};
  mutable std::atomic_bool hashKindComputed_{false};
  mutable std::atomic_size_t hashKind_;
};

using RowTypePtr = std::shared_ptr<const RowType>;

inline RowTypePtr asRowType(const TypePtr& type) {
  return std::dynamic_pointer_cast<const RowType>(type);
}

/// Represents a lambda function. The children are the argument types
/// followed by the return value type.
class FunctionType : public TypeBase<TypeKind::FUNCTION> {
 public:
  FunctionType(std::vector<TypePtr>&& argumentTypes, TypePtr returnType);

  uint32_t size() const override {
    return children_.size();
  }

  const TypePtr& childAt(uint32_t idx) const override {
    VELOX_CHECK_LT(idx, children_.size());
    return children_[idx];
  }

  const std::vector<TypePtr>& children() const {
    return children_;
  }

  bool isOrderable() const override {
    return false;
  }

  bool isComparable() const override {
    return false;
  }

  bool equivalent(const Type& other) const override;

  std::string toString() const override;

  folly::dynamic serialize() const override;

  const std::vector<TypeParameter>& parameters() const override {
    return parameters_;
  }

 protected:
  bool equals(const Type& other) const override;

 private:
  static std::vector<TypePtr> allChildren(
      std::vector<TypePtr>&& argumentTypes,
      TypePtr returnType) {
    auto children = std::move(argumentTypes);
    children.push_back(returnType);
    return children;
  }
  // Argument types from left to right followed by return value type.
  const std::vector<TypePtr> children_;
  const std::vector<TypeParameter> parameters_;
};

class OpaqueType : public TypeBase<TypeKind::OPAQUE> {
 public:
  template <typename T>
  using SerializeFunc = std::function<std::string(const std::shared_ptr<T>&)>;
  template <typename T>
  using DeserializeFunc = std::function<std::shared_ptr<T>(const std::string&)>;

  explicit OpaqueType(std::type_index typeIndex) : typeIndex_{typeIndex} {}

  uint32_t size() const override {
    return 0;
  }

  const TypePtr& childAt(uint32_t) const override {
    VELOX_FAIL("OpaqueType type has no children");
  }

  std::string toString() const override;

  bool equivalent(const Type& other) const override;

  std::type_index typeIndex() const {
    return typeIndex_;
  }

  folly::dynamic serialize() const override;

  /// In special cases specific OpaqueTypes might want to serialize additional
  /// metadata. In those cases we need to deserialize it back. Since
  /// OpaqueType::create<T>() returns canonical type for T without metadata,
  /// we allow to create new instance here or return nullptr if the same one
  /// can be used. Note that it's about deserialization of type itself,
  /// DeserializeFunc above is about deserializing instances of the type. It's
  /// implemented as a virtual member instead of a standalone registry just
  /// for convenience.
  virtual std::shared_ptr<const OpaqueType> deserializeExtra(
      const folly::dynamic& json) const;

  /// Function for converting std::shared_ptr<T> into a string. Always returns
  /// non-nullptr function or throws if not function has been registered.
  SerializeFunc<void> getSerializeFunc() const;
  DeserializeFunc<void> getDeserializeFunc() const;

  template <typename Class>
  FOLLY_NOINLINE static std::shared_ptr<const OpaqueType> create() {
    /// static vars in templates are dangerous across DSOs, but it's just a
    /// performance optimization. Comparison looks at type_index anyway.
    static const OpaqueType kInstance{std::type_index(typeid(Class))};
    return {std::shared_ptr<const OpaqueType>{}, &kInstance};
  }

  /// This function currently doesn't do synchronization neither with reads
  /// or writes, so it's caller's responsibility to not invoke it concurrently
  /// with other Velox code. Usually it'd be invoked at static initialization
  /// time. It can be changed in the future if it becomes a problem.
  template <typename T>
  FOLLY_NOINLINE static void registerSerialization(
      const std::string& persistentName,
      SerializeFunc<T> serialize = nullptr,
      DeserializeFunc<T> deserialize = nullptr) {
    SerializeFunc<void> serializeTypeErased;
    if (serialize) {
      serializeTypeErased =
          [serialize](const std::shared_ptr<void>& x) -> std::string {
        return serialize(std::static_pointer_cast<T>(x));
      };
    }
    DeserializeFunc<void> deserializeTypeErased;
    if (deserialize) {
      deserializeTypeErased =
          [deserialize](const std::string& s) -> std::shared_ptr<void> {
        return std::static_pointer_cast<void>(deserialize(s));
      };
    }
    registerSerializationTypeErased(
        OpaqueType::create<T>(),
        persistentName,
        serializeTypeErased,
        deserializeTypeErased);
  }

  static void clearSerializationRegistry();

 protected:
  bool equals(const Type& other) const override;

 private:
  const std::type_index typeIndex_;

  static void registerSerializationTypeErased(
      const std::shared_ptr<const OpaqueType>& type,
      const std::string& persistentName,
      SerializeFunc<void> serialize = nullptr,
      DeserializeFunc<void> deserialize = nullptr);
};

using IntegerType = ScalarType<TypeKind::INTEGER>;
using BooleanType = ScalarType<TypeKind::BOOLEAN>;
using TinyintType = ScalarType<TypeKind::TINYINT>;
using SmallintType = ScalarType<TypeKind::SMALLINT>;
using BigintType = ScalarType<TypeKind::BIGINT>;
using HugeintType = ScalarType<TypeKind::HUGEINT>;
using RealType = ScalarType<TypeKind::REAL>;
using DoubleType = ScalarType<TypeKind::DOUBLE>;
using TimestampType = ScalarType<TypeKind::TIMESTAMP>;
using VarcharType = ScalarType<TypeKind::VARCHAR>;
using VarbinaryType = ScalarType<TypeKind::VARBINARY>;

constexpr long kMillisInSecond = 1000;
constexpr long kMillisInMinute = 60 * kMillisInSecond;
constexpr long kMillisInHour = 60 * kMillisInMinute;
constexpr long kMillisInDay = 24 * kMillisInHour;

/// Time interval in milliseconds.
class IntervalDayTimeType final : public BigintType {
  IntervalDayTimeType() = default;

 public:
  static std::shared_ptr<const IntervalDayTimeType> get() {
    VELOX_CONSTEXPR_SINGLETON IntervalDayTimeType kInstance;
    return {std::shared_ptr<const IntervalDayTimeType>{}, &kInstance};
  }

  const char* name() const override {
    return "INTERVAL DAY TO SECOND";
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  std::string toString() const override {
    return name();
  }

  /// Returns the interval 'value' (milliseconds) formatted as DAYS
  /// HOURS:MINUTES:SECONDS.MILLIS. For example, 1 03:48:20.100.
  /// TODO Figure out how to make this API generic, i.e. available via Type.
  /// Perhaps, Type::valueToString(variant)?
  std::string valueToString(int64_t value) const;

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "IntervalDayTimeType";
    obj["type"] = name();
    return obj;
  }

  static TypePtr deserialize(const folly::dynamic& /*obj*/) {
    return IntervalDayTimeType::get();
  }
};

FOLLY_ALWAYS_INLINE std::shared_ptr<const IntervalDayTimeType>
INTERVAL_DAY_TIME() {
  return IntervalDayTimeType::get();
}

FOLLY_ALWAYS_INLINE bool Type::isIntervalDayTime() const {
  // Pointer comparison works since this type is a singleton.
  return (this == INTERVAL_DAY_TIME().get());
}

constexpr long kMonthInYear = 12;
/// Time interval in months.
class IntervalYearMonthType final : public IntegerType {
  IntervalYearMonthType() = default;

 public:
  static std::shared_ptr<const IntervalYearMonthType> get() {
    VELOX_CONSTEXPR_SINGLETON IntervalYearMonthType kInstance;
    return {std::shared_ptr<const IntervalYearMonthType>{}, &kInstance};
  }

  const char* name() const override {
    return "INTERVAL YEAR TO MONTH";
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  std::string toString() const override {
    return name();
  }

  /// Returns the interval 'value' (months) formatted as YEARS MONTHS.
  /// For example, 14 months (INTERVAL '1-2' YEAR TO MONTH) would be
  /// represented as 1-2; -14 months would be represents as -1-2.
  std::string valueToString(int32_t value) const;

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "IntervalYearMonthType";
    obj["type"] = name();
    return obj;
  }

  static TypePtr deserialize(const folly::dynamic& /*obj*/) {
    return IntervalYearMonthType::get();
  }
};

FOLLY_ALWAYS_INLINE std::shared_ptr<const IntervalYearMonthType>
INTERVAL_YEAR_MONTH() {
  return IntervalYearMonthType::get();
}

FOLLY_ALWAYS_INLINE bool Type::isIntervalYearMonth() const {
  // Pointer comparison works since this type is a singleton.
  return (this == INTERVAL_YEAR_MONTH().get());
}

/// Date is represented as the number of days since epoch start using int32_t.
class DateType final : public IntegerType {
  DateType() = default;

 public:
  static std::shared_ptr<const DateType> get() {
    VELOX_CONSTEXPR_SINGLETON DateType kInstance;
    return {std::shared_ptr<const DateType>{}, &kInstance};
  }

  const char* name() const override {
    return "DATE";
  }

  bool equivalent(const Type& other) const override {
    return this == &other;
  }

  std::string toString() const override {
    return name();
  }

  std::string toString(int32_t days) const;

  /// Returns a date, represented as days since epoch,
  /// as an ISO 8601-formatted string.
  static std::string toIso8601(int32_t days);

  int32_t toDays(folly::StringPiece in) const;

  int32_t toDays(const char* in, size_t len) const;

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "DateType";
    obj["type"] = name();
    return obj;
  }

  static TypePtr deserialize(const folly::dynamic& /*obj*/) {
    return DateType::get();
  }
};

FOLLY_ALWAYS_INLINE std::shared_ptr<const DateType> DATE() {
  return DateType::get();
}

FOLLY_ALWAYS_INLINE bool isDateName(const std::string& name) {
  return (name == DateType::get()->name());
}

FOLLY_ALWAYS_INLINE bool Type::isDate() const {
  // The pointers can be compared since DATE is a singleton.
  return this == DATE().get();
}

/// Represents TIME as a bigint (milliseconds since midnight).
class TimeType final : public BigintType {
  TimeType() = default;

 public:
  static std::shared_ptr<const TimeType> get() {
    VELOX_CONSTEXPR_SINGLETON TimeType kInstance;
    return {std::shared_ptr<const TimeType>{}, &kInstance};
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "TIME";
  }

  std::string toString() const override {
    return name();
  }

  folly::dynamic serialize() const override;

  static TypePtr deserialize(const folly::dynamic& /*obj*/) {
    return TimeType::get();
  }

  bool isOrderable() const override {
    return true;
  }

  bool isComparable() const override {
    return true;
  }
};

using TimeTypePtr = std::shared_ptr<const TimeType>;

FOLLY_ALWAYS_INLINE TimeTypePtr TIME() {
  return TimeType::get();
}

FOLLY_ALWAYS_INLINE bool Type::isTime() const {
  // Pointer comparison works since this type is a singleton.
  return (this == TIME().get());
}

struct Time {
 private:
  Time() {}
};

/// Used as T for SimpleVector subclasses that wrap another vector when
/// the wrapped vector is of a complex type. Applies to
/// DictionaryVector, SequenceVector and ConstantVector. This must have
/// a size different from any of the scalar data type sizes to enable
/// run time checking with 'elementSize_'.
struct ComplexType {
  TypePtr create() {
    VELOX_NYI();
  }

  operator folly::dynamic() const {
    return folly::dynamic("ComplexType");
  }
};

template <TypeKind KIND>
struct TypeFactory {
  static std::shared_ptr<const typename TypeTraits<KIND>::ImplType> create() {
    return TypeTraits<KIND>::ImplType::create();
  }
};

template <>
struct TypeFactory<TypeKind::UNKNOWN> {
  static std::shared_ptr<const UnknownType> create() {
    VELOX_CONSTEXPR_SINGLETON UnknownType kInstance;
    return {std::shared_ptr<const UnknownType>{}, &kInstance};
  }
};

template <>
struct TypeFactory<TypeKind::ARRAY> {
  static ArrayTypePtr create(TypePtr elementType) {
    return std::make_shared<const ArrayType>(std::move(elementType));
  }
};

template <>
struct TypeFactory<TypeKind::MAP> {
  static MapTypePtr create(TypePtr keyType, TypePtr valType) {
    return std::make_shared<const MapType>(
        std::move(keyType), std::move(valType));
  }
};

template <>
struct TypeFactory<TypeKind::ROW> {
  static RowTypePtr create(
      std::vector<std::string>&& names,
      std::vector<TypePtr>&& types) {
    return std::make_shared<const RowType>(std::move(names), std::move(types));
  }
};

/// Returns an array of 'elementType'.
///
/// Example: ARRAY(INTEGER()).
ArrayTypePtr ARRAY(TypePtr elementType);

/// Returns a map of 'keyType' and 'valueType'.
///
/// Example: MAP(INTEGER(), REAL()).
MapTypePtr MAP(TypePtr keyType, TypePtr valueType);

/// Returns a struct with specified field names and types. Number of 'names'
/// must match number of 'types'. Empty 'names' and 'types' are allowed.
///
/// Example: ROW({"a", "b", "c"}, {INTEGER(), BIGINT(), VARCHAR()}).
RowTypePtr ROW(std::vector<std::string> names, std::vector<TypePtr> types);

/// Returns a homogenous struct where all fields have the same type.
///
/// Example:
///
///   ROW({"a", "b", "c"}, REAL()) is a shortcut for
///     ROW({"a", "b", "c"}, {REAL(), REAL(), REAL()}).
RowTypePtr ROW(std::vector<std::string> names, const TypePtr& childType);

RowTypePtr ROW(
    std::initializer_list<std::string> names,
    const TypePtr& childType);

/// Creates a RowType from list of (name, type) pairs.
///
/// Example: ROW({{"a", INTEGER()}, {"b", BIGINT()}, {"c", VARCHAR()}}).
RowTypePtr ROW(
    std::initializer_list<std::pair<const std::string, TypePtr>>&& pairs);

/// Returns a struct with a single field.
///
/// Example: ROW("a", BIGINT()) is a shortcut for ROW({{"a", BIGINT()}}).
RowTypePtr ROW(std::string name, TypePtr type);

/// Returns anonymoous struct where field names are empty.
///
/// Examples:
///    ROW({INTEGER(), BIGINT(), VARCHAR()})
///    ROW({}) // Struct with no fields.
RowTypePtr ROW(std::vector<TypePtr>&& types);

std::shared_ptr<const FunctionType> FUNCTION(
    std::vector<TypePtr>&& argumentTypes,
    TypePtr returnType);

template <typename Class>
std::shared_ptr<const OpaqueType> OPAQUE() {
  return OpaqueType::create<Class>();
}

#define VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(TEMPLATE_FUNC, typeKind, ...)      \
  [&]() {                                                                     \
    switch (typeKind) {                                                       \
      case ::facebook::velox::TypeKind::BOOLEAN: {                            \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::BOOLEAN>(           \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::INTEGER: {                            \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::INTEGER>(           \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::TINYINT: {                            \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::TINYINT>(           \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::SMALLINT: {                           \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::SMALLINT>(          \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::BIGINT: {                             \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::BIGINT>(            \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::HUGEINT: {                            \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::HUGEINT>(           \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::REAL: {                               \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::REAL>(__VA_ARGS__); \
      }                                                                       \
      case ::facebook::velox::TypeKind::DOUBLE: {                             \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::DOUBLE>(            \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::VARCHAR: {                            \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::VARCHAR>(           \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::VARBINARY: {                          \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::VARBINARY>(         \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::TIMESTAMP: {                          \
        return TEMPLATE_FUNC<::facebook::velox::TypeKind::TIMESTAMP>(         \
            __VA_ARGS__);                                                     \
      }                                                                       \
      default:                                                                \
        VELOX_FAIL(                                                           \
            "not a scalar type! kind: {}", mapTypeKindToName(typeKind));      \
    }                                                                         \
  }()

#define VELOX_DYNAMIC_SCALAR_TEMPLATE_TYPE_DISPATCH(                     \
    TEMPLATE_FUNC, T, typeKind, ...)                                     \
  [&]() {                                                                \
    switch (typeKind) {                                                  \
      case ::facebook::velox::TypeKind::BOOLEAN: {                       \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::BOOLEAN>(   \
            __VA_ARGS__);                                                \
      }                                                                  \
      case ::facebook::velox::TypeKind::INTEGER: {                       \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::INTEGER>(   \
            __VA_ARGS__);                                                \
      }                                                                  \
      case ::facebook::velox::TypeKind::TINYINT: {                       \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::TINYINT>(   \
            __VA_ARGS__);                                                \
      }                                                                  \
      case ::facebook::velox::TypeKind::SMALLINT: {                      \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::SMALLINT>(  \
            __VA_ARGS__);                                                \
      }                                                                  \
      case ::facebook::velox::TypeKind::BIGINT: {                        \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::BIGINT>(    \
            __VA_ARGS__);                                                \
      }                                                                  \
      case ::facebook::velox::TypeKind::HUGEINT: {                       \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::HUGEINT>(   \
            __VA_ARGS__);                                                \
      }                                                                  \
      case ::facebook::velox::TypeKind::REAL: {                          \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::REAL>(      \
            __VA_ARGS__);                                                \
      }                                                                  \
      case ::facebook::velox::TypeKind::DOUBLE: {                        \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::DOUBLE>(    \
            __VA_ARGS__);                                                \
      }                                                                  \
      case ::facebook::velox::TypeKind::VARCHAR: {                       \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::VARCHAR>(   \
            __VA_ARGS__);                                                \
      }                                                                  \
      case ::facebook::velox::TypeKind::VARBINARY: {                     \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::VARBINARY>( \
            __VA_ARGS__);                                                \
      }                                                                  \
      case ::facebook::velox::TypeKind::TIMESTAMP: {                     \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::TIMESTAMP>( \
            __VA_ARGS__);                                                \
      }                                                                  \
      default:                                                           \
        VELOX_FAIL(                                                      \
            "not a scalar type! kind: {}", mapTypeKindToName(typeKind)); \
    }                                                                    \
  }()

#define VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(TEMPLATE_FUNC, T, typeKind, ...) \
  [&]() {                                                                     \
    switch (typeKind) {                                                       \
      case ::facebook::velox::TypeKind::BOOLEAN: {                            \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::BOOLEAN>(        \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::INTEGER: {                            \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::INTEGER>(        \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::TINYINT: {                            \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::TINYINT>(        \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::SMALLINT: {                           \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::SMALLINT>(       \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::BIGINT: {                             \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::BIGINT>(         \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::HUGEINT: {                            \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::HUGEINT>(        \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::REAL: {                               \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::REAL>(           \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::DOUBLE: {                             \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::DOUBLE>(         \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::VARCHAR: {                            \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::VARCHAR>(        \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::VARBINARY: {                          \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::VARBINARY>(      \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::TIMESTAMP: {                          \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::TIMESTAMP>(      \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::MAP: {                                \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::MAP>(            \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::ARRAY: {                              \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::ARRAY>(          \
            __VA_ARGS__);                                                     \
      }                                                                       \
      case ::facebook::velox::TypeKind::ROW: {                                \
        return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::ROW>(            \
            __VA_ARGS__);                                                     \
      }                                                                       \
      default:                                                                \
        VELOX_FAIL("not a known type kind: {}", mapTypeKindToName(typeKind)); \
    }                                                                         \
  }()

#define VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(                    \
    TEMPLATE_FUNC, T, typeKind, ...)                                 \
  [&]() {                                                            \
    if ((typeKind) == ::facebook::velox::TypeKind::UNKNOWN) {        \
      return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::UNKNOWN>( \
          __VA_ARGS__);                                              \
    } else if ((typeKind) == ::facebook::velox::TypeKind::OPAQUE) {  \
      return TEMPLATE_FUNC<T, ::facebook::velox::TypeKind::OPAQUE>(  \
          __VA_ARGS__);                                              \
    } else {                                                         \
      return VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH(                   \
          TEMPLATE_FUNC, T, typeKind, __VA_ARGS__);                  \
    }                                                                \
  }()

#define VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(TEMPLATE_FUNC, typeKind, ...)   \
  [&]() {                                                                      \
    if ((typeKind) == ::facebook::velox::TypeKind::UNKNOWN) {                  \
      return TEMPLATE_FUNC<::facebook::velox::TypeKind::UNKNOWN>(__VA_ARGS__); \
    } else if ((typeKind) == ::facebook::velox::TypeKind::OPAQUE) {            \
      return TEMPLATE_FUNC<::facebook::velox::TypeKind::OPAQUE>(__VA_ARGS__);  \
    } else {                                                                   \
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(                               \
          TEMPLATE_FUNC, typeKind, __VA_ARGS__);                               \
    }                                                                          \
  }()

#define VELOX_DYNAMIC_TYPE_DISPATCH_IMPL(PREFIX, SUFFIX, typeKind, ...)        \
  [&]() {                                                                      \
    switch (typeKind) {                                                        \
      case ::facebook::velox::TypeKind::BOOLEAN: {                             \
        return PREFIX<::facebook::velox::TypeKind::BOOLEAN> SUFFIX(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::facebook::velox::TypeKind::INTEGER: {                             \
        return PREFIX<::facebook::velox::TypeKind::INTEGER> SUFFIX(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::facebook::velox::TypeKind::TINYINT: {                             \
        return PREFIX<::facebook::velox::TypeKind::TINYINT> SUFFIX(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::facebook::velox::TypeKind::SMALLINT: {                            \
        return PREFIX<::facebook::velox::TypeKind::SMALLINT> SUFFIX(           \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::facebook::velox::TypeKind::BIGINT: {                              \
        return PREFIX<::facebook::velox::TypeKind::BIGINT> SUFFIX(             \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::facebook::velox::TypeKind::HUGEINT: {                             \
        return PREFIX<::facebook::velox::TypeKind::HUGEINT> SUFFIX(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::facebook::velox::TypeKind::REAL: {                                \
        return PREFIX<::facebook::velox::TypeKind::REAL> SUFFIX(__VA_ARGS__);  \
      }                                                                        \
      case ::facebook::velox::TypeKind::DOUBLE: {                              \
        return PREFIX<::facebook::velox::TypeKind::DOUBLE> SUFFIX(             \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::facebook::velox::TypeKind::VARCHAR: {                             \
        return PREFIX<::facebook::velox::TypeKind::VARCHAR> SUFFIX(            \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::facebook::velox::TypeKind::VARBINARY: {                           \
        return PREFIX<::facebook::velox::TypeKind::VARBINARY> SUFFIX(          \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::facebook::velox::TypeKind::TIMESTAMP: {                           \
        return PREFIX<::facebook::velox::TypeKind::TIMESTAMP> SUFFIX(          \
            __VA_ARGS__);                                                      \
      }                                                                        \
      case ::facebook::velox::TypeKind::ARRAY: {                               \
        return PREFIX<::facebook::velox::TypeKind::ARRAY> SUFFIX(__VA_ARGS__); \
      }                                                                        \
      case ::facebook::velox::TypeKind::MAP: {                                 \
        return PREFIX<::facebook::velox::TypeKind::MAP> SUFFIX(__VA_ARGS__);   \
      }                                                                        \
      case ::facebook::velox::TypeKind::ROW: {                                 \
        return PREFIX<::facebook::velox::TypeKind::ROW> SUFFIX(__VA_ARGS__);   \
      }                                                                        \
      default:                                                                 \
        VELOX_FAIL("not a known type kind: {}", mapTypeKindToName(typeKind));  \
    }                                                                          \
  }()

#define VELOX_DYNAMIC_TYPE_DISPATCH(TEMPLATE_FUNC, typeKind, ...) \
  VELOX_DYNAMIC_TYPE_DISPATCH_IMPL(TEMPLATE_FUNC, , typeKind, __VA_ARGS__)

#define VELOX_DYNAMIC_TYPE_DISPATCH_ALL(TEMPLATE_FUNC, typeKind, ...)          \
  [&]() {                                                                      \
    if ((typeKind) == ::facebook::velox::TypeKind::UNKNOWN) {                  \
      return TEMPLATE_FUNC<::facebook::velox::TypeKind::UNKNOWN>(__VA_ARGS__); \
    } else if ((typeKind) == ::facebook::velox::TypeKind::OPAQUE) {            \
      return TEMPLATE_FUNC<::facebook::velox::TypeKind::OPAQUE>(__VA_ARGS__);  \
    } else {                                                                   \
      return VELOX_DYNAMIC_TYPE_DISPATCH_IMPL(                                 \
          TEMPLATE_FUNC, , typeKind, __VA_ARGS__);                             \
    }                                                                          \
  }()

#define VELOX_DYNAMIC_TYPE_DISPATCH_METHOD( \
    CLASS_NAME, METHOD_NAME, typeKind, ...) \
  VELOX_DYNAMIC_TYPE_DISPATCH_IMPL(         \
      CLASS_NAME, ::METHOD_NAME, typeKind, __VA_ARGS__)

#define VELOX_DYNAMIC_TYPE_DISPATCH_METHOD_ALL(                             \
    CLASS_NAME, METHOD_NAME, typeKind, ...)                                 \
  [&]() {                                                                   \
    if ((typeKind) == ::facebook::velox::TypeKind::UNKNOWN) {               \
      return CLASS_NAME<::facebook::velox::TypeKind::UNKNOWN>::METHOD_NAME( \
          __VA_ARGS__);                                                     \
    } else if ((typeKind) == ::facebook::velox::TypeKind::OPAQUE) {         \
      return CLASS_NAME<::facebook::velox::TypeKind::OPAQUE>::METHOD_NAME(  \
          __VA_ARGS__);                                                     \
    } else {                                                                \
      return VELOX_DYNAMIC_TYPE_DISPATCH_IMPL(                              \
          CLASS_NAME, ::METHOD_NAME, typeKind, __VA_ARGS__);                \
    }                                                                       \
  }()

#define VELOX_SCALAR_ACCESSOR(KIND) \
  std::shared_ptr<const ScalarType<TypeKind::KIND>> KIND()

#define VELOX_STATIC_FIELD_DYNAMIC_DISPATCH(CLASS, FIELD, typeKind)           \
  [&]() {                                                                     \
    switch (typeKind) {                                                       \
      case ::facebook::velox::TypeKind::BOOLEAN: {                            \
        return CLASS<::facebook::velox::TypeKind::BOOLEAN>::FIELD;            \
      }                                                                       \
      case ::facebook::velox::TypeKind::INTEGER: {                            \
        return CLASS<::facebook::velox::TypeKind::INTEGER>::FIELD;            \
      }                                                                       \
      case ::facebook::velox::TypeKind::TINYINT: {                            \
        return CLASS<::facebook::velox::TypeKind::TINYINT>::FIELD;            \
      }                                                                       \
      case ::facebook::velox::TypeKind::SMALLINT: {                           \
        return CLASS<::facebook::velox::TypeKind::SMALLINT>::FIELD;           \
      }                                                                       \
      case ::facebook::velox::TypeKind::BIGINT: {                             \
        return CLASS<::facebook::velox::TypeKind::BIGINT>::FIELD;             \
      }                                                                       \
      case ::facebook::velox::TypeKind::REAL: {                               \
        return CLASS<::facebook::velox::TypeKind::REAL>::FIELD;               \
      }                                                                       \
      case ::facebook::velox::TypeKind::DOUBLE: {                             \
        return CLASS<::facebook::velox::TypeKind::DOUBLE>::FIELD;             \
      }                                                                       \
      case ::facebook::velox::TypeKind::VARCHAR: {                            \
        return CLASS<::facebook::velox::TypeKind::VARCHAR>::FIELD;            \
      }                                                                       \
      case ::facebook::velox::TypeKind::VARBINARY: {                          \
        return CLASS<::facebook::velox::TypeKind::VARBINARY>::FIELD;          \
      }                                                                       \
      case ::facebook::velox::TypeKind::TIMESTAMP: {                          \
        return CLASS<::facebook::velox::TypeKind::TIMESTAMP>::FIELD;          \
      }                                                                       \
      case ::facebook::velox::TypeKind::ARRAY: {                              \
        return CLASS<::facebook::velox::TypeKind::ARRAY>::FIELD;              \
      }                                                                       \
      case ::facebook::velox::TypeKind::MAP: {                                \
        return CLASS<::facebook::velox::TypeKind::MAP>::FIELD;                \
      }                                                                       \
      case ::facebook::velox::TypeKind::ROW: {                                \
        return CLASS<::facebook::velox::TypeKind::ROW>::FIELD;                \
      }                                                                       \
      default:                                                                \
        VELOX_FAIL("not a known type kind: {}", mapTypeKindToName(typeKind)); \
    }                                                                         \
  }()

#define VELOX_STATIC_FIELD_DYNAMIC_DISPATCH_ALL(CLASS, FIELD, typeKind)   \
  [&]() {                                                                 \
    if ((typeKind) == ::facebook::velox::TypeKind::UNKNOWN) {             \
      return CLASS<::facebook::velox::TypeKind::UNKNOWN>::FIELD;          \
    } else if ((typeKind) == ::facebook::velox::TypeKind::OPAQUE) {       \
      return CLASS<::facebook::velox::TypeKind::OPAQUE>::FIELD;           \
    } else if ((typeKind) == ::facebook::velox::TypeKind::HUGEINT) {      \
      return CLASS<::facebook::velox::TypeKind::HUGEINT>::FIELD;          \
    } else {                                                              \
      return VELOX_STATIC_FIELD_DYNAMIC_DISPATCH(CLASS, FIELD, typeKind); \
    }                                                                     \
  }()

// todo: union convenience creators

VELOX_SCALAR_ACCESSOR(INTEGER);
VELOX_SCALAR_ACCESSOR(BOOLEAN);
VELOX_SCALAR_ACCESSOR(TINYINT);
VELOX_SCALAR_ACCESSOR(SMALLINT);
VELOX_SCALAR_ACCESSOR(BIGINT);
VELOX_SCALAR_ACCESSOR(HUGEINT);
VELOX_SCALAR_ACCESSOR(REAL);
VELOX_SCALAR_ACCESSOR(DOUBLE);
VELOX_SCALAR_ACCESSOR(TIMESTAMP);
VELOX_SCALAR_ACCESSOR(VARCHAR);
VELOX_SCALAR_ACCESSOR(VARBINARY);

TypePtr UNKNOWN();

template <TypeKind KIND>
TypePtr createScalarType() {
  return ScalarType<KIND>::create();
}

TypePtr createScalarType(TypeKind kind);

TypePtr createType(TypeKind kind, std::vector<TypePtr>&& children);

/// Returns true built-in or custom type with specified name exists.
bool hasType(const std::string& name);

/// Returns built-in or custom type with specified name and child types.
/// Returns nullptr if type with specified name doesn't exist.
TypePtr getType(
    const std::string& name,
    const std::vector<TypeParameter>& parameters);

template <TypeKind KIND>
TypePtr createType(std::vector<TypePtr>&& children) {
  if (children.size() != 0) {
    throw std::invalid_argument{
        std::string(TypeTraits<KIND>::name) +
        " primitive type takes no children"};
  }
  static_assert(TypeTraits<KIND>::isPrimitiveType);
  return ScalarType<KIND>::create();
}

template <>
TypePtr createType<TypeKind::ROW>(std::vector<TypePtr>&& children);

template <>
TypePtr createType<TypeKind::ARRAY>(std::vector<TypePtr>&& children);

template <>
TypePtr createType<TypeKind::MAP>(std::vector<TypePtr>&& children);

template <>
TypePtr createType<TypeKind::OPAQUE>(std::vector<TypePtr>&& children);

#undef VELOX_SCALAR_ACCESSOR

template <typename T>
struct SimpleTypeTrait {};

template <>
struct SimpleTypeTrait<int128_t> : public TypeTraits<TypeKind::HUGEINT> {};

template <>
struct SimpleTypeTrait<int64_t> : public TypeTraits<TypeKind::BIGINT> {};

template <>
struct SimpleTypeTrait<int32_t> : public TypeTraits<TypeKind::INTEGER> {};

template <>
struct SimpleTypeTrait<int16_t> : public TypeTraits<TypeKind::SMALLINT> {};

template <>
struct SimpleTypeTrait<int8_t> : public TypeTraits<TypeKind::TINYINT> {};

template <>
struct SimpleTypeTrait<float> : public TypeTraits<TypeKind::REAL> {};

template <>
struct SimpleTypeTrait<double> : public TypeTraits<TypeKind::DOUBLE> {};

template <>
struct SimpleTypeTrait<bool> : public TypeTraits<TypeKind::BOOLEAN> {};

template <>
struct SimpleTypeTrait<Timestamp> : public TypeTraits<TypeKind::TIMESTAMP> {};

template <>
struct SimpleTypeTrait<UnknownValue> : public TypeTraits<TypeKind::UNKNOWN> {};

template <TypeKind KIND>
static inline int32_t sizeOfTypeKindHelper() {
  return sizeof(typename TypeTraits<KIND>::NativeType);
}

static inline int32_t sizeOfTypeKind(TypeKind kind) {
  if (kind == TypeKind::BOOLEAN) {
    throw std::invalid_argument("sizeOfTypeKind dos not apply to boolean");
  }
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(sizeOfTypeKindHelper, kind);
}

template <typename T, typename U>
static inline T to(const U& value) {
  return folly::to<T>(value);
}

template <>
inline Timestamp to(const std::string&) {
  return Timestamp(0, 0);
}

template <>
inline UnknownValue to(const std::string& /* value */) {
  return UnknownValue();
}

template <>
inline std::string to(const Timestamp& value) {
  return value.toString();
}

template <>
inline std::string to(const int128_t& value) {
  return std::to_string(value);
}

template <>
inline std::string to(const velox::StringView& value) {
  return std::string(value.data(), value.size());
}

template <>
inline std::string to(const ComplexType&) {
  return std::string("ComplexType");
}

template <>
inline velox::StringView to(const std::string& value) {
  return velox::StringView(value);
}

namespace exec {

/// Forward declaration.
class CastOperator;

using CastOperatorPtr = std::shared_ptr<const CastOperator>;
} // namespace exec

/// Forward declaration.
class Variant;
class AbstractInputGenerator;

namespace memory {
class MemoryPool;
} // namespace memory

using AbstractInputGeneratorPtr = std::shared_ptr<AbstractInputGenerator>;
using FuzzerGenerator = folly::detail::DefaultGenerator;

struct InputGeneratorConfig {
  // TODO: hook up the rest options in VectorFuzzer::Options.
  size_t seed_;
  double nullRatio_;
  memory::MemoryPool* pool_;
  TypePtr type_{nullptr}; // Added type to support
};

/// Associates custom types with their custom operators to be the payload in
/// the custom type registry.
class CustomTypeFactory {
 public:
  virtual ~CustomTypeFactory();

  /// Returns a shared pointer to the custom type.
  virtual TypePtr getType(
      const std::vector<TypeParameter>& parameters) const = 0;

  /// Returns a shared pointer to the custom cast operator. If a custom type
  /// should be treated as its underlying native type during type castings,
  /// return a nullptr. If a custom type does not support castings, throw an
  /// exception.
  virtual exec::CastOperatorPtr getCastOperator() const = 0;

  virtual AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& config) const = 0;
};

class AbstractInputGenerator {
 public:
  AbstractInputGenerator(
      size_t seed,
      const TypePtr& type,
      std::unique_ptr<AbstractInputGenerator>&& next,
      double nullRatio)
      : type_{type}, next_{std::move(next)}, nullRatio_{nullRatio} {
    rng_.seed(seed);
  }

  virtual ~AbstractInputGenerator();

  virtual Variant generate() = 0;

  TypePtr type() const {
    return type_;
  }

 protected:
  FuzzerGenerator rng_;

  TypePtr type_;

  std::unique_ptr<AbstractInputGenerator> next_;

  double nullRatio_;
};

/// Adds custom type to the registry if it doesn't exist already. No-op if
/// type with specified name already exists. Returns true if type was added,
/// false if type with the specified name already exists.
bool registerCustomType(
    const std::string& name,
    std::unique_ptr<const CustomTypeFactory> factories);

// See registerOpaqueType() for documentation on type index and opaque type
// alias.
std::unordered_map<std::string, std::type_index>& getTypeIndexByOpaqueAlias();

// Reverse of getTypeIndexByOpaqueAlias() when we need to look up the opaque
// alias by its type index.
std::unordered_map<std::type_index, std::string>& getOpaqueAliasByTypeIndex();

std::type_index getTypeIdForOpaqueTypeAlias(const std::string& name);

std::string getOpaqueAliasForTypeId(std::type_index typeIndex);

/// OpaqueType represents a type that is not part of the Velox type system.
/// To identify the underlying type we use std::type_index which is stable
/// within the same process. However, it is not necessarily stable across
/// processes.
///
/// So if we were to serialize an opaque type using its std::type_index, we
/// might not be able to deserialize it in another process. To solve this
/// problem, we require that both the serializing and deserializing processes
/// register the opaque type using registerOpaqueType() with the same alias.
template <typename Class>
bool registerOpaqueType(const std::string& alias) {
  auto typeIndex = std::type_index(typeid(Class));
  return getTypeIndexByOpaqueAlias().emplace(alias, typeIndex).second &&
      getOpaqueAliasByTypeIndex().emplace(typeIndex, alias).second;
}

/// Unregisters an opaque type. Returns true if the type was unregistered.
/// Currently, it is only used for testing to provide isolation between tests
/// when using the same alias.
template <typename Class>
bool unregisterOpaqueType(const std::string& alias) {
  auto typeIndex = std::type_index(typeid(Class));
  return getTypeIndexByOpaqueAlias().erase(alias) == 1 &&
      getOpaqueAliasByTypeIndex().erase(typeIndex) == 1;
}

/// Return true if a custom type with the specified name exists.
bool customTypeExists(const std::string& name);

/// Returns a set of all registered custom type names.
std::unordered_set<std::string> getCustomTypeNames();

/// Returns an instance of a custom type with the specified name and specified
/// child types.
TypePtr getCustomType(
    const std::string& name,
    const std::vector<TypeParameter>& parameters);

/// Removes custom type from the registry if exists. Returns true if type was
/// removed, false if type didn't exist.
bool unregisterCustomType(const std::string& name);

/// Returns the custom cast operator for the custom type with the specified
/// name. Returns nullptr if a type with the specified name does not exist or
/// does not have a dedicated custom cast operator.
exec::CastOperatorPtr getCustomTypeCastOperator(const std::string& name);

/// Returns the input generator for the custom type with the specified name.
AbstractInputGeneratorPtr getCustomTypeInputGenerator(
    const std::string& name,
    const InputGeneratorConfig& config);

// Allows us to transparently use folly::toAppend(), folly::join(), etc.
template <class TString>
void toAppend(
    const std::shared_ptr<const facebook::velox::Type>& type,
    TString* result) {
  result->append(type->toString());
}

/// Appends type's SQL string to 'out'. Uses DuckDB SQL.
void toTypeSql(const TypePtr& type, std::ostream& out);

/// Cache of serialized RowType instances. Useful to reduce the size of
/// serialized expressions and plans. Disabled by default. Not thread safe.
///
/// To enable, call 'serializedTypeCache().enable()'. This enables the cache for
/// the current thread. To disable, call 'serializedTypeCache().disable()'.
/// While enables, type serialization will use the cache and serialize the types
/// using IDs stored in the cache. The caller is responsible for saving
/// serialized types from the cache and using these to hidrate
/// 'deserializedTypeCache()' before deserializing the types.
class SerializedTypeCache {
 public:
  struct Options {
    // Caching applies to RowType's with at least this many fields.
    size_t minRowTypeSize = 10;
  };

  bool isEnabled() const {
    return enabled_;
  }

  const Options& options() const {
    return options_;
  }

  void enable(const Options& options = {.minRowTypeSize = 10}) {
    enabled_ = true;
    options_ = options;
  }

  void disable() {
    enabled_ = false;
  }

  size_t size() const {
    return cache_.size();
  }

  void clear() {
    cache_.clear();
  }

  /// Returns the ID of the type if it is in the cache. Returns std::nullopt if
  /// type is not found in the cache. Cache key is type instance pointer. Hence,
  /// equal but different instances are stored separately.
  std::optional<int32_t> get(const Type& type) const;

  /// Stores the type in the cache. Returns the ID of the type. Reports an error
  /// if type is already present in the cache. IDs are monotonically increasing.
  /// Serialized type may refer to types stored previously in the cache. When
  /// deserializing type cache, make sure to deserialize types in the order of
  /// cache IDs.
  int32_t put(const Type& type, folly::dynamic serialized);

  /// Serialized the types stored in the cache. Use
  /// DeserializedTypeCache::deserialize to deserialize.
  folly::dynamic serialize();

 private:
  bool enabled_{false};
  Options options_;
  folly::F14FastMap<const Type*, std::pair<int32_t, folly::dynamic>> cache_;
};

/// Thread local cache of serialized RowType instances. Used by
/// RowType::serialize.
SerializedTypeCache& serializedTypeCache();

/// Thread local cache of deserialized RowType instances. Used when
/// deserializing Type objects.
class DeserializedTypeCache {
 public:
  void deserialize(const folly::dynamic& obj);

  size_t size() const {
    return cache_.size();
  }

  const TypePtr& get(int32_t id) const;

  void clear() {
    cache_.clear();
  }

 private:
  folly::F14FastMap<int32_t, TypePtr> cache_;
};

DeserializedTypeCache& deserializedTypeCache();

template <typename T>
std::string Type::valueToString(T value) const {
  if constexpr (std::is_same_v<T, bool>) {
    return value ? "true" : "false";
  } else if constexpr (std::is_same_v<T, std::shared_ptr<void>>) {
    return "<opaque>";
  } else if constexpr (
      std::is_same_v<T, int64_t> || std::is_same_v<T, int128_t>) {
    if (isDecimal()) {
      return LongDecimalType::toString(value, *this);
    } else {
      return velox::to<std::string>(value);
    }
  } else if constexpr (std::is_same_v<T, int32_t>) {
    if (isDate()) {
      return DATE()->toString(value);
    } else {
      return velox::to<std::string>(value);
    }
  } else {
    return velox::to<std::string>(value);
  }
}

/// Return a string representation of a limited number of elements at the
/// start of the array or map.
///
/// @param size Total number of elements.
/// @param stringifyElement Function to call to append individual elements.
/// Will be called up to 'limit' times.
/// @param limit Maximum number of elements to include in the result.
std::string stringifyTruncatedElementList(
    size_t size,
    const std::function<void(std::stringstream&, size_t)>& stringifyElement,
    size_t limit = 5);

} // namespace facebook::velox

namespace folly {
template <>
struct hasher<::facebook::velox::UnknownValue> {
  size_t operator()(const ::facebook::velox::UnknownValue /* value */) const {
    return 0;
  }
};

// Helper functions to allow TypeKind and some common variations to be
// transparently used by folly::sformat.
//
// e.g: folly::sformat("type: {}", typeKind);
template <>
class FormatValue<facebook::velox::TypeKind> {
 public:
  explicit FormatValue(const facebook::velox::TypeKind& type) : type_(type) {}

  template <typename FormatCallback>
  void format(FormatArg& arg, FormatCallback& cb) const {
    return format_value::formatString(
        facebook::velox::mapTypeKindToName(type_), arg, cb);
  }

 private:
  facebook::velox::TypeKind type_;
};

/// Prints all types derived from `velox::Type`.
template <typename T>
class FormatValue<
    std::shared_ptr<T>,
    typename std::enable_if_t<std::is_base_of_v<facebook::velox::Type, T>>> {
 public:
  explicit FormatValue(const std::shared_ptr<const facebook::velox::Type>& type)
      : type_(type) {}

  template <typename FormatCallback>
  void format(FormatArg& arg, FormatCallback& cb) const {
    return format_value::formatString(type_->toString(), arg, cb);
  }

 private:
  std::shared_ptr<const facebook::velox::Type> type_;
};

} // namespace folly

template <>
struct fmt::formatter<facebook::velox::TypeKind> : fmt::formatter<string_view> {
  template <typename FormatContext>
  auto format(facebook::velox::TypeKind k, FormatContext& ctx) const {
    return formatter<string_view>::format(
        facebook::velox::mapTypeKindToName(k), ctx);
  }
};

template <typename T>
struct fmt::formatter<
    std::shared_ptr<T>,
    typename std::
        enable_if_t<std::is_base_of_v<facebook::velox::Type, T>, char>>
    : fmt::formatter<string_view> {
  template <typename FormatContext>
  auto format(const std::shared_ptr<T>& k, FormatContext& ctx) const {
    return formatter<string_view>::format(k->toString(), ctx);
  }
};
