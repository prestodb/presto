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

#include <map>

#include <fmt/format.h>

#include "folly/dynamic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/VeloxException.h"
#include "velox/type/Conversions.h"
#include "velox/type/Type.h"

namespace facebook::velox {

// Constant used in comparison of REAL and DOUBLE values.
constexpr double kEpsilon{0.00001};

// note: while this is not intended for use in real critical code paths,
//       it's probably worthwhile to make it not completely suck
// todo(youknowjack): make this class not completely suck

struct VariantConverter;

class variant;

template <TypeKind KIND>
struct VariantEquality;

template <>
struct VariantEquality<TypeKind::TIMESTAMP>;

template <>
struct VariantEquality<TypeKind::DATE>;

template <>
struct VariantEquality<TypeKind::INTERVAL_DAY_TIME>;

template <>
struct VariantEquality<TypeKind::ARRAY>;

template <>
struct VariantEquality<TypeKind::ROW>;

template <>
struct VariantEquality<TypeKind::MAP>;

bool dispatchDynamicVariantEquality(
    const variant& a,
    const variant& b,
    const bool& enableNullEqualsNull);

namespace detail {
template <TypeKind KIND>
using scalar_stored_type = typename TypeTraits<KIND>::DeepCopiedType;

template <TypeKind KIND, typename = void>
struct VariantTypeTraits {};

template <TypeKind KIND>
struct VariantTypeTraits<
    KIND,
    std::enable_if_t<
        TypeTraits<KIND>::isPrimitiveType && KIND != TypeKind::VARCHAR &&
            KIND != TypeKind::VARBINARY,
        void>> {
  using native_type = typename TypeTraits<KIND>::NativeType;
  using stored_type = scalar_stored_type<KIND>;
};

template <TypeKind KIND>
struct VariantTypeTraits<
    KIND,
    std::enable_if_t<
        KIND == TypeKind::VARCHAR || KIND == TypeKind::VARBINARY,
        void>> {
  using native_type = folly::StringPiece;
  using stored_type = scalar_stored_type<KIND>;
};

template <>
struct VariantTypeTraits<TypeKind::ROW> {
  using stored_type = std::vector<variant>;
};

template <>
struct VariantTypeTraits<TypeKind::MAP> {
  using stored_type = std::map<variant, variant>;
};

template <>
struct VariantTypeTraits<TypeKind::ARRAY> {
  using stored_type = std::vector<variant>;
};

struct OpaqueCapsule {
  std::shared_ptr<const OpaqueType> type;
  std::shared_ptr<void> obj;

  bool operator<(const OpaqueCapsule& other) const {
    if (type->typeIndex() == other.type->typeIndex()) {
      return obj < other.obj;
    }
    return type->typeIndex() < other.type->typeIndex();
  }

  bool operator==(const OpaqueCapsule& other) const {
    return type->typeIndex() == other.type->typeIndex() && obj == other.obj;
  }
};

template <>
struct VariantTypeTraits<TypeKind::OPAQUE> {
  using stored_type = OpaqueCapsule;
};
} // namespace detail

#define VELOX_VARIANT_SCALAR_MEMBERS(KIND)                            \
  /* implicit */ variant(                                             \
      const typename detail::VariantTypeTraits<KIND>::native_type& v) \
      : kind_{KIND},                                                  \
        ptr_{new detail::VariantTypeTraits<KIND>::stored_type{v}} {}  \
  /* implicit */ variant(                                             \
      typename detail::VariantTypeTraits<KIND>::native_type&& v)      \
      : kind_{KIND},                                                  \
        ptr_{new detail::VariantTypeTraits<KIND>::stored_type{v}} {}

class variant {
 private:
  variant(TypeKind kind, void* ptr) : kind_{kind}, ptr_{ptr} {}

  template <TypeKind KIND>
  bool lessThan(const variant& a, const variant& b) const {
    if (a.isNull() && !b.isNull()) {
      return true;
    }
    if (a.isNull() || b.isNull()) {
      return false;
    }
    return a.value<KIND>() < b.value<KIND>();
  }

  template <TypeKind KIND>
  bool equals(const variant& a, const variant& b) const {
    if (a.isNull() || b.isNull()) {
      return false;
    }
    // todo(youknowjack): centralize equality semantics
    return a.value<KIND>() == b.value<KIND>();
  }

  template <TypeKind KIND>
  void typedDestroy() {
    delete static_cast<
        const typename detail::VariantTypeTraits<KIND>::stored_type*>(ptr_);
    ptr_ = nullptr;
  }

  template <TypeKind KIND>
  void typedCopy(const void* other) {
    using stored_type = typename detail::VariantTypeTraits<KIND>::stored_type;
    ptr_ = new stored_type{*static_cast<const stored_type*>(other)};
  }

  void dynamicCopy(const void* p, const TypeKind kind) {
    VELOX_DYNAMIC_TYPE_DISPATCH_ALL(typedCopy, kind, p);
  }

  void dynamicFree() {
    VELOX_DYNAMIC_TYPE_DISPATCH_ALL(typedDestroy, kind_);
  }

  template <TypeKind K>
  static const std::shared_ptr<const Type> kind2type() {
    return TypeFactory<K>::create();
  }

  [[noreturn]] void throwCheckIsKindError(TypeKind kind) const;

  [[noreturn]] void throwCheckNotNullError() const;

 public:
  struct Hasher {
    size_t operator()(variant const& input) const noexcept {
      return input.hash();
    }
  };

  struct NullEqualsNullsComparator {
    bool operator()(const variant& a, const variant& b) const {
      return a.equalsWithNullEqualsNull(b);
    }
  };

  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::BOOLEAN)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::TINYINT)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::SMALLINT)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::INTEGER)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::BIGINT)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::SHORT_DECIMAL)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::LONG_DECIMAL)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::REAL)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::DOUBLE)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::VARCHAR)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::DATE)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::INTERVAL_DAY_TIME)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::TIMESTAMP)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::UNKNOWN)
  // On 64-bit platforms `int64_t` is declared as `long int`, not `long long
  // int`, thus adding an extra overload to make literals like 1LL resolve
  // correctly. Note that one has to use template T because otherwise SFINAE
  // doesn't work, but in this case T = long long
  template <
      typename T = long long,
      std::enable_if_t<
          std::is_same<T, long long>::value &&
              !std::is_same<long long, int64_t>::value,
          bool> = true>
  /* implicit */ variant(const T& v) : variant(static_cast<int64_t>(v)) {}

  // VARBINARY conflicts with VARCHAR, so we don't gen these methods
  // VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::VARBINARY);

  static variant row(const std::vector<variant>& inputs) {
    return {
        TypeKind::ROW,
        new
        typename detail::VariantTypeTraits<TypeKind::ROW>::stored_type{inputs}};
  }

  static variant row(std::vector<variant>&& inputs) {
    return {
        TypeKind::ROW,
        new typename detail::VariantTypeTraits<TypeKind::ROW>::stored_type{
            std::move(inputs)}};
  }

  static variant map(const std::map<variant, variant>& inputs) {
    return {
        TypeKind::MAP,
        new
        typename detail::VariantTypeTraits<TypeKind::MAP>::stored_type{inputs}};
  }

  static variant map(std::map<variant, variant>&& inputs) {
    return {
        TypeKind::MAP,
        new typename detail::VariantTypeTraits<TypeKind::MAP>::stored_type{
            std::move(inputs)}};
  }

  static variant timestamp(const Timestamp& input) {
    return {
        TypeKind::TIMESTAMP,
        new
        typename detail::VariantTypeTraits<TypeKind::TIMESTAMP>::stored_type{
            input}};
  }

  static variant date(const Date& input) {
    return {
        TypeKind::DATE,
        new
        typename detail::VariantTypeTraits<TypeKind::DATE>::stored_type{input}};
  }

  static variant intervalDayTime(const IntervalDayTime& input) {
    return {
        TypeKind::INTERVAL_DAY_TIME,
        new typename detail::VariantTypeTraits<
            TypeKind::INTERVAL_DAY_TIME>::stored_type{input}};
  }

  template <class T>
  static variant opaque(const std::shared_ptr<T>& input) {
    VELOX_CHECK(input.get(), "Can't create a variant of nullptr opaque type");
    return {
        TypeKind::OPAQUE,
        new detail::OpaqueCapsule{OpaqueType::create<T>(), input}};
  }

  static variant opaque(
      const std::shared_ptr<void>& input,
      const std::shared_ptr<const OpaqueType>& type) {
    VELOX_CHECK(input.get(), "Can't create a variant of nullptr opaque type");
    return {TypeKind::OPAQUE, new detail::OpaqueCapsule{type, input}};
  }

  static variant array(const std::vector<variant>& inputs) {
    return {
        TypeKind::ARRAY,
        new typename detail::VariantTypeTraits<TypeKind::ARRAY>::stored_type{
            inputs}};
  }

  static variant array(std::vector<variant>&& inputs) {
    return {
        TypeKind::ARRAY,
        new typename detail::VariantTypeTraits<TypeKind::ARRAY>::stored_type{
            std::move(inputs)}};
  }

  variant() : kind_{TypeKind::INVALID}, ptr_{nullptr} {}

  variant(TypeKind kind) : kind_{kind}, ptr_{nullptr} {}

  variant(const variant& other) : kind_{other.kind_}, ptr_{nullptr} {
    auto op = other.ptr_;
    if (op != nullptr) {
      dynamicCopy(other.ptr_, other.kind_);
    }
  }

  variant(const char* str)
      : kind_{TypeKind::VARCHAR}, ptr_{new std::string{str}} {}

  template <TypeKind KIND>
  static variant create(
      typename detail::VariantTypeTraits<KIND>::stored_type&& v) {
    return variant{
        KIND,
        new
        typename detail::VariantTypeTraits<KIND>::stored_type{std::move(v)}};
  }

  template <TypeKind KIND>
  static variant create(
      const typename detail::VariantTypeTraits<KIND>::stored_type& v) {
    return variant{
        KIND, new typename detail::VariantTypeTraits<KIND>::stored_type{v}};
  }

  template <typename T>
  static variant create(const typename detail::VariantTypeTraits<
                        CppToType<T>::typeKind>::stored_type& v) {
    return create<CppToType<T>::typeKind>(v);
  }

  static variant null(TypeKind kind) {
    return variant{kind};
  }

  static variant binary(std::string&& val) {
    return variant{TypeKind::VARBINARY, new std::string{std::move(val)}};
  }

  static variant binary(const std::string& val) {
    return variant{TypeKind::VARBINARY, new std::string{val}};
  }

  variant& operator=(const variant& other) {
    if (ptr_ != nullptr) {
      dynamicFree();
    }
    kind_ = other.kind_;
    if (other.ptr_ != nullptr) {
      dynamicCopy(other.ptr_, other.kind_);
    }
    return *this;
  }

  variant& operator=(variant&& other) {
    if (ptr_ != nullptr) {
      dynamicFree();
    }
    kind_ = other.kind_;
    if (other.ptr_ != nullptr) {
      ptr_ = other.ptr_;
      other.ptr_ = nullptr;
    }
    return *this;
  }

  bool operator<(const variant& other) const {
    if (other.kind_ != this->kind_) {
      return other.kind_ < this->kind_;
    }
    return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(lessThan, kind_, *this, other);
  }

  bool equals(const variant& other) const {
    if (other.kind_ != this->kind_) {
      return false;
    }
    if (other.isNull()) {
      return this->isNull();
    }
    return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(equals, kind_, *this, other);
  }

  bool equalsWithNullEqualsNull(const variant& other) const {
    if (other.kind_ != this->kind_) {
      return false;
    }
    return dispatchDynamicVariantEquality(*this, other, true);
  }

  variant(variant&& other) : kind_{other.kind_}, ptr_{other.ptr_} {
    other.ptr_ = nullptr;
  }

  ~variant() {
    if (ptr_ != nullptr) {
      dynamicFree();
    }
  }

  std::string toJson() const;

  folly::dynamic serialize() const;

  static variant create(const folly::dynamic&);

  bool isNull() const {
    return ptr_ == nullptr;
  }

  bool isSet() const {
    return ptr_ != nullptr;
  }

  bool hasValue() const {
    return ptr_ != nullptr;
  }

  void checkNotNull() const {
    if (ptr_ == nullptr) {
      // Error path outlined to encourage inlining of the branch.
      throwCheckNotNullError();
    }
  }

  void checkIsKind(TypeKind kind) const {
    if (kind_ != kind) {
      // Error path outlined to encourage inlining of the branch.
      throwCheckIsKindError(kind);
    }
  }

  TypeKind kind() const {
    return kind_;
  }

  template <TypeKind KIND>
  const auto& value() const {
    checkIsKind(KIND);
    checkNotNull();
    return *static_cast<
        const typename detail::VariantTypeTraits<KIND>::stored_type*>(ptr_);
  }

  template <typename T>
  const auto& value() const {
    return value<CppToType<T>::typeKind>();
  }

  uint64_t hash() const;

  template <TypeKind KIND>
  const auto* valuePointer() const {
    checkIsKind(KIND);
    return static_cast<
        const typename detail::VariantTypeTraits<KIND>::stored_type*>(ptr_);
  }

  template <typename T>
  const auto* valuePointer() const {
    return valuePointer<CppToType<T>::typeKind>();
  }

  const std::vector<variant>& row() const {
    return value<TypeKind::ROW>();
  }

  const std::map<variant, variant>& map() const {
    return value<TypeKind::MAP>();
  }

  const std::vector<variant>& array() const {
    return value<TypeKind::ARRAY>();
  }

  template <class T>
  std::shared_ptr<T> opaque() const {
    const auto& capsule = value<TypeKind::OPAQUE>();
    VELOX_CHECK(
        capsule.type->typeIndex() == std::type_index(typeid(T)),
        "Requested {} but contains {}",
        OpaqueType::create<T>()->toString(),
        capsule.type->toString());
    return std::static_pointer_cast<T>(capsule.obj);
  }

  std::shared_ptr<const Type> inferType() const {
    switch (kind_) {
      case TypeKind::MAP: {
        TypePtr keyType;
        TypePtr valueType;
        auto& m = map();
        for (auto& pair : m) {
          if (keyType == nullptr && !pair.first.isNull()) {
            keyType = pair.first.inferType();
          }
          if (valueType == nullptr && !pair.second.isNull()) {
            valueType = pair.second.inferType();
          }
          if (keyType && valueType) {
            break;
          }
        }
        return MAP(
            keyType ? keyType : UNKNOWN(), valueType ? valueType : UNKNOWN());
      }
      case TypeKind::ROW: {
        auto& r = row();
        std::vector<std::shared_ptr<const Type>> children{};
        for (auto& v : r) {
          children.push_back(v.inferType());
        }
        return ROW(std::move(children));
      }
      case TypeKind::ARRAY: {
        TypePtr elementType = UNKNOWN();
        if (!isNull()) {
          auto& a = array();
          if (!a.empty()) {
            elementType = a.at(0).inferType();
          }
        }
        return ARRAY(elementType);
      }
      case TypeKind::OPAQUE: {
        return value<TypeKind::OPAQUE>().type;
      }
      case TypeKind::UNKNOWN: {
        return UNKNOWN();
      }
      default:
        return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(kind2type, kind_);
    }
  }

  friend std::ostream& operator<<(std::ostream& stream, const variant& k) {
    stream << k.toJson();
    return stream;
  }

  // Compares REAL and DOUBLE (only) types for equality using kEpsilon.
  // For testing purposes.
  static bool equalsFloatingPointWithEpsilon(
      const variant& a,
      const variant& b);

  // Uses kEpsilon to compare floating point types (REAL and DOUBLE).
  // For testing purposes.
  bool lessThanWithEpsilon(const variant& other) const;

  // Uses kEpsilon to compare floating point types (REAL and DOUBLE).
  // For testing purposes.
  bool equalsWithEpsilon(const variant& other) const;

 private:
  TypeKind kind_;
  // TODO: it'd be more efficient to put union here if it ever becomes a problem
  const void* ptr_;
};

inline bool operator==(const variant& a, const variant& b) {
  return a.equals(b);
}

inline bool operator!=(const variant& a, const variant& b) {
  return !(a == b);
}

struct VariantConverter {
  template <TypeKind FromKind, TypeKind ToKind>
  static variant convert(const variant& value) {
    if (value.isNull()) {
      return variant{value.kind()};
    } else {
      bool nullOutput = false;
      auto v = variant{
          util::Converter<ToKind>::cast(value.value<FromKind>(), nullOutput)};
      if (nullOutput) {
        throw std::invalid_argument("Velox cast error");
      }
      return v;
    }
  }

  template <TypeKind ToKind>
  static variant convert(const variant& value) {
    switch (value.kind()) {
      case TypeKind::BOOLEAN:
        return convert<TypeKind::BOOLEAN, ToKind>(value);
      case TypeKind::TINYINT:
        return convert<TypeKind::TINYINT, ToKind>(value);
      case TypeKind::SMALLINT:
        return convert<TypeKind::SMALLINT, ToKind>(value);
      case TypeKind::INTEGER:
        return convert<TypeKind::INTEGER, ToKind>(value);
      case TypeKind::BIGINT:
        return convert<TypeKind::BIGINT, ToKind>(value);
      case TypeKind::REAL:
        return convert<TypeKind::REAL, ToKind>(value);
      case TypeKind::DOUBLE:
        return convert<TypeKind::DOUBLE, ToKind>(value);
      case TypeKind::VARCHAR:
        return convert<TypeKind::VARCHAR, ToKind>(value);
      case TypeKind::VARBINARY:
        return convert<TypeKind::VARBINARY, ToKind>(value);
      case TypeKind::DATE:
      case TypeKind::INTERVAL_DAY_TIME:
      case TypeKind::TIMESTAMP:
        // Default date/timestamp conversion is prone to errors and implicit
        // assumptions. Block converting timestamp to integer, double and
        // std::string types. The callers should implement their own conversion
        //  from value.
        VELOX_NYI();
      default:
        VELOX_NYI();
    }
  }

  static variant convert(const variant& value, TypeKind toKind) {
    return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(convert, toKind, value);
  }
};

#undef VELOX_VARIANT_SCALAR_MEMBERS

} // namespace facebook::velox
