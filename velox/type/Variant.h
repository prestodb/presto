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
#include <folly/Conv.h>

#include "folly/dynamic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/VeloxException.h"
#include "velox/type/Conversions.h"
#include "velox/type/CppToType.h"
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
template <typename T, TypeKind KIND, bool usesCustomComparison>
struct TypeStorage {
  T storedValue;
};

template <typename T, TypeKind KIND>
struct TypeStorage<T, KIND, true> {
  T storedValue;
  std::shared_ptr<const CanProvideCustomComparisonType<KIND>>
      typeWithCustomComparison;
};

template <TypeKind KIND>
using scalar_stored_type = typename TypeTraits<KIND>::DeepCopiedType;

template <TypeKind KIND, bool usesCustomComparison = false, typename = void>
struct VariantTypeTraits {};

template <TypeKind KIND, bool usesCustomComparison>
struct VariantTypeTraits<
    KIND,
    usesCustomComparison,
    std::enable_if_t<
        TypeTraits<KIND>::isPrimitiveType && KIND != TypeKind::VARCHAR &&
            KIND != TypeKind::VARBINARY,
        void>> {
  using native_type = typename TypeTraits<KIND>::NativeType;
  using stored_type =
      TypeStorage<scalar_stored_type<KIND>, KIND, usesCustomComparison>;
  using value_type = scalar_stored_type<KIND>;
};

template <TypeKind KIND, bool usesCustomComparison>
struct VariantTypeTraits<
    KIND,
    usesCustomComparison,
    std::enable_if_t<
        KIND == TypeKind::VARCHAR || KIND == TypeKind::VARBINARY,
        void>> {
  using native_type = folly::StringPiece;
  using stored_type =
      TypeStorage<scalar_stored_type<KIND>, KIND, usesCustomComparison>;
  using value_type = scalar_stored_type<KIND>;
};

template <bool usesCustomComparison>
struct VariantTypeTraits<TypeKind::ROW, usesCustomComparison> {
  using stored_type =
      TypeStorage<std::vector<variant>, TypeKind::ROW, usesCustomComparison>;
  using value_type = std::vector<variant>;
};

template <bool usesCustomComparison>
struct VariantTypeTraits<TypeKind::MAP, usesCustomComparison> {
  using stored_type = TypeStorage<
      std::map<variant, variant>,
      TypeKind::MAP,
      usesCustomComparison>;
  using value_type = std::map<variant, variant>;
};

template <bool usesCustomComparison>
struct VariantTypeTraits<TypeKind::ARRAY, usesCustomComparison> {
  using stored_type =
      TypeStorage<std::vector<variant>, TypeKind::ARRAY, usesCustomComparison>;
  using value_type = std::vector<variant>;
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

template <bool usesCustomComparison>
struct VariantTypeTraits<TypeKind::OPAQUE, usesCustomComparison> {
  using stored_type =
      TypeStorage<OpaqueCapsule, TypeKind::OPAQUE, usesCustomComparison>;
  using value_type = OpaqueCapsule;
};
} // namespace detail

class variant {
 private:
  variant(TypeKind kind, void* ptr, bool usesCustomComparison = false)
      : ptr_{ptr}, kind_{kind}, usesCustomComparison_(usesCustomComparison) {}

  template <TypeKind KIND>
  bool lessThan(const variant& other) const;

  template <TypeKind KIND>
  bool equals(const variant& other) const;

  template <TypeKind KIND>
  uint64_t hash() const;

  template <bool usesCustomComparison, TypeKind KIND>
  void typedDestroy() {
    delete static_cast<const typename detail::VariantTypeTraits<
        KIND,
        usesCustomComparison>::stored_type*>(ptr_);
    ptr_ = nullptr;
  }

  template <bool usesCustomComparison, TypeKind KIND>
  void typedCopy(const void* other) {
    using stored_type = typename detail::
        VariantTypeTraits<KIND, usesCustomComparison>::stored_type;
    ptr_ = new stored_type{*static_cast<const stored_type*>(other)};
  }

  void dynamicCopy(const void* p, const TypeKind kind) {
    if (usesCustomComparison_) {
      VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(typedCopy, true, kind, p);
    } else {
      VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(typedCopy, false, kind, p);
    }
  }

  void dynamicFree() {
    if (usesCustomComparison_) {
      VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(typedDestroy, true, kind_);
    } else {
      VELOX_DYNAMIC_TEMPLATE_TYPE_DISPATCH_ALL(typedDestroy, false, kind_);
    }
  }

  template <TypeKind K>
  static const std::shared_ptr<const Type> kind2type() {
    return TypeFactory<K>::create();
  }

  [[noreturn]] void throwCheckIsKindError(TypeKind kind) const;

  [[noreturn]] void throwCheckPtrError() const;

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

#define VELOX_VARIANT_SCALAR_MEMBERS(KIND)                                \
  /* implicit */ variant(                                                 \
      typename detail::VariantTypeTraits<KIND, false>::native_type v)     \
      : ptr_{new detail::VariantTypeTraits<KIND, false>::stored_type{v}}, \
        kind_{KIND},                                                      \
        usesCustomComparison_{false} {}

  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::BOOLEAN)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::TINYINT)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::SMALLINT)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::INTEGER)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::BIGINT)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::HUGEINT)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::REAL)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::DOUBLE)
  // VARBINARY conflicts with VARCHAR, so we don't gen these methods
  // VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::VARBINARY);
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::TIMESTAMP)
  VELOX_VARIANT_SCALAR_MEMBERS(TypeKind::UNKNOWN)
#undef VELOX_VARIANT_SCALAR_MEMBERS

  /* implicit */ variant(
      typename detail::VariantTypeTraits<TypeKind::VARCHAR, false>::native_type
          v)
      : ptr_{new detail::VariantTypeTraits<TypeKind::VARCHAR, false>::
                 stored_type{v.str()}},
        kind_{TypeKind::VARCHAR},
        usesCustomComparison_{false} {}

  // On 64-bit platforms `int64_t` is declared as `long int`, not `long long
  // int`, thus adding an extra overload to make literals like 1LL resolve
  // correctly. Note that one has to use template T because otherwise SFINAE
  // doesn't work, but in this case T = long long
  template <
      typename T = long long,
      std::enable_if_t<
          std::is_same_v<T, long long> && !std::is_same_v<long long, int64_t>,
          bool> = true>
  /* implicit */ variant(const T& v) : variant(static_cast<int64_t>(v)) {}

  static variant row(const std::vector<variant>& inputs) {
    return {
        TypeKind::ROW,
        new
        typename detail::VariantTypeTraits<TypeKind::ROW, false>::stored_type{
            inputs}};
  }

  static variant row(std::vector<variant>&& inputs) {
    return {
        TypeKind::ROW,
        new
        typename detail::VariantTypeTraits<TypeKind::ROW, false>::stored_type{
            std::move(inputs)}};
  }

  static variant map(const std::map<variant, variant>& inputs) {
    return {
        TypeKind::MAP,
        new
        typename detail::VariantTypeTraits<TypeKind::MAP, false>::stored_type{
            inputs}};
  }

  static variant map(std::map<variant, variant>&& inputs) {
    return {
        TypeKind::MAP,
        new
        typename detail::VariantTypeTraits<TypeKind::MAP, false>::stored_type{
            std::move(inputs)}};
  }

  static variant timestamp(const Timestamp& input) {
    return {
        TypeKind::TIMESTAMP,
        new typename detail::VariantTypeTraits<TypeKind::TIMESTAMP, false>::
            stored_type{input}};
  }

  template <TypeKind KIND>
  static variant typeWithCustomComparison(
      typename TypeTraits<KIND>::NativeType input,
      const TypePtr& type) {
    return {
        KIND,
        new typename detail::VariantTypeTraits<KIND, true>::stored_type{
            input,
            std::dynamic_pointer_cast<
                const CanProvideCustomComparisonType<KIND>>(type)},
        true};
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

  static void verifyArrayElements(const std::vector<variant>& inputs);

  static variant array(const std::vector<variant>& inputs) {
    verifyArrayElements(inputs);
    return {
        TypeKind::ARRAY,
        new
        typename detail::VariantTypeTraits<TypeKind::ARRAY, false>::stored_type{
            inputs}};
  }

  static variant array(std::vector<variant>&& inputs) {
    verifyArrayElements(inputs);
    return {
        TypeKind::ARRAY,
        new
        typename detail::VariantTypeTraits<TypeKind::ARRAY, false>::stored_type{
            std::move(inputs)}};
  }

  variant()
      : ptr_{nullptr}, kind_{TypeKind::INVALID}, usesCustomComparison_(false) {}

  /* implicit */ variant(TypeKind kind)
      : ptr_{nullptr}, kind_{kind}, usesCustomComparison_(false) {}

  variant(const variant& other)
      : ptr_{nullptr},
        kind_{other.kind_},
        usesCustomComparison_(other.usesCustomComparison_) {
    auto op = other.ptr_;
    if (op != nullptr) {
      dynamicCopy(other.ptr_, other.kind_);
    }
  }

  // Support construction from StringView as well as StringPiece.
  /* implicit */ variant(StringView view) : variant{folly::StringPiece{view}} {}

  // Break ties between implicit conversions to StringView/StringPiece.
  /* implicit */ variant(std::string str)
      : ptr_{new std::string{std::move(str)}},
        kind_{TypeKind::VARCHAR},
        usesCustomComparison_(false) {}

  /* implicit */ variant(const char* str)
      : ptr_{new std::string{str}},
        kind_{TypeKind::VARCHAR},
        usesCustomComparison_(false) {}

  template <TypeKind KIND>
  static variant create(
      typename detail::VariantTypeTraits<KIND, false>::value_type&& v) {
    return variant{
        KIND,
        new typename detail::VariantTypeTraits<KIND, false>::stored_type{
            std::move(v)}};
  }

  template <TypeKind KIND>
  static variant create(
      const typename detail::VariantTypeTraits<KIND, false>::value_type& v) {
    return variant{
        KIND,
        new typename detail::VariantTypeTraits<KIND, false>::stored_type{v}};
  }

  template <typename T>
  static variant create(const typename detail::VariantTypeTraits<
                        CppToType<T>::typeKind,
                        false>::value_type& v) {
    return create<CppToType<T>::typeKind>(v);
  }

  static variant null(TypeKind kind) {
    return variant{kind};
  }

  static variant binary(std::string val) {
    return variant{TypeKind::VARBINARY, new std::string{std::move(val)}};
  }

  variant& operator=(const variant& other) {
    if (ptr_ != nullptr) {
      dynamicFree();
    }
    kind_ = other.kind_;
    usesCustomComparison_ = other.usesCustomComparison_;
    if (other.ptr_ != nullptr) {
      dynamicCopy(other.ptr_, other.kind_);
    }
    return *this;
  }

  variant& operator=(variant&& other) noexcept {
    if (ptr_ != nullptr) {
      dynamicFree();
    }
    kind_ = other.kind_;
    usesCustomComparison_ = other.usesCustomComparison_;
    if (other.ptr_ != nullptr) {
      ptr_ = other.ptr_;
      other.ptr_ = nullptr;
    }
    return *this;
  }

  bool operator<(const variant& other) const;

  bool equals(const variant& other) const;

  bool equalsWithNullEqualsNull(const variant& other) const {
    if (other.kind_ != this->kind_) {
      return false;
    }
    return dispatchDynamicVariantEquality(*this, other, true);
  }

  variant(variant&& other) noexcept
      : ptr_{other.ptr_},
        kind_{other.kind_},
        usesCustomComparison_(other.usesCustomComparison_) {
    other.ptr_ = nullptr;
  }

  ~variant() {
    if (ptr_ != nullptr) {
      dynamicFree();
    }
  }

  std::string toJson(const TypePtr& type) const;
  std::string toJsonUnsafe(const TypePtr& type = nullptr) const;

  /// Used by python binding, do not change signature.
  std::string pyToJson() const {
    return toJsonUnsafe();
  }

  folly::dynamic serialize() const;

  static variant create(const folly::dynamic&);

  bool hasValue() const {
    return !isNull();
  }

  /// Similar to hasValue(). Legacy.
  bool isSet() const {
    return hasValue();
  }

  void checkPtr() const {
    if (ptr_ == nullptr) {
      // Error path outlined to encourage inlining of the branch.
      throwCheckPtrError();
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
    checkPtr();

    if (usesCustomComparison_) {
      return static_cast<const typename detail::VariantTypeTraits<KIND, true>::
                             stored_type*>(ptr_)
          ->storedValue;
    } else {
      return static_cast<const typename detail::VariantTypeTraits<KIND, false>::
                             stored_type*>(ptr_)
          ->storedValue;
    }
  }

  template <typename T>
  const auto& value() const {
    return value<CppToType<T>::typeKind>();
  }

  bool isNull() const {
    return ptr_ == nullptr;
  }

  uint64_t hash() const;

  template <TypeKind KIND>
  const auto* valuePointer() const {
    checkIsKind(KIND);

    if (usesCustomComparison_) {
      return static_cast<
          const typename detail::VariantTypeTraits<KIND, true>::stored_type*>(
          ptr_);
    } else {
      return static_cast<
          const typename detail::VariantTypeTraits<KIND, false>::stored_type*>(
          ptr_);
    }
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
        children.reserve(r.size());
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
    const auto type = k.inferType();
    stream << k.toJson(type);
    return stream;
  }

  // Uses kEpsilon to compare floating point types (REAL and DOUBLE).
  // For testing purposes.
  bool lessThanWithEpsilon(const variant& other) const;

  // Uses kEpsilon to compare floating point types (REAL and DOUBLE).
  // For testing purposes.
  bool equalsWithEpsilon(const variant& other) const;

 private:
  template <TypeKind KIND>
  std::shared_ptr<const CanProvideCustomComparisonType<KIND>>
  customComparisonType() const {
    return static_cast<const typename detail::VariantTypeTraits<KIND, true>::
                           stored_type*>(ptr_)
        ->typeWithCustomComparison;
  }

  // TODO: it'd be more efficient to put union here if it ever becomes a
  // problem
  const void* ptr_;
  TypeKind kind_;
  // If the variant represents the value of a type that provides custom
  // comparisons.
  bool usesCustomComparison_;
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
    }

    const auto converted =
        util::Converter<ToKind>::tryCast(value.value<FromKind>())
            .thenOrThrow(folly::identity, [&](const Status& status) {
              VELOX_USER_FAIL("{}", status.message());
            });
    return {converted};
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
      case TypeKind::TIMESTAMP:
      case TypeKind::HUGEINT:
        // Default date/timestamp conversion is prone to errors and implicit
        // assumptions. Block converting timestamp to integer, double and
        // std::string types. The callers should implement their own
        // conversion
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

} // namespace facebook::velox
