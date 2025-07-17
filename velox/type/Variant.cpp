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

#include "velox/type/Variant.h"
#include <cfloat>
#include "folly/json.h"
#include "velox/common/encode/Base64.h"
#include "velox/type/DecimalUtil.h"
#include "velox/type/FloatingPointUtil.h"

namespace facebook::velox {

namespace {

bool dispatchDynamicVariantEquality(
    const Variant& a,
    const Variant& b,
    const bool& enableNullEqualsNull);

template <bool nullEqualsNull>
bool evaluateNullEquality(const Variant& a, const Variant& b) {
  if constexpr (nullEqualsNull) {
    if (a.isNull() && b.isNull()) {
      return true;
    }
  }
  return false;
}

template <TypeKind KIND>
struct VariantEquality;

// scalars
template <TypeKind KIND>
struct VariantEquality {
  template <bool NullEqualsNull>
  static bool equals(const Variant& a, const Variant& b) {
    if (a.isNull() || b.isNull()) {
      return evaluateNullEquality<NullEqualsNull>(a, b);
    }
    return a.value<KIND>() == b.value<KIND>();
  }
};

// timestamp
template <>
struct VariantEquality<TypeKind::TIMESTAMP> {
  template <bool NullEqualsNull>
  static bool equals(const Variant& a, const Variant& b) {
    if (a.isNull() || b.isNull()) {
      return evaluateNullEquality<NullEqualsNull>(a, b);
    } else {
      return a.value<TypeKind::TIMESTAMP>() == b.value<TypeKind::TIMESTAMP>();
    }
  }
};

// array
template <>
struct VariantEquality<TypeKind::ARRAY> {
  template <bool NullEqualsNull>
  static bool equals(const Variant& a, const Variant& b) {
    if (a.isNull() || b.isNull()) {
      return evaluateNullEquality<NullEqualsNull>(a, b);
    }
    auto& aArray = a.value<TypeKind::ARRAY>();
    auto& bArray = b.value<TypeKind::ARRAY>();
    if (aArray.size() != bArray.size()) {
      return false;
    }
    for (size_t i = 0; i != aArray.size(); ++i) {
      // todo(youknowjack): switch outside the loop
      bool result =
          dispatchDynamicVariantEquality(aArray[i], bArray[i], NullEqualsNull);
      if (!result) {
        return false;
      }
    }
    return true;
  }
};

template <>
struct VariantEquality<TypeKind::ROW> {
  template <bool NullEqualsNull>
  static bool equals(const Variant& a, const Variant& b) {
    if (a.isNull() || b.isNull()) {
      return evaluateNullEquality<NullEqualsNull>(a, b);
    }
    auto& aRow = a.value<TypeKind::ROW>();
    auto& bRow = b.value<TypeKind::ROW>();

    // compare array size
    if (aRow.size() != bRow.size()) {
      return false;
    }
    // compare array values
    for (size_t i = 0; i != aRow.size(); ++i) {
      bool result =
          dispatchDynamicVariantEquality(aRow[i], bRow[i], NullEqualsNull);
      if (!result) {
        return false;
      }
    }
    return true;
  }
};

template <>
struct VariantEquality<TypeKind::MAP> {
  template <bool NullEqualsNull>
  static bool equals(const Variant& a, const Variant& b) {
    if (a.isNull() || b.isNull()) {
      return evaluateNullEquality<NullEqualsNull>(a, b);
    }

    auto& aMap = a.value<TypeKind::MAP>();
    auto& bMap = b.value<TypeKind::MAP>();
    // compare map size
    if (aMap.size() != bMap.size()) {
      return false;
    }
    // compare map values
    for (auto it_a = aMap.begin(), it_b = bMap.begin();
         it_a != aMap.end() && it_b != bMap.end();
         ++it_a, ++it_b) {
      if (dispatchDynamicVariantEquality(
              it_a->first, it_b->first, NullEqualsNull) &&
          dispatchDynamicVariantEquality(
              it_a->second, it_b->second, NullEqualsNull)) {
        continue;
      } else {
        return false;
      }
    }
    return true;
  }
};

bool dispatchDynamicVariantEquality(
    const Variant& a,
    const Variant& b,
    const bool& enableNullEqualsNull) {
  if (enableNullEqualsNull) {
    return VELOX_DYNAMIC_TYPE_DISPATCH_METHOD(
        VariantEquality, equals<true>, a.kind(), a, b);
  }
  return VELOX_DYNAMIC_TYPE_DISPATCH_METHOD(
      VariantEquality, equals<false>, a.kind(), a, b);
}
} // namespace

std::string encloseWithQuote(std::string str) {
  constexpr auto kDoubleQuote = '"';

  std::stringstream ss;
  ss << std::quoted(str, kDoubleQuote, kDoubleQuote);
  return ss.str();
}

template <typename T>
std::string stringifyFloatingPointerValue(T val) {
  if (std::isinf(val) || std::isnan(val)) {
    return encloseWithQuote(folly::to<std::string>(val));
  } else {
    return folly::to<std::string>(val);
  }
}

void Variant::throwCheckIsKindError(TypeKind kind) const {
  throw std::invalid_argument{fmt::format(
      "wrong kind! {} != {}",
      mapTypeKindToName(kind_),
      mapTypeKindToName(kind))};
}

void Variant::throwCheckPtrError() const {
  throw std::invalid_argument{"missing Variant value"};
}

std::string Variant::toString(const TypePtr& type) const {
  if (isNull()) {
    return "null";
  }

  VELOX_CHECK(type);

  VELOX_CHECK_EQ(this->kind(), type->kind(), "Wrong type in Variant::toString");

  switch (type->kind()) {
    case TypeKind::VARBINARY: {
      auto& str = value<TypeKind::VARBINARY>();
      auto encoded = encoding::Base64::encode(str);
      return encoded;
    }
    case TypeKind::VARCHAR: {
      auto& str = value<TypeKind::VARCHAR>();
      return str;
    }
    case TypeKind::HUGEINT: {
      VELOX_CHECK(type->isLongDecimal());
      return DecimalUtil::toString(value<TypeKind::HUGEINT>(), type);
    }
    case TypeKind::TINYINT:
      [[fallthrough]];
    case TypeKind::SMALLINT:
      [[fallthrough]];
    case TypeKind::INTEGER:
      if (type->isDate()) {
        return DATE()->toString(value<TypeKind::INTEGER>());
      }
      [[fallthrough]];
    case TypeKind::BIGINT:
      if (type->isShortDecimal()) {
        return DecimalUtil::toString(value<TypeKind::BIGINT>(), type);
      }
      [[fallthrough]];
    case TypeKind::BOOLEAN: {
      auto converted = VariantConverter::convert<TypeKind::VARCHAR>(*this);
      if (converted.isNull()) {
        return "null";
      } else {
        return converted.value<TypeKind::VARCHAR>();
      }
    }
    case TypeKind::REAL:
      return folly::to<std::string>(value<TypeKind::REAL>());
    case TypeKind::DOUBLE:
      return folly::to<std::string>(value<TypeKind::DOUBLE>());
    case TypeKind::TIMESTAMP: {
      auto& timestamp = value<TypeKind::TIMESTAMP>();
      return timestamp.toString();
    }
    case TypeKind::ARRAY:
    case TypeKind::MAP:
    case TypeKind::ROW:
    case TypeKind::OPAQUE:
    case TypeKind::FUNCTION:
    case TypeKind::UNKNOWN:
    case TypeKind::INVALID:
      return toJson(type);
  }

  folly::assume_unreachable();
}

namespace {
const folly::json::serialization_opts& getOpts() {
  static const folly::json::serialization_opts kOpts;
  return kOpts;
}
} // namespace

std::string Variant::toJson(const TypePtr& type) const {
  // todo(youknowjack): consistent story around std::stringifying, converting,
  // and other basic operations. Stringification logic should not be specific
  // to variants; it should be consistent for all map representations

  if (isNull()) {
    return "null";
  }

  VELOX_CHECK(type);

  VELOX_CHECK_EQ(this->kind(), type->kind(), "Wrong type in Variant::toJson");

  switch (kind_) {
    case TypeKind::MAP: {
      auto& map = value<TypeKind::MAP>();
      std::string b{};
      b += "[";
      bool first = true;
      for (auto& pair : map) {
        if (!first) {
          b += ",";
        }
        b += "{\"key\":";
        b += pair.first.toJson(type->childAt(0));
        b += ",\"value\":";
        b += pair.second.toJson(type->childAt(1));
        b += "}";
        first = false;
      }
      b += "]";
      return b;
    }
    case TypeKind::ROW: {
      auto& row = value<TypeKind::ROW>();
      std::string b{};
      b += "[";
      bool first = true;
      uint32_t idx = 0;
      VELOX_CHECK_EQ(
          row.size(),
          type->size(),
          "Wrong number of fields in a struct in Variant::toJson");
      for (auto& v : row) {
        if (!first) {
          b += ",";
        }
        b += v.toJson(type->childAt(idx++));
        first = false;
      }
      b += "]";
      return b;
    }
    case TypeKind::ARRAY: {
      auto& array = value<TypeKind::ARRAY>();
      std::string b{};
      b += "[";
      bool first = true;
      auto arrayElementType = type->childAt(0);
      for (auto& v : array) {
        if (!first) {
          b += ",";
        }
        b += v.toJson(arrayElementType);
        first = false;
      }
      b += "]";
      return b;
    }
    case TypeKind::VARBINARY: {
      auto& str = value<TypeKind::VARBINARY>();
      auto encoded = encoding::Base64::encode(str);
      return '"' + encoded + '"';
    }
    case TypeKind::VARCHAR: {
      auto& str = value<TypeKind::VARCHAR>();
      std::string target;
      folly::json::escapeString(str, target, getOpts());
      return target;
    }
    case TypeKind::HUGEINT: {
      VELOX_CHECK(type->isLongDecimal());
      return DecimalUtil::toString(value<TypeKind::HUGEINT>(), type);
    }
    case TypeKind::TINYINT:
      [[fallthrough]];
    case TypeKind::SMALLINT:
      [[fallthrough]];
    case TypeKind::INTEGER:
      if (type->isDate()) {
        return '"' + DATE()->toString(value<TypeKind::INTEGER>()) + '"';
      }
      [[fallthrough]];
    case TypeKind::BIGINT:
      if (type->isShortDecimal()) {
        return DecimalUtil::toString(value<TypeKind::BIGINT>(), type);
      }
      [[fallthrough]];
    case TypeKind::BOOLEAN: {
      auto converted = VariantConverter::convert<TypeKind::VARCHAR>(*this);
      if (converted.isNull()) {
        return "null";
      } else {
        return converted.value<TypeKind::VARCHAR>();
      }
    }
    case TypeKind::REAL: {
      return stringifyFloatingPointerValue<float>(value<TypeKind::REAL>());
    }
    case TypeKind::DOUBLE: {
      return stringifyFloatingPointerValue<double>(value<TypeKind::DOUBLE>());
    }
    case TypeKind::TIMESTAMP: {
      auto& timestamp = value<TypeKind::TIMESTAMP>();
      return '"' + timestamp.toString() + '"';
    }
    case TypeKind::OPAQUE: {
      // Although this is not used for deserialization, we need to include the
      // real data because commonExpressionEliminationRules uses
      // CallTypedExpr.toString as key, which ends up using this string.
      // Opaque types that want to use common expression elimination need to
      // make their serialization deterministic.
      const detail::OpaqueCapsule& capsule = value<TypeKind::OPAQUE>();
      auto serializeFunction = capsule.type->getSerializeFunc();
      return "Opaque<type:" + capsule.type->toString() + ",value:\"" +
          serializeFunction(capsule.obj) + "\">";
    }
    case TypeKind::FUNCTION:
    case TypeKind::UNKNOWN:
    case TypeKind::INVALID:
      VELOX_NYI();
  }

  VELOX_UNSUPPORTED(
      "Unsupported: given type {} is not json-ready", mapTypeKindToName(kind_));
}

// This is the unsafe older implementation of toJson. It is kept here for
// backward compatibility with Meta's internal python bindings.
std::string Variant::toJsonUnsafe(const TypePtr& type) const {
  if (isNull()) {
    return "null";
  }

  switch (kind_) {
    case TypeKind::MAP: {
      auto& map = value<TypeKind::MAP>();
      std::string b{};
      b += "[";
      bool first = true;
      for (auto& pair : map) {
        if (!first) {
          b += ",";
        }
        b += "{\"key\":";
        b += pair.first.toJsonUnsafe();
        b += ",\"value\":";
        b += pair.second.toJsonUnsafe();
        b += "}";
        first = false;
      }
      b += "]";
      return b;
    }
    case TypeKind::ROW: {
      auto& row = value<TypeKind::ROW>();
      std::string b{};
      b += "[";
      bool first = true;
      for (auto& v : row) {
        if (!first) {
          b += ",";
        }
        b += v.toJsonUnsafe();

        first = false;
      }
      b += "]";
      return b;
    }
    case TypeKind::ARRAY: {
      auto& array = value<TypeKind::ARRAY>();
      std::string b{};
      b += "[";
      bool first = true;
      for (auto& v : array) {
        if (!first) {
          b += ",";
        }
        b += v.toJsonUnsafe();
        first = false;
      }
      b += "]";
      return b;
    }
    case TypeKind::VARBINARY: {
      auto& str = value<TypeKind::VARBINARY>();
      auto encoded = encoding::Base64::encode(str);
      return '"' + encoded + '"';
    }
    case TypeKind::VARCHAR: {
      auto& str = value<TypeKind::VARCHAR>();
      std::string target;
      folly::json::escapeString(str, target, getOpts());
      return target;
    }
    case TypeKind::HUGEINT: {
      VELOX_CHECK(type && type->isLongDecimal());
      return DecimalUtil::toString(value<TypeKind::HUGEINT>(), type);
    }
    case TypeKind::TINYINT:
      [[fallthrough]];
    case TypeKind::SMALLINT:
      [[fallthrough]];
    case TypeKind::INTEGER:
      if (type->isDate()) {
        return '"' + DATE()->toString(value<TypeKind::INTEGER>()) + '"';
      }
      [[fallthrough]];
    case TypeKind::BIGINT:
      if (type && type->isShortDecimal()) {
        return DecimalUtil::toString(value<TypeKind::BIGINT>(), type);
      }
      [[fallthrough]];
    case TypeKind::BOOLEAN: {
      auto converted = VariantConverter::convert<TypeKind::VARCHAR>(*this);
      if (converted.isNull()) {
        return "null";
      } else {
        return converted.value<TypeKind::VARCHAR>();
      }
    }
    case TypeKind::REAL: {
      return stringifyFloatingPointerValue<float>(value<TypeKind::REAL>());
    }
    case TypeKind::DOUBLE: {
      return stringifyFloatingPointerValue<double>(value<TypeKind::DOUBLE>());
    }
    case TypeKind::TIMESTAMP: {
      auto& timestamp = value<TypeKind::TIMESTAMP>();
      return '"' + timestamp.toString() + '"';
    }
    case TypeKind::OPAQUE: {
      // Although this is not used for deserialization, we need to include the
      // real data because commonExpressionEliminationRules uses
      // CallTypedExpr.toString as key, which ends up using this string.
      // Opaque types that want to use common expression elimination need to
      // make their serialization deterministic.
      const detail::OpaqueCapsule& capsule = value<TypeKind::OPAQUE>();
      auto serializeFunction = capsule.type->getSerializeFunc();
      return "Opaque<type:" + capsule.type->toString() + ",value:\"" +
          serializeFunction(capsule.obj) + "\">";
    }
    case TypeKind::FUNCTION:
    case TypeKind::UNKNOWN:
    case TypeKind::INVALID:
      VELOX_NYI();
  }

  VELOX_UNSUPPORTED(
      "Unsupported: given type {} is not json-ready", mapTypeKindToName(kind_));
}

void serializeOpaque(
    folly::dynamic& variantObj,
    const detail::OpaqueCapsule& opaqueValue) {
  try {
    auto serializeFunction = opaqueValue.type->getSerializeFunc();
    variantObj["value"] = serializeFunction(opaqueValue.obj);
    variantObj["opaque_type"] = folly::json::serialize(
        opaqueValue.type->serialize(), getSerializationOptions());
  } catch (VeloxRuntimeError& ex) {
    // Re-throw error for backwards compatibility.
    // Want to return error_code::kNotImplemented rather
    // than error_code::kInvalidState
    VELOX_NYI(ex.message());
  }
}

folly::dynamic Variant::serialize() const {
  folly::dynamic variantObj = folly::dynamic::object;

  variantObj["type"] = mapTypeKindToName(kind_);
  auto& objValue = variantObj["value"];
  if (isNull()) {
    objValue = nullptr;
    return variantObj;
  }
  switch (kind_) {
    case TypeKind::MAP: {
      auto& map = value<TypeKind::MAP>();
      objValue = velox::ISerializable::serialize(map);
      break;
    }
    case TypeKind::ROW: {
      auto& row = value<TypeKind::ROW>();
      folly::dynamic arr = folly::dynamic::array;
      for (auto& v : row) {
        arr.push_back(v.serialize());
      }
      objValue = std::move(arr);
      break;
    }
    case TypeKind::ARRAY: {
      auto& array = value<TypeKind::ARRAY>();
      folly::dynamic arr = folly::dynamic::array;
      for (auto& v : array) {
        arr.push_back(v.serialize());
      }
      objValue = std::move(arr);
      break;
    }
    case TypeKind::VARBINARY: {
      auto& str = value<TypeKind::VARBINARY>();
      objValue = encoding::Base64::encode(str);
      break;
    }

    case TypeKind::TINYINT: {
      objValue = value<TypeKind::TINYINT>();
      break;
    }
    case TypeKind::SMALLINT: {
      objValue = value<TypeKind::SMALLINT>();
      break;
    }
    case TypeKind::INTEGER: {
      objValue = value<TypeKind::INTEGER>();
      break;
    }
    case TypeKind::BIGINT: {
      objValue = value<TypeKind::BIGINT>();
      break;
    }
    case TypeKind::HUGEINT: {
      objValue = value<TypeKind::HUGEINT>();
      break;
    }
    case TypeKind::BOOLEAN: {
      objValue = value<TypeKind::BOOLEAN>();
      break;
    }
    case TypeKind::REAL: {
      objValue = value<TypeKind::REAL>();
      break;
    }
    case TypeKind::DOUBLE: {
      objValue = value<TypeKind::DOUBLE>();
      break;
    }
    case TypeKind::VARCHAR: {
      objValue = value<TypeKind::VARCHAR>();
      break;
    }
    case TypeKind::OPAQUE: {
      serializeOpaque(variantObj, value<TypeKind::OPAQUE>());
      break;
    }
    case TypeKind::TIMESTAMP: {
      auto ts = value<TypeKind::TIMESTAMP>();
      objValue = -1; // Not used, but cannot be null.
      variantObj["seconds"] = ts.getSeconds();
      variantObj["nanos"] = ts.getNanos();
      break;
    }
    case TypeKind::INVALID:
      VELOX_NYI();

    default:
      VELOX_NYI();
  }

  return variantObj;
}

Variant deserializeOpaque(const folly::dynamic& variantobj) {
  auto typ = folly::parseJson(variantobj["opaque_type"].asString());
  auto opaqueType =
      std::dynamic_pointer_cast<const OpaqueType>(Type::create(typ));

  try {
    auto deserializeFunc = opaqueType->getDeserializeFunc();
    auto value = variantobj["value"].asString();
    return Variant::opaque(deserializeFunc(value), opaqueType);
  } catch (VeloxRuntimeError& ex) {
    // Re-throw error for backwards compatibility.
    // Want to return error_code::kNotImplemented rather
    // than error_code::kInvalidState
    VELOX_NYI(ex.message());
  }
}

Variant Variant::create(const folly::dynamic& variantobj) {
  TypeKind kind = mapNameToTypeKind(variantobj["type"].asString());
  const folly::dynamic& obj = variantobj["value"];

  if (obj.isNull()) {
    return Variant::null(kind);
  }
  switch (kind) {
    case TypeKind::MAP: {
      std::map<Variant, Variant> map;
      const folly::dynamic& keys = obj["keys"];
      const folly::dynamic& values = obj["values"];
      VELOX_USER_CHECK(keys.isArray() && values.isArray());
      VELOX_USER_CHECK_EQ(keys.size(), values.size());
      for (size_t idx = 0; idx < keys.size(); ++idx) {
        auto first = Variant::create(keys[idx]);
        auto second = Variant::create(values[idx]);
        map.insert(std::pair<Variant, Variant>(first, second));
      }
      return Variant::map(map);
    }
    case TypeKind::ROW:
      [[fallthrough]];
    case TypeKind::ARRAY: {
      VELOX_USER_CHECK(kind == TypeKind::ARRAY || kind == TypeKind::ROW);
      std::vector<Variant> values;
      for (auto& val : obj) {
        values.push_back(Variant::create(val));
      }
      return kind == TypeKind::ARRAY ? Variant::array(values)
                                     : Variant::row(values);
    }

    case TypeKind::VARBINARY: {
      auto str = obj.asString();
      auto result = encoding::Base64::decode(str);
      return Variant::binary(std::move(result));
    }
    case TypeKind::VARCHAR:
      return Variant::create<TypeKind::VARCHAR>(obj.asString());
    case TypeKind::TINYINT:
      return Variant::create<TypeKind::TINYINT>(obj.asInt());
    case TypeKind::SMALLINT:
      return Variant::create<TypeKind::SMALLINT>(obj.asInt());
    case TypeKind::INTEGER:
      return Variant::create<TypeKind::INTEGER>(obj.asInt());
    case TypeKind::BIGINT:
      return Variant::create<TypeKind::BIGINT>(obj.asInt());
    case TypeKind::HUGEINT:
      return Variant::create<TypeKind::HUGEINT>(obj.asInt());
    case TypeKind::BOOLEAN: {
      return Variant(obj.asBool());
    }
    case TypeKind::REAL:
      if (obj.isInt()) {
        // folly::parseJson() parses eg: "2293699590479675400"
        // to int64 instead of double, and asDouble() will throw
        // "folly::ConversionError: Loss of precision", so we do
        // the check here to make it more robust.
        return Variant::create<TypeKind::REAL>(obj.asInt());
      }
      return Variant::create<TypeKind::REAL>(obj.asDouble());
    case TypeKind::DOUBLE: {
      if (obj.isInt()) {
        return Variant::create<TypeKind::DOUBLE>(obj.asInt());
      }
      return Variant::create<TypeKind::DOUBLE>(obj.asDouble());
    }
    case TypeKind::OPAQUE: {
      return deserializeOpaque(variantobj);
    }
    case TypeKind::TIMESTAMP: {
      return Variant::create<TypeKind::TIMESTAMP>(Timestamp(
          variantobj["seconds"].asInt(), variantobj["nanos"].asInt()));
    }
    case TypeKind::INVALID:
      VELOX_NYI();

    default:
      VELOX_UNSUPPORTED(
          "specified object can not be converted to Variant: {}",
          variantobj["type"].asString());
  }
}

template <TypeKind KIND>
bool Variant::lessThan(const Variant& other) const {
  using namespace facebook::velox::util::floating_point;
  if (isNull() && !other.isNull()) {
    return true;
  }
  if (isNull() || other.isNull()) {
    return false;
  }
  using T = typename TypeTraits<KIND>::NativeType;

  if constexpr (kindCanProvideCustomComparison<KIND>::value) {
    if (usesCustomComparison_) {
      return customComparisonType<KIND>()->compare(
                 value<KIND>(), other.value<KIND>()) < 0;
    }
  }

  if constexpr (std::is_floating_point_v<T>) {
    return NaNAwareLessThan<T>{}(value<KIND>(), other.value<KIND>());
  }
  return value<KIND>() < other.value<KIND>();
}

bool Variant::operator<(const Variant& other) const {
  if (other.kind_ != this->kind_) {
    return other.kind_ < this->kind_;
  }
  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(lessThan, kind_, other);
}

template <TypeKind KIND>
bool Variant::equals(const Variant& other) const {
  using namespace facebook::velox::util::floating_point;
  if (isNull() || other.isNull()) {
    return false;
  }
  using T = typename TypeTraits<KIND>::NativeType;

  if constexpr (kindCanProvideCustomComparison<KIND>::value) {
    if (usesCustomComparison_) {
      return customComparisonType<KIND>()->compare(
                 value<KIND>(), other.value<KIND>()) == 0;
    }
  }

  if constexpr (std::is_floating_point_v<T>) {
    return NaNAwareEquals<T>{}(value<KIND>(), other.value<KIND>());
  }
  return value<KIND>() == other.value<KIND>();
}

bool Variant::equals(const Variant& other) const {
  if (other.kind_ != this->kind_) {
    return false;
  }
  if (other.isNull()) {
    return this->isNull();
  }
  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(equals, kind_, other);
}

template <TypeKind KIND>
uint64_t Variant::hash() const {
  using namespace facebook::velox::util::floating_point;
  using T = typename TypeTraits<KIND>::NativeType;

  if constexpr (kindCanProvideCustomComparison<KIND>::value) {
    if (usesCustomComparison_) {
      return customComparisonType<KIND>()->hash(value<KIND>());
    }
  }

  if constexpr (std::is_floating_point_v<T>) {
    return NaNAwareHash<T>{}(value<KIND>());
  }

  return folly::Hash{}(value<KIND>());
}

template <>
uint64_t Variant::hash<TypeKind::ARRAY>() const {
  auto& arrayVariant = value<TypeKind::ARRAY>();
  auto hasher = folly::Hash{};
  uint64_t hash = 0;
  for (int32_t i = 0; i < arrayVariant.size(); i++) {
    hash =
        folly::hash::hash_combine_generic(hasher, hash, arrayVariant[i].hash());
  }
  return hash;
}

template <>
uint64_t Variant::hash<TypeKind::ROW>() const {
  auto hasher = folly::Hash{};
  uint64_t hash = 0;
  auto& rowVariant = value<TypeKind::ROW>();
  for (int32_t i = 0; i < rowVariant.size(); i++) {
    hash =
        folly::hash::hash_combine_generic(hasher, hash, rowVariant[i].hash());
  }
  return hash;
}

template <>
uint64_t Variant::hash<TypeKind::TIMESTAMP>() const {
  auto timestampValue = value<TypeKind::TIMESTAMP>();
  return folly::Hash{}(timestampValue.getSeconds(), timestampValue.getNanos());
}

template <>
uint64_t Variant::hash<TypeKind::MAP>() const {
  auto hasher = folly::Hash{};

  const auto& mapVariant = value<TypeKind::MAP>();

  // Map is already sorted by key.
  uint64_t hash = 0;
  for (const auto& [key, value] : mapVariant) {
    hash = folly::hash::hash_combine_generic(hasher, hash, key.hash());
    hash = folly::hash::hash_combine_generic(hasher, hash, value.hash());
  }

  return hash;
}

uint64_t Variant::hash() const {
  if (isNull()) {
    return folly::Hash{}(static_cast<int32_t>(kind_));
  }

  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(hash, kind_);
}

namespace {

// Compare floating point numbers using relative epsilon comparison.
// See
// https://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/
// for details.
template <TypeKind KIND, typename TFloat>
bool equalsFloatingPointWithEpsilonTyped(const Variant& a, const Variant& b) {
  TFloat f1 = a.value<KIND>();
  TFloat f2 = b.value<KIND>();

  // Check if the numbers are all NaN value, we need to treat two NaN values to
  // be equal as well.
  if (std::isnan(f1) && std::isnan(f2)) {
    return true;
  }
  if (std::isinf(f1)) {
    // fabs(inf - inf) is indeterminate.
    return f1 == f2;
  }

  // Check if the numbers are really close -- needed
  // when comparing numbers near zero.
  if (fabs(f1 - f2) < kEpsilon) {
    return true;
  }

  TFloat largest = std::max(abs(f1), abs(f2));

  return fabs(f1 - f2) <= largest * 2 * FLT_EPSILON;
}

bool equalsFloatingPointWithEpsilon(const Variant& a, const Variant& b) {
  if (a.isNull() or b.isNull()) {
    return false;
  }

  if (a.kind() == TypeKind::REAL) {
    return equalsFloatingPointWithEpsilonTyped<TypeKind::REAL, float>(a, b);
  } else {
    VELOX_CHECK_EQ(a.kind(), TypeKind::DOUBLE);
    return equalsFloatingPointWithEpsilonTyped<TypeKind::DOUBLE, double>(a, b);
  }
}
} // namespace

bool Variant::lessThanWithEpsilon(const Variant& other) const {
  if (other.kind_ != this->kind_) {
    return other.kind_ < this->kind_;
  }
  if ((kind_ == TypeKind::REAL) or (kind_ == TypeKind::DOUBLE)) {
    if (isNull() && !other.isNull()) {
      return true;
    }
    if (isNull() || other.isNull()) {
      return false;
    }

    // If floating point values are roughly equal, then none of them is less.
    if (equalsFloatingPointWithEpsilon(*this, other)) {
      return false;
    }
    return *this < other;
  }

  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(lessThan, kind_, other);
}

namespace {

// Compare variants of Array or Row type.
template <TypeKind KIND>
bool compareComplexTypeWithEpsilon(const Variant& left, const Variant& right) {
  auto& leftContainer = left.value<KIND>();
  auto& rightContainer = right.value<KIND>();
  if (leftContainer.size() != rightContainer.size()) {
    return false;
  }
  for (int32_t i = 0; i < leftContainer.size(); i++) {
    if (!leftContainer[i].equalsWithEpsilon(rightContainer[i])) {
      return false;
    }
  }
  return true;
}

// Compare variants of Map type.
template <>
bool compareComplexTypeWithEpsilon<TypeKind::MAP>(
    const Variant& left,
    const Variant& right) {
  auto& leftMap = left.value<TypeKind::MAP>();
  auto& rightMap = right.value<TypeKind::MAP>();
  if (leftMap.size() != rightMap.size()) {
    return false;
  }
  for (auto it = leftMap.begin(); it != leftMap.end(); ++it) {
    auto otherIt = rightMap.find(it->first);
    if (otherIt == rightMap.end()) {
      return false;
    }
    if (!it->second.equalsWithEpsilon(otherIt->second)) {
      return false;
    }
  }
  return true;
}
} // namespace

// Uses kEpsilon to compare floating point types (REAL and DOUBLE).
// For testing purposes.
bool Variant::equalsWithEpsilon(const Variant& other) const {
  if (other.kind_ != this->kind_) {
    return false;
  }
  if (other.isNull() || this->isNull()) {
    return other.isNull() && this->isNull();
  }
  if ((kind_ == TypeKind::REAL) or (kind_ == TypeKind::DOUBLE)) {
    return equalsFloatingPointWithEpsilon(*this, other);
  }

  switch (kind_) {
    case TypeKind::ARRAY:
      return compareComplexTypeWithEpsilon<TypeKind::ARRAY>(*this, other);
    case TypeKind::MAP:
      return compareComplexTypeWithEpsilon<TypeKind::MAP>(*this, other);
    case TypeKind::ROW:
      return compareComplexTypeWithEpsilon<TypeKind::ROW>(*this, other);
    default:
      return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(equals, kind_, other);
  }
}

void Variant::verifyArrayElements(const std::vector<Variant>& inputs) {
  if (!inputs.empty()) {
    auto elementTypeKind = TypeKind::UNKNOWN;
    // Find the typeKind from the first non-null element.
    int i = 0;
    for (; i < inputs.size(); ++i) {
      if (!inputs[i].isNull()) {
        elementTypeKind = inputs[i].kind();
        break;
      }
    }
    // Verify that the remaining non-null elements match.
    for (; i < inputs.size(); ++i) {
      if (!inputs[i].isNull()) {
        VELOX_CHECK_EQ(
            elementTypeKind,
            inputs[i].kind(),
            "All array elements must be of the same kind");
      }
    }
  }
}

bool Variant::equalsWithNullEqualsNull(const Variant& other) const {
  if (other.kind_ != this->kind_) {
    return false;
  }
  return dispatchDynamicVariantEquality(*this, other, true);
}

TypePtr Variant::inferType() const {
  switch (kind_) {
    case TypeKind::MAP: {
      TypePtr keyType;
      TypePtr valueType;
      if (!isNull()) {
        const auto& m = map();
        for (const auto& [key, value] : m) {
          if (keyType == nullptr && !key.isNull()) {
            keyType = key.inferType();
          }
          if (valueType == nullptr && !value.isNull()) {
            valueType = value.inferType();
          }
          if (keyType && valueType) {
            break;
          }
        }
      }
      return MAP(
          keyType ? keyType : UNKNOWN(), valueType ? valueType : UNKNOWN());
    }
    case TypeKind::ROW: {
      std::vector<TypePtr> children;
      if (!isNull()) {
        const auto& r = row();
        children.reserve(r.size());
        for (auto& v : r) {
          children.push_back(v.inferType());
        }
      }
      return ROW(std::move(children));
    }
    case TypeKind::ARRAY: {
      TypePtr elementType = UNKNOWN();
      if (!isNull()) {
        const auto& a = array();
        if (!a.empty()) {
          elementType = a.at(0).inferType();
        }
      }
      return ARRAY(std::move(elementType));
    }
    case TypeKind::OPAQUE: {
      return value<TypeKind::OPAQUE>().type;
    }
    case TypeKind::UNKNOWN: {
      return UNKNOWN();
    }
    default:
      return createScalarType(kind_);
  }
}

bool Variant::isTypeCompatible(const TypePtr& type) const {
  if (kind_ == TypeKind::UNKNOWN) {
    return true;
  }

  if (kind_ != type->kind()) {
    return false;
  }

  switch (kind_) {
    case TypeKind::ARRAY: {
      if (!isNull()) {
        const auto& a = array();
        if (!a.empty()) {
          if (!a[0].isTypeCompatible(type->childAt(0))) {
            return false;
          }
        }
      }

      return true;
    }
    case TypeKind::MAP: {
      if (!isNull()) {
        bool keyTypeChecked = false;
        bool valueTypeChecked = false;

        const auto& m = map();
        for (auto& [key, value] : m) {
          if (!keyTypeChecked && !key.isNull()) {
            if (!key.isTypeCompatible(type->childAt(0))) {
              return false;
            }
            keyTypeChecked = true;
          }
          if (!valueTypeChecked && !value.isNull()) {
            if (!value.isTypeCompatible(type->childAt(1))) {
              return false;
            }
            valueTypeChecked = true;
          }

          if (keyTypeChecked && valueTypeChecked) {
            break;
          }
        }
      }

      return true;
    }
    case TypeKind::ROW: {
      if (!isNull()) {
        const auto& r = row();
        if (r.size() != type->size()) {
          return false;
        }

        for (auto i = 0; i < r.size(); ++i) {
          if (!r[i].isTypeCompatible(type->childAt(i))) {
            return false;
          }
        }
      }

      return true;
    }
    default:
      return true;
  }
}

} // namespace facebook::velox

namespace folly {

// For opaque values, we hash the shared_ptr<void> which will hash the
// underlying pointer.
template <>
struct hasher<facebook::velox::detail::OpaqueCapsule> {
  size_t operator()(const facebook::velox::detail::OpaqueCapsule& key) const {
    return Hash()(key.obj);
  }
};

} // namespace folly
