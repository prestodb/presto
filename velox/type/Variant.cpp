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
#include "common/encode/Base64.h"
#include "folly/json.h"

namespace facebook::velox {

namespace {
folly::json::serialization_opts& getOpts() {
  static folly::json::serialization_opts opts;
  opts.sort_keys = true;
  return opts;
}
} // namespace

template <bool nullEqualsNull>
bool evaluateNullEquality(const variant& a, const variant& b) {
  if constexpr (nullEqualsNull) {
    if (a.isNull() && b.isNull()) {
      return true;
    }
  }
  return false;
}

// scalars
template <TypeKind KIND>
struct VariantEquality {
  template <bool NullEqualsNull>
  static bool equals(const variant& a, const variant& b) {
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
  static bool equals(const variant& a, const variant& b) {
    if (a.isNull() || b.isNull()) {
      return evaluateNullEquality<NullEqualsNull>(a, b);
    } else {
      return a.value<TypeKind::TIMESTAMP>() == b.value<TypeKind::TIMESTAMP>();
    }
  }
};

// date
template <>
struct VariantEquality<TypeKind::DATE> {
  template <bool NullEqualsNull>
  static bool equals(const variant& a, const variant& b) {
    if (a.isNull() || b.isNull()) {
      return evaluateNullEquality<NullEqualsNull>(a, b);
    } else {
      return a.value<TypeKind::DATE>() == b.value<TypeKind::DATE>();
    }
  }
};

// interval day time
template <>
struct VariantEquality<TypeKind::INTERVAL_DAY_TIME> {
  template <bool NullEqualsNull>
  static bool equals(const variant& a, const variant& b) {
    if (a.isNull() || b.isNull()) {
      return evaluateNullEquality<NullEqualsNull>(a, b);
    } else {
      return a.value<TypeKind::INTERVAL_DAY_TIME>() ==
          b.value<TypeKind::INTERVAL_DAY_TIME>();
    }
  }
};

// array
template <>
struct VariantEquality<TypeKind::ARRAY> {
  template <bool NullEqualsNull>
  static bool equals(const variant& a, const variant& b) {
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
  static bool equals(const variant& a, const variant& b) {
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
  static bool equals(const variant& a, const variant& b) {
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
    const variant& a,
    const variant& b,
    const bool& enableNullEqualsNull) {
  if (enableNullEqualsNull) {
    return VELOX_DYNAMIC_TYPE_DISPATCH_METHOD(
        VariantEquality, equals<true>, a.kind(), a, b);
  }
  return VELOX_DYNAMIC_TYPE_DISPATCH_METHOD(
      VariantEquality, equals<false>, a.kind(), a, b);
}

void variant::throwCheckIsKindError(TypeKind kind) const {
  throw std::invalid_argument{fmt::format(
      "wrong kind! {} != {}",
      mapTypeKindToName(kind_),
      mapTypeKindToName(kind))};
}

void variant::throwCheckNotNullError() const {
  throw std::invalid_argument{"it's null!"};
}

std::string variant::toJson() const {
  // todo(youknowjack): consistent story around std::stringifying, converting,
  // and other basic operations. Stringification logic should not be specific
  // to variants; it should be consistent for all map representations

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
        b += pair.first.toJson();
        b += ",\"value\":";
        b += pair.second.toJson();
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
        b += v.toJson();
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
        b += v.toJson();
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
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::BOOLEAN: {
      auto converted = VariantConverter::convert<TypeKind::VARCHAR>(*this);
      if (converted.isNull()) {
        return "null";
      } else {
        return converted.value<TypeKind::VARCHAR>();
      }
    }
    case TypeKind::REAL: {
      auto val = value<TypeKind::REAL>();
      if (std::isnormal(val)) {
        return folly::to<std::string>(val);
      } else {
        return '"' + folly::to<std::string>(val) + '"';
      }
    }
    case TypeKind::DOUBLE: {
      auto val = value<TypeKind::DOUBLE>();
      if (std::isnormal(val)) {
        return folly::to<std::string>(val);
      } else {
        return '"' + folly::to<std::string>(val) + '"';
      }
    }
    case TypeKind::TIMESTAMP: {
      auto& timestamp = value<TypeKind::TIMESTAMP>();
      return '"' + timestamp.toString() + '"';
    }
    case TypeKind::DATE: {
      auto& date = value<TypeKind::DATE>();
      return '"' + date.toString() + '"';
    }
    case TypeKind::INTERVAL_DAY_TIME: {
      auto& interval = value<TypeKind::INTERVAL_DAY_TIME>();
      return '"' + interval.toString() + '"';
    }
    case TypeKind::OPAQUE: {
      // Return expression that we can't parse back - we use toJson for
      // debugging only. Variant::serialize should actually serialize the data.
      return "\"Opaque<" + value<TypeKind::OPAQUE>().type->toString() + ">\"";
    }
    case TypeKind::SHORT_DECIMAL:
    case TypeKind::LONG_DECIMAL:
    case TypeKind::FUNCTION:
    case TypeKind::UNKNOWN:
    case TypeKind::INVALID:
      VELOX_NYI();
  }

  VELOX_UNSUPPORTED(
      "Unsupported: given type {} is not json-ready", mapTypeKindToName(kind_));
}

folly::dynamic variant::serialize() const {
  folly::dynamic variantObj = folly::dynamic::object;

  variantObj["type"] = std::string(
      VELOX_STATIC_FIELD_DYNAMIC_DISPATCH_ALL(TypeTraits, name, kind_));
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
      auto& row = value<TypeKind::ARRAY>();
      folly::dynamic arr = folly::dynamic::array;
      for (auto& v : row) {
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

    case TypeKind::DATE:
    case TypeKind::INTERVAL_DAY_TIME:
    case TypeKind::TIMESTAMP:
    case TypeKind::INVALID:
      VELOX_NYI();

    case TypeKind::OPAQUE:
      VELOX_NYI(
          "Opaque types serialization is potentially possible but not implemented yet");

    default:
      VELOX_NYI();
  }

  return variantObj;
}

variant variant::create(const folly::dynamic& variantobj) {
  TypeKind kind = mapNameToTypeKind(variantobj["type"].asString());
  const folly::dynamic& obj = variantobj["value"];

  if (obj.isNull()) {
    return variant::null(kind);
  }
  switch (kind) {
    case TypeKind::MAP: {
      std::map<variant, variant> map;
      const folly::dynamic& keys = obj["keys"];
      const folly::dynamic& values = obj["values"];
      VELOX_USER_CHECK(keys.isArray() && values.isArray());
      VELOX_USER_CHECK_EQ(keys.size(), values.size());
      for (size_t idx = 0; idx < keys.size(); ++idx) {
        auto first = variant::create(keys[idx]);
        auto second = variant::create(values[idx]);
        map.insert(std::pair<variant, variant>(first, second));
      }
      return variant::map(map);
    }
    case TypeKind::ROW:
      FOLLY_FALLTHROUGH;
    case TypeKind::ARRAY: {
      VELOX_USER_CHECK(kind == TypeKind::ARRAY || kind == TypeKind::ROW);
      std::vector<variant> values;
      for (auto& val : obj) {
        values.push_back(variant::create(val));
      }
      return kind == TypeKind::ARRAY ? variant::array(values)
                                     : variant::row(values);
    }

    case TypeKind::VARBINARY: {
      auto str = obj.asString();
      auto result = encoding::Base64::decode(str);
      return variant::binary(std::move(result));
    }
    case TypeKind::VARCHAR:
      return variant::create<TypeKind::VARCHAR>(obj.asString());
    case TypeKind::TINYINT:
      return variant::create<TypeKind::TINYINT>(obj.asInt());
    case TypeKind::SMALLINT:
      return variant::create<TypeKind::SMALLINT>(obj.asInt());
    case TypeKind::INTEGER:
      return variant::create<TypeKind::INTEGER>(obj.asInt());
    case TypeKind::BIGINT:
      return variant::create<TypeKind::BIGINT>(obj.asInt());
    case TypeKind::BOOLEAN: {
      return variant(obj.asBool());
    }
    case TypeKind::REAL:
      if (obj.isInt()) {
        // folly::parseJson() parses eg: "2293699590479675400"
        // to int64 instead of double, and asDouble() will throw
        // "folly::ConversionError: Loss of precision", so we do
        // the check here to make it more robust.
        return variant::create<TypeKind::REAL>(obj.asInt());
      }
      return variant::create<TypeKind::REAL>(obj.asDouble());
    case TypeKind::DOUBLE: {
      if (obj.isInt()) {
        return variant::create<TypeKind::DOUBLE>(obj.asInt());
      }
      return variant::create<TypeKind::DOUBLE>(obj.asDouble());
    }
    case TypeKind::DATE:
    case TypeKind::INTERVAL_DAY_TIME:
    case TypeKind::TIMESTAMP:
      FOLLY_FALLTHROUGH;
    case TypeKind::INVALID:
      VELOX_NYI();

    default:
      VELOX_UNSUPPORTED(
          "specified object can not be converted to variant ",
          variantobj["type"].asString());
  }
}

uint64_t variant::hash() const {
  uint64_t hash = 0;
  if (isNull()) {
    return folly::Hash{}(static_cast<int32_t>(kind_));
  }

  switch (kind_) {
    case TypeKind::BIGINT:
      return folly::Hash{}(value<TypeKind::BIGINT>());
    case TypeKind::INTEGER:
      return folly::Hash{}(value<TypeKind::INTEGER>());
    case TypeKind::SMALLINT:
      return folly::Hash{}(value<TypeKind::SMALLINT>());
    case TypeKind::TINYINT:
      return folly::Hash{}(value<TypeKind::TINYINT>());
    case TypeKind::BOOLEAN:
      return folly::Hash{}(value<TypeKind::BOOLEAN>());
    case TypeKind::REAL:
      return folly::Hash{}(value<TypeKind::REAL>());
    case TypeKind::DOUBLE:
      return folly::Hash{}(value<TypeKind::DOUBLE>());
    case TypeKind::VARBINARY:
      return folly::Hash{}(value<TypeKind::VARBINARY>());
    case TypeKind::VARCHAR:
      return folly::Hash{}(value<TypeKind::VARCHAR>());
    case TypeKind::ARRAY: {
      auto& arrayVariant = value<TypeKind::ARRAY>();
      auto hasher = folly::Hash{};
      for (int32_t i = 0; i < arrayVariant.size(); i++) {
        hash = folly::hash::hash_combine_generic(
            hasher, hash, arrayVariant[i].hash());
      }
      return hash;
    }
    case TypeKind::ROW: {
      auto hasher = folly::Hash{};
      auto& rowVariant = value<TypeKind::ROW>();
      for (int32_t i = 0; i < rowVariant.size(); i++) {
        hash = folly::hash::hash_combine_generic(
            hasher, hash, rowVariant[i].hash());
      }
      return hash;
    }
    case TypeKind::DATE: {
      auto dateValue = value<TypeKind::DATE>();
      return folly::Hash{}(dateValue.days());
    }
    case TypeKind::INTERVAL_DAY_TIME: {
      auto interval = value<TypeKind::INTERVAL_DAY_TIME>();
      return folly::Hash{}(interval.milliseconds());
    }
    case TypeKind::TIMESTAMP: {
      auto timestampValue = value<TypeKind::TIMESTAMP>();
      return folly::Hash{}(
          timestampValue.getSeconds(), timestampValue.getNanos());
    }

    case TypeKind::MAP: {
      auto hasher = folly::Hash{};
      auto& mapVariant = value<TypeKind::MAP>();
      uint64_t combinedKeyHash = 0, combinedValueHash = 0;
      uint64_t singleKeyHash = 0, singleValueHash = 0;
      for (auto it = mapVariant.begin(); it != mapVariant.end(); ++it) {
        singleKeyHash = it->first.hash();
        singleValueHash = it->second.hash();
        combinedKeyHash = folly::hash::commutative_hash_combine_value_generic(
            combinedKeyHash, hasher, singleKeyHash);
        combinedValueHash = folly::hash::commutative_hash_combine_value_generic(
            combinedValueHash, hasher, singleValueHash);
      }

      return folly::hash::hash_combine_generic(
          folly::Hash{}, combinedKeyHash, combinedValueHash);
    }
    default:
      VELOX_NYI();
  }
}

/*static*/ bool variant::equalsFloatingPointWithEpsilon(
    const variant& a,
    const variant& b) {
  if (a.isNull() or b.isNull()) {
    return false;
  }
  if (a.kind_ == TypeKind::REAL) {
    return fabs(a.value<TypeKind::REAL>() - b.value<TypeKind::REAL>()) <
        kEpsilon;
  }
  return fabs(a.value<TypeKind::DOUBLE>() - b.value<TypeKind::DOUBLE>()) <
      kEpsilon;
}

bool variant::lessThanWithEpsilon(const variant& other) const {
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

  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(lessThan, kind_, *this, other);
}

// Uses kEpsilon to compare floating point types (REAL and DOUBLE).
// For testing purposes.
bool variant::equalsWithEpsilon(const variant& other) const {
  if (other.kind_ != this->kind_) {
    return false;
  }
  if (other.isNull()) {
    return this->isNull();
  }
  if ((kind_ == TypeKind::REAL) or (kind_ == TypeKind::DOUBLE)) {
    return equalsFloatingPointWithEpsilon(*this, other);
  }

  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(equals, kind_, *this, other);
}

} // namespace facebook::velox
