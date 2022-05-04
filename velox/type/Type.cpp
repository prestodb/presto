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

#include "velox/type/Type.h"

#include <sstream>
#include <typeindex>

#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>

#include <folly/Demangle.h>

#include "velox/common/base/Exceptions.h"

namespace std {
template <>
struct hash<facebook::velox::TypeKind> {
  size_t operator()(const facebook::velox::TypeKind& typeKind) const {
    return std::hash<int32_t>()((int32_t)typeKind);
  }
};
} // namespace std

namespace {
bool isColumnNameRequiringEscaping(const std::string& name) {
  static const boost::regex re("^[a-zA-Z_][a-zA-Z0-9_]*$");
  return !regex_match(name, re);
}
} // namespace

namespace facebook::velox {

// Static variable intialization is not thread safe for non
// constant-initialization, but scoped static initialization is thread safe.
const std::unordered_map<std::string, TypeKind>& getTypeStringMap() {
  static const std::unordered_map<std::string, TypeKind> kTypeStringMap{
      {"BOOLEAN", TypeKind::BOOLEAN},
      {"TINYINT", TypeKind::TINYINT},
      {"SMALLINT", TypeKind::SMALLINT},
      {"INTEGER", TypeKind::INTEGER},
      {"BIGINT", TypeKind::BIGINT},
      {"REAL", TypeKind::REAL},
      {"DOUBLE", TypeKind::DOUBLE},
      {"VARCHAR", TypeKind::VARCHAR},
      {"VARBINARY", TypeKind::VARBINARY},
      {"TIMESTAMP", TypeKind::TIMESTAMP},
      {"DATE", TypeKind::DATE},
      {"ARRAY", TypeKind::ARRAY},
      {"MAP", TypeKind::MAP},
      {"ROW", TypeKind::ROW},
      {"FUNCTION", TypeKind::FUNCTION},
      {"UNKNOWN", TypeKind::UNKNOWN},
      {"OPAQUE", TypeKind::OPAQUE},
      {"INVALID", TypeKind::INVALID}};
  return kTypeStringMap;
}

std::optional<TypeKind> tryMapNameToTypeKind(const std::string& name) {
  auto found = getTypeStringMap().find(name);

  if (found == getTypeStringMap().end()) {
    return std::nullopt;
  }

  return found->second;
}

TypeKind mapNameToTypeKind(const std::string& name) {
  auto found = getTypeStringMap().find(name);

  if (found == getTypeStringMap().end()) {
    VELOX_USER_FAIL("Specified element is not found : {}", name);
  }

  return found->second;
}

std::string mapTypeKindToName(const TypeKind& typeKind) {
  static std::unordered_map<TypeKind, std::string> typeEnumMap{
      {TypeKind::BOOLEAN, "BOOLEAN"},
      {TypeKind::TINYINT, "TINYINT"},
      {TypeKind::SMALLINT, "SMALLINT"},
      {TypeKind::INTEGER, "INTEGER"},
      {TypeKind::BIGINT, "BIGINT"},
      {TypeKind::REAL, "REAL"},
      {TypeKind::DOUBLE, "DOUBLE"},
      {TypeKind::VARCHAR, "VARCHAR"},
      {TypeKind::VARBINARY, "VARBINARY"},
      {TypeKind::TIMESTAMP, "TIMESTAMP"},
      {TypeKind::DATE, "DATE"},
      {TypeKind::ARRAY, "ARRAY"},
      {TypeKind::MAP, "MAP"},
      {TypeKind::ROW, "ROW"},
      {TypeKind::FUNCTION, "FUNCTION"},
      {TypeKind::UNKNOWN, "UNKNOWN"},
      {TypeKind::OPAQUE, "OPAQUE"},
      {TypeKind::INVALID, "INVALID"}};

  auto found = typeEnumMap.find(typeKind);

  if (found == typeEnumMap.end()) {
    VELOX_USER_FAIL("Specified element is not found : {}", (int32_t)typeKind);
  }

  return found->second;
}

namespace {
struct OpaqueSerdeRegistry {
  struct Entry {
    std::string persistentName;
    // to avoid creating new shared_ptr's every time
    OpaqueType::SerializeFunc<void> serialize;
    OpaqueType::DeserializeFunc<void> deserialize;
  };
  std::unordered_map<std::type_index, Entry> mapping;
  std::unordered_map<std::string, std::shared_ptr<const OpaqueType>> reverse;

  static OpaqueSerdeRegistry& get() {
    static OpaqueSerdeRegistry instance;
    return instance;
  }
};
} // namespace

std::ostream& operator<<(std::ostream& os, const TypeKind& kind) {
  os << mapTypeKindToName(kind);
  return os;
}

std::shared_ptr<const Type> Type::create(const folly::dynamic& obj) {
  TypeKind type = mapNameToTypeKind(obj["type"].asString());
  switch (type) {
    case TypeKind::ROW: {
      VELOX_USER_CHECK(obj["names"].isArray());
      std::vector<std::string> names;
      for (const auto& name : obj["names"]) {
        names.push_back(name.asString());
      }

      auto child =
          velox::ISerializable::deserialize<std::vector<Type>>(obj["cTypes"]);
      return std::make_shared<const RowType>(move(names), move(child));
    }

    case TypeKind::OPAQUE: {
      const auto& persistentName = obj["opaque"].asString();
      const auto& registry = OpaqueSerdeRegistry::get();
      auto it = registry.reverse.find(persistentName);
      VELOX_USER_CHECK(
          it != registry.reverse.end(),
          "Opaque type with persistent name '{}' is not registered",
          persistentName);
      if (auto withExtra = it->second->deserializeExtra(obj)) {
        return withExtra;
      }
      return it->second;
    }

    default: {
      std::vector<std::shared_ptr<const Type>> childTypes;
      if (obj.find("cTypes") != obj.items().end()) {
        childTypes =
            velox::ISerializable::deserialize<std::vector<Type>>(obj["cTypes"]);
      }
      return createType(type, move(childTypes));
    }
  }
}

std::string ArrayType::toString() const {
  return "ARRAY<" + child_->toString() + ">";
}

const std::shared_ptr<const Type>& ArrayType::childAt(uint32_t idx) const {
  VELOX_USER_CHECK_EQ(idx, 0, "List type should have only one child");
  return elementType();
}

ArrayType::ArrayType(std::shared_ptr<const Type> child)
    : child_{std::move(child)} {}

bool ArrayType::operator==(const Type& other) const {
  if (&other == this) {
    return true;
  }
  if (!other.isArray()) {
    return false;
  }
  auto& otherArray = other.asArray();
  return *child_ == *otherArray.child_;
}

folly::dynamic ArrayType::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "Type";
  obj["type"] = TypeTraits<TypeKind::ARRAY>::name;

  folly::dynamic children = folly::dynamic::array;
  children.push_back(child_->serialize());
  obj["cTypes"] = children;
  ;
  return obj;
}

FixedSizeArrayType::FixedSizeArrayType(
    FixedSizeArrayType::size_type len,
    std::shared_ptr<const Type> child)
    : ArrayType(child), len_(len) {}

std::string FixedSizeArrayType::toString() const {
  std::stringstream ss;
  ss << "FIXED_SIZE_ARRAY(" << len_ << ")<" << child_->toString() << ">";
  return ss.str();
}

const std::shared_ptr<const Type>& MapType::childAt(uint32_t idx) const {
  if (idx == 0) {
    return keyType();
  } else if (idx == 1) {
    return valueType();
  }
  VELOX_USER_FAIL(
      "Map type should have only two children. Tried to access child '{}'",
      idx);
}

MapType::MapType(
    std::shared_ptr<const Type> keyType,
    std::shared_ptr<const Type> valueType)
    : keyType_{std::move(keyType)}, valueType_{std::move(valueType)} {}

std::string MapType::toString() const {
  return "MAP<" + keyType()->toString() + "," + valueType()->toString() + ">";
}

folly::dynamic MapType::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "Type";
  obj["type"] = TypeTraits<TypeKind::MAP>::name;

  folly::dynamic children = folly::dynamic::array;
  children.push_back(keyType()->serialize());
  children.push_back(valueType()->serialize());
  obj["cTypes"] = children;

  return obj;
}

RowType::RowType(
    std::vector<std::string>&& names,
    std::vector<std::shared_ptr<const Type>>&& types)
    : names_{std::move(names)}, children_{std::move(types)} {
  VELOX_USER_CHECK_EQ(
      names_.size(), children_.size(), "Mismatch names/types sizes");
}

uint32_t RowType::size() const {
  return children_.size();
}

const std::shared_ptr<const Type>& RowType::childAt(uint32_t idx) const {
  return children_.at(idx);
}

const std::shared_ptr<const Type>& RowType::findChild(
    folly::StringPiece name) const {
  for (uint32_t i = 0; i < names_.size(); ++i) {
    if (names_.at(i) == name) {
      return children_.at(i);
    }
  }
  VELOX_USER_FAIL("Field not found: {}", name);
}

bool RowType::containsChild(std::string_view name) const {
  return std::find(names_.begin(), names_.end(), name) != names_.end();
}

uint32_t RowType::getChildIdx(const std::string& name) const {
  auto index = getChildIdxIfExists(name);
  VELOX_USER_CHECK(index.has_value(), "Field not found: {}", name);
  return index.value();
}

std::optional<uint32_t> RowType::getChildIdxIfExists(
    const std::string& name) const {
  for (uint32_t i = 0; i < names_.size(); i++) {
    if (names_.at(i) == name) {
      return i;
    }
  }
  return std::nullopt;
}

bool RowType::operator==(const Type& other) const {
  if (&other == this) {
    return true;
  }
  if (other.kind() != TypeKind::ROW) {
    return false;
  }
  auto& otherTyped = other.asRow();
  if (otherTyped.size() != size()) {
    return false;
  }
  for (size_t i = 0; i < size(); ++i) {
    // todo: case sensitivity
    if (nameOf(i) != otherTyped.nameOf(i)) {
      return false;
    }
    if (*childAt(i) != *otherTyped.childAt(i)) {
      return false;
    }
  }
  return true;
}

void RowType::printChildren(std::stringstream& ss, std::string_view delimiter)
    const {
  bool any = false;
  for (size_t i = 0; i < children_.size(); ++i) {
    if (any) {
      ss << delimiter;
    }
    const auto& name = names_.at(i);
    if (isColumnNameRequiringEscaping(name)) {
      ss << std::quoted(name, '"', '"');
    } else {
      ss << name;
    }
    ss << ':' << children_.at(i)->toString();
    any = true;
  }
}

std::string RowType::toString() const {
  std::stringstream ss;
  ss << (TypeTraits<TypeKind::ROW>::name) << "<";
  printChildren(ss);
  ss << ">";
  return ss.str();
}

folly::dynamic RowType::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "Type";
  obj["type"] = TypeTraits<TypeKind::ROW>::name;
  obj["names"] = velox::ISerializable::serialize(names_);
  obj["cTypes"] = velox::ISerializable::serialize(children_);
  return obj;
}

size_t Type::hashKind() const {
  size_t hash = (int32_t)kind();
  for (auto& child : *this) {
    hash = hash * 31 + child->hashKind();
  }
  return hash;
}

bool Type::kindEquals(const std::shared_ptr<const Type>& other) const {
  // recursive kind match (ignores names)
  if (this->kind() != other->kind()) {
    return false;
  }
  if (this->size() != other->size()) {
    return false;
  }
  for (size_t i = 0; i < this->size(); ++i) {
    if (!this->childAt(i)->kindEquals(other->childAt(i))) {
      return false;
    }
  }
  return true;
}

bool MapType::operator==(const Type& other) const {
  if (&other == this) {
    return true;
  }
  if (!other.isMap()) {
    return false;
  }
  auto& otherMap = other.asMap();
  return *keyType_ == *otherMap.keyType_ && *valueType_ == *otherMap.valueType_;
}

bool FunctionType::operator==(const Type& other) const {
  if (&other == this) {
    return true;
  }
  if (other.kind() != TypeKind::FUNCTION) {
    return false;
  }
  auto& otherTyped = *reinterpret_cast<const FunctionType*>(&other);
  return children_ == otherTyped.children_;
}

std::string FunctionType::toString() const {
  std::stringstream out;
  out << "FUNCTION<";
  for (auto i = 0; i < children_.size(); ++i) {
    out << children_[i]->toString() << (i == children_.size() - 1 ? "" : ", ");
  }
  out << ">";
  return out.str();
}

folly::dynamic FunctionType::serialize() const {
  throw std::logic_error("FUNCTION type is not serializable");
}

OpaqueType::OpaqueType(const std::type_index& typeIndex)
    : typeIndex_(typeIndex) {}

bool OpaqueType::operator==(const Type& other) const {
  if (&other == this) {
    return true;
  }
  if (other.kind() != TypeKind::OPAQUE) {
    return false;
  }
  auto& otherTyped = *reinterpret_cast<const OpaqueType*>(&other);
  return typeIndex_ == otherTyped.typeIndex_;
}

std::string OpaqueType::toString() const {
  std::stringstream out;
  out << "OPAQUE<" << folly::demangle(typeIndex_.name()) << ">";
  return out.str();
}

folly::dynamic OpaqueType::serialize() const {
  const auto& registry = OpaqueSerdeRegistry::get();
  auto it = registry.mapping.find(typeIndex_);
  VELOX_CHECK(
      it != registry.mapping.end(),
      "No serialization persistent name registered for {}",
      toString());

  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "Type";
  obj["type"] = TypeTraits<TypeKind::OPAQUE>::name;
  obj["opaque"] = it->second.persistentName;
  return obj;
}

OpaqueType::SerializeFunc<void> OpaqueType::getSerializeFunc() const {
  const auto& registry = OpaqueSerdeRegistry::get();
  auto it = registry.mapping.find(typeIndex_);
  VELOX_CHECK(
      it != registry.mapping.end() && it->second.serialize,
      "No serialization function registered for {}",
      toString());
  return it->second.serialize;
}

OpaqueType::DeserializeFunc<void> OpaqueType::getDeserializeFunc() const {
  const auto& registry = OpaqueSerdeRegistry::get();
  auto it = registry.mapping.find(typeIndex_);
  VELOX_CHECK(
      it != registry.mapping.end() && it->second.deserialize,
      "No deserialization function registered for {}",
      toString());
  return it->second.deserialize;
}

std::shared_ptr<const OpaqueType> OpaqueType::deserializeExtra(
    const folly::dynamic&) const {
  return nullptr;
}

void OpaqueType::registerSerializationTypeErased(
    const std::shared_ptr<const OpaqueType>& type,
    const std::string& persistentName,
    SerializeFunc<void> serialize,
    DeserializeFunc<void> deserialize) {
  auto& registry = OpaqueSerdeRegistry::get();
  VELOX_CHECK(
      !registry.mapping.count(type->typeIndex_),
      "Trying to register duplicated serialization information for type {}",
      type->toString());
  VELOX_CHECK(
      !registry.reverse.count(persistentName),
      "Trying to register duplicated persistent type name '{}' for type {}, "
      "it's already taken by type {}",
      persistentName,
      type->toString(),
      registry.reverse.at(persistentName)->toString());
  registry.mapping[type->typeIndex_] = {
      .persistentName = persistentName,
      .serialize = serialize,
      .deserialize = deserialize};
  registry.reverse[persistentName] = type;
}

std::shared_ptr<const ArrayType> ARRAY(
    std::shared_ptr<const Type> elementType) {
  return std::make_shared<const ArrayType>(std::move(elementType));
}

std::shared_ptr<const FixedSizeArrayType> FIXED_SIZE_ARRAY(
    FixedSizeArrayType::size_type len,
    std::shared_ptr<const Type> elementType) {
  return std::make_shared<const FixedSizeArrayType>(
      len, std::move(elementType));
}

std::shared_ptr<const RowType> ROW(
    std::vector<std::string>&& names,
    std::vector<std::shared_ptr<const Type>>&& types) {
  return TypeFactory<TypeKind::ROW>::create(std::move(names), std::move(types));
}

std::shared_ptr<const RowType> ROW(
    std::initializer_list<
        std::pair<const std::string, std::shared_ptr<const Type>>>&& pairs) {
  std::vector<std::shared_ptr<const Type>> types;
  std::vector<std::string> names;
  types.reserve(pairs.size());
  names.reserve(pairs.size());
  for (auto& p : pairs) {
    types.push_back(p.second);
    names.push_back(p.first);
  }
  return TypeFactory<TypeKind::ROW>::create(std::move(names), std::move(types));
}

std::shared_ptr<const RowType> ROW(
    std::vector<std::shared_ptr<const Type>>&& types) {
  std::vector<std::string> names;
  names.reserve(types.size());
  for (auto& p : types) {
    names.push_back("");
  }
  return TypeFactory<TypeKind::ROW>::create(std::move(names), std::move(types));
}

std::shared_ptr<const MapType> MAP(
    std::shared_ptr<const Type> keyType,
    std::shared_ptr<const Type> valType) {
  return std::make_shared<const MapType>(
      std::move(keyType), std::move(valType));
};

std::shared_ptr<const FunctionType> FUNCTION(
    std::vector<std::shared_ptr<const Type>>&& argumentTypes,
    std::shared_ptr<const Type> returnType) {
  return std::make_shared<const FunctionType>(
      std::move(argumentTypes), std::move(returnType));
};

#define KOSKI_DEFINE_SCALAR_ACCESSOR(KIND)                   \
  std::shared_ptr<const ScalarType<TypeKind::KIND>> KIND() { \
    return ScalarType<TypeKind::KIND>::create();             \
  }

KOSKI_DEFINE_SCALAR_ACCESSOR(INTEGER);
KOSKI_DEFINE_SCALAR_ACCESSOR(BOOLEAN);
KOSKI_DEFINE_SCALAR_ACCESSOR(TINYINT);
KOSKI_DEFINE_SCALAR_ACCESSOR(SMALLINT);
KOSKI_DEFINE_SCALAR_ACCESSOR(BIGINT);
KOSKI_DEFINE_SCALAR_ACCESSOR(REAL);
KOSKI_DEFINE_SCALAR_ACCESSOR(DOUBLE);
KOSKI_DEFINE_SCALAR_ACCESSOR(TIMESTAMP);
KOSKI_DEFINE_SCALAR_ACCESSOR(VARCHAR);
KOSKI_DEFINE_SCALAR_ACCESSOR(VARBINARY);
KOSKI_DEFINE_SCALAR_ACCESSOR(DATE);
KOSKI_DEFINE_SCALAR_ACCESSOR(UNKNOWN);

#undef KOSKI_DEFINE_SCALAR_ACCESSOR

std::shared_ptr<const Type> createScalarType(TypeKind kind) {
  return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(createScalarType, kind);
}

std::shared_ptr<const Type> createType(
    TypeKind kind,
    std::vector<std::shared_ptr<const Type>>&& children) {
  return VELOX_DYNAMIC_TYPE_DISPATCH(createType, kind, std::move(children));
}

template <>
std::shared_ptr<const Type> createType<TypeKind::ROW>(
    std::vector<std::shared_ptr<const Type>>&& /*children*/) {
  std::string name{TypeTraits<TypeKind::ROW>::name};
  VELOX_USER_FAIL("Not supported for kind: {}", name);
}

template <>
std::shared_ptr<const Type> createType<TypeKind::ARRAY>(
    std::vector<std::shared_ptr<const Type>>&& children) {
  VELOX_USER_CHECK_EQ(children.size(), 1, "ARRAY should have only one child");
  return ARRAY(children.at(0));
}

template <>
std::shared_ptr<const Type> createType<TypeKind::MAP>(
    std::vector<std::shared_ptr<const Type>>&& children) {
  VELOX_USER_CHECK_EQ(children.size(), 2, "MAP should have only two children");
  return MAP(children.at(0), children.at(1));
}

template <>
std::shared_ptr<const Type> createType<TypeKind::OPAQUE>(
    std::vector<std::shared_ptr<const Type>>&& /*children*/) {
  std::string name{TypeTraits<TypeKind::OPAQUE>::name};
  VELOX_USER_FAIL("Not supported for kind: {}", name);
}

bool Type::containsUnknown() const {
  if (kind_ == TypeKind::UNKNOWN) {
    return true;
  }
  for (auto i = 0; i < size(); ++i) {
    if (childAt(i)->containsUnknown()) {
      return true;
    }
  }
  return false;
}

namespace {

std::unordered_map<std::string, std::unique_ptr<const CustomTypeFactories>>&
typeFactories() {
  static std::
      unordered_map<std::string, std::unique_ptr<const CustomTypeFactories>>
          factories;
  return factories;
}

} // namespace

void registerType(
    const std::string& name,
    std::unique_ptr<const CustomTypeFactories> factories) {
  auto uppercaseName = boost::algorithm::to_upper_copy(name);
  typeFactories().emplace(uppercaseName, std::move(factories));
}

bool typeExists(const std::string& name) {
  auto uppercaseName = boost::algorithm::to_upper_copy(name);
  return typeFactories().count(uppercaseName) > 0;
}

const CustomTypeFactories* FOLLY_NULLABLE
getTypeFactories(const std::string& name) {
  auto uppercaseName = boost::algorithm::to_upper_copy(name);
  auto it = typeFactories().find(uppercaseName);

  if (it != typeFactories().end()) {
    return it->second.get();
  }

  return nullptr;
}

TypePtr getType(const std::string& name, std::vector<TypePtr> childTypes) {
  auto factories = getTypeFactories(name);
  if (factories) {
    return factories->getType(std::move(childTypes));
  }

  return nullptr;
}

exec::CastOperatorPtr getCastOperator(const std::string& name) {
  auto factories = getTypeFactories(name);
  if (factories) {
    return factories->getCastOperator();
  }

  return nullptr;
}

} // namespace facebook::velox
