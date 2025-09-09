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
#include "velox/core/Expressions.h"
#include "velox/common/Casts.h"
#include "velox/common/encode/Base64.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/VectorSaver.h"

namespace facebook::velox::core {

namespace {
TypePtr deserializeType(const folly::dynamic& obj, void* context) {
  return ISerializable::deserialize<Type>(obj["type"]);
}

std::vector<TypedExprPtr> deserializeInputs(
    const folly::dynamic& obj,
    void* context) {
  if (obj.count("inputs")) {
    return ISerializable::deserialize<std::vector<ITypedExpr>>(
        obj["inputs"], context);
  }

  return {};
}
} // namespace

folly::dynamic ITypedExpr::serializeBase(std::string_view name) const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = name;
  obj["type"] = type_->serialize();

  if (!inputs_.empty()) {
    folly::dynamic serializedInputs = folly::dynamic::array;
    for (const auto& input : inputs_) {
      serializedInputs.push_back(input->serialize());
    }

    obj["inputs"] = serializedInputs;
  }

  return obj;
}

// static
void ITypedExpr::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();

  registry.Register("CallTypedExpr", core::CallTypedExpr::create);
  registry.Register("CastTypedExpr", core::CastTypedExpr::create);
  registry.Register("ConcatTypedExpr", core::ConcatTypedExpr::create);
  registry.Register("ConstantTypedExpr", core::ConstantTypedExpr::create);
  registry.Register("DereferenceTypedExpr", core::DereferenceTypedExpr::create);
  registry.Register("FieldAccessTypedExpr", core::FieldAccessTypedExpr::create);
  registry.Register("InputTypedExpr", core::InputTypedExpr::create);
  registry.Register("LambdaTypedExpr", core::LambdaTypedExpr::create);
}

void InputTypedExpr::accept(
    const ITypedExprVisitor& visitor,
    ITypedExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

folly::dynamic InputTypedExpr::serialize() const {
  return ITypedExpr::serializeBase("InputTypedExpr");
}

// static
TypedExprPtr InputTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto type = core::deserializeType(obj, context);

  return std::make_shared<InputTypedExpr>(std::move(type));
}

void ConstantTypedExpr::accept(
    const ITypedExprVisitor& visitor,
    ITypedExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

folly::dynamic ConstantTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("ConstantTypedExpr");
  if (valueVector_) {
    std::ostringstream out;
    saveVector(*valueVector_, out);
    auto serializedValue = out.str();
    obj["valueVector"] = encoding::Base64::encode(
        serializedValue.data(), serializedValue.size());
  } else {
    obj["value"] = value_.serialize();
  }

  return obj;
}

// static
TypedExprPtr ConstantTypedExpr::create(
    const folly::dynamic& obj,
    void* context) {
  auto type = core::deserializeType(obj, context);

  if (obj.count("value")) {
    auto value = Variant::create(obj["value"]);
    return std::make_shared<ConstantTypedExpr>(std::move(type), value);
  }

  auto encodedData = obj["valueVector"].asString();
  auto serializedData = encoding::Base64::decode(encodedData);
  std::istringstream dataStream(serializedData);

  auto* pool = static_cast<memory::MemoryPool*>(context);

  return std::make_shared<ConstantTypedExpr>(restoreVector(dataStream, pool));
}

std::string ConstantTypedExpr::toString() const {
  if (hasValueVector()) {
    return valueVector_->toString(0);
  }

  return value_.toStringAsVector(type());
}

namespace {

bool equalsImpl(
    const VectorPtr& vector,
    vector_size_t index,
    const Variant& value);

template <TypeKind Kind>
bool equalsNoNulls(
    const VectorPtr& vector,
    vector_size_t index,
    const Variant& value) {
  using T = typename TypeTraits<Kind>::NativeType;

  const auto thisValue = vector->as<SimpleVector<T>>()->valueAt(index);
  const auto otherValue = T(value.value<Kind>());

  const auto& type = vector->type();

  auto result = type->providesCustomComparison()
      ? SimpleVector<T>::comparePrimitiveAscWithCustomComparison(
            type.get(), thisValue, otherValue)
      : SimpleVector<T>::comparePrimitiveAsc(thisValue, otherValue);
  return result == 0;
}

template <>
bool equalsNoNulls<TypeKind::OPAQUE>(
    const VectorPtr& vector,
    vector_size_t index,
    const Variant& value) {
  using T = std::shared_ptr<void>;
  const auto thisValue = vector->as<SimpleVector<T>>()->valueAt(index);
  const auto& otherValue = value.value<TypeKind::OPAQUE>().obj;

  const auto& type = vector->type();

  auto result = type->providesCustomComparison()
      ? SimpleVector<T>::comparePrimitiveAscWithCustomComparison(
            type.get(), thisValue, otherValue)
      : SimpleVector<T>::comparePrimitiveAsc(thisValue, otherValue);
  return result == 0;
}

template <>
bool equalsNoNulls<TypeKind::ARRAY>(
    const VectorPtr& vector,
    vector_size_t index,
    const Variant& value) {
  auto* wrappedVector = vector->wrappedVector();
  VELOX_CHECK_EQ(VectorEncoding::Simple::ARRAY, wrappedVector->encoding());

  auto* arrayVector = wrappedVector->asUnchecked<ArrayVector>();

  index = vector->wrappedIndex(index);

  const auto offset = arrayVector->offsetAt(index);
  const auto size = arrayVector->sizeAt(index);

  const auto& arrayValue = value.value<TypeKind::ARRAY>();
  if (size != arrayValue.size()) {
    return false;
  }

  for (auto i = 0; i < size; ++i) {
    if (!equalsImpl(arrayVector->elements(), offset + i, arrayValue.at(i))) {
      return false;
    }
  }

  return true;
}

template <>
bool equalsNoNulls<TypeKind::MAP>(
    const VectorPtr& vector,
    vector_size_t index,
    const Variant& value) {
  auto* wrappedVector = vector->wrappedVector();
  VELOX_CHECK_EQ(VectorEncoding::Simple::MAP, wrappedVector->encoding());

  auto* mapVector = wrappedVector->asUnchecked<MapVector>();

  index = vector->wrappedIndex(index);

  const auto size = mapVector->sizeAt(index);

  const auto& mapValue = value.value<TypeKind::MAP>();
  if (size != mapValue.size()) {
    return false;
  }

  const auto sortedIndices = mapVector->sortedKeyIndices(index);

  size_t i = 0;
  for (const auto& [key, value] : mapValue) {
    if (!equalsImpl(mapVector->mapKeys(), sortedIndices[i], key)) {
      return false;
    }

    if (!equalsImpl(mapVector->mapValues(), sortedIndices[i], value)) {
      return false;
    }

    ++i;
  }

  return true;
}

template <>
bool equalsNoNulls<TypeKind::ROW>(
    const VectorPtr& vector,
    vector_size_t index,
    const Variant& value) {
  auto* wrappedVector = vector->wrappedVector();
  VELOX_CHECK_EQ(VectorEncoding::Simple::ROW, wrappedVector->encoding());

  auto* rowVector = wrappedVector->asUnchecked<RowVector>();

  index = vector->wrappedIndex(index);

  const auto size = rowVector->type()->size();

  const auto& rowValue = value.value<TypeKind::ROW>();
  if (size != rowValue.size()) {
    return false;
  }

  for (auto i = 0; i < size; ++i) {
    if (rowVector->childAt(i) == nullptr) {
      return false;
    }

    if (!equalsImpl(rowVector->childAt(i), index, rowValue.at(i))) {
      return false;
    }
  }

  return true;
}

bool equalsImpl(
    const VectorPtr& vector,
    vector_size_t index,
    const Variant& value) {
  static constexpr CompareFlags kEqualValueAtFlags =
      CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);

  bool thisNull = vector->isNullAt(index);
  bool otherNull = value.isNull();

  if (otherNull || thisNull) {
    return BaseVector::compareNulls(thisNull, otherNull, kEqualValueAtFlags)
               .value() == 0;
  }

  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      equalsNoNulls, vector->typeKind(), vector, index, value);
}
} // namespace

bool ConstantTypedExpr::equals(const ITypedExpr& other) const {
  const auto* casted = dynamic_cast<const ConstantTypedExpr*>(&other);
  if (!casted) {
    return false;
  }

  if (*this->type() != *casted->type()) {
    return false;
  }

  if (this->hasValueVector() != casted->hasValueVector()) {
    return this->hasValueVector()
        ? equalsImpl(this->valueVector_, 0, casted->value_)
        : equalsImpl(casted->valueVector_, 0, this->value_);
  }

  if (this->hasValueVector()) {
    return this->valueVector_->equalValueAt(casted->valueVector_.get(), 0, 0);
  }

  return this->value_ == casted->value_;
}

namespace {

uint64_t hashImpl(const TypePtr& type, const Variant& value);

template <TypeKind Kind>
uint64_t hashImpl(const TypePtr& type, const Variant& value) {
  using T = typename TypeTraits<Kind>::NativeType;

  const auto& v = value.value<Kind>();

  if (type->providesCustomComparison()) {
    return SimpleVector<T>::hashValueAtWithCustomType(type, T(v));
  }

  if constexpr (std::is_floating_point_v<T>) {
    return util::floating_point::NaNAwareHash<T>{}(T(v));
  } else {
    return folly::hasher<T>{}(T(v));
  }
}

template <>
uint64_t hashImpl<TypeKind::OPAQUE>(const TypePtr& type, const Variant& value) {
  return value.hash();
}

template <>
uint64_t hashImpl<TypeKind::ARRAY>(const TypePtr& type, const Variant& value) {
  const auto& arrayValue = value.value<TypeKind::ARRAY>();

  const auto& elementType = type->childAt(0);

  uint64_t hash = BaseVector::kNullHash;
  for (auto i = 0; i < arrayValue.size(); ++i) {
    hash = bits::hashMix(hash, hashImpl(elementType, arrayValue.at(i)));
  }
  return hash;
}

template <>
uint64_t hashImpl<TypeKind::MAP>(const TypePtr& type, const Variant& value) {
  const auto& mapValue = value.value<TypeKind::MAP>();

  const auto& keyType = type->childAt(0);
  const auto& valueType = type->childAt(1);

  uint64_t hash = BaseVector::kNullHash;
  for (const auto& [key, value] : mapValue) {
    const auto keyValueHash =
        bits::hashMix(hashImpl(keyType, key), hashImpl(valueType, value));
    hash = bits::commutativeHashMix(hash, keyValueHash);
  }

  return hash;
}

template <>
uint64_t hashImpl<TypeKind::ROW>(const TypePtr& type, const Variant& value) {
  const auto& rowValue = value.value<TypeKind::ROW>();

  uint64_t hash = BaseVector::kNullHash;
  for (auto i = 0; i < rowValue.size(); ++i) {
    const auto value = hashImpl(type->childAt(i), rowValue.at(i));
    if (i == 0) {
      hash = value;
    } else {
      hash = bits::hashMix(hash, value);
    }
  }
  return hash;
}

uint64_t hashImpl(const TypePtr& type, const Variant& value) {
  if (value.isNull()) {
    return BaseVector::kNullHash;
  }

  return VELOX_DYNAMIC_TYPE_DISPATCH_ALL(hashImpl, type->kind(), type, value);
}

} // namespace

size_t ConstantTypedExpr::localHash() const {
  static const size_t kBaseHash = std::hash<const char*>()("ConstantTypedExpr");

  uint64_t h;

  if (hasValueVector()) {
    h = valueVector_->hashValueAt(0);
  } else {
    h = hashImpl(type(), value_);
  }

  return bits::hashMix(kBaseHash, h);
}

std::string CallTypedExpr::toString() const {
  std::string str{};
  str += name();
  str += "(";
  for (size_t i = 0; i < inputs().size(); ++i) {
    auto& input = inputs().at(i);
    if (i != 0) {
      str += ",";
    }
    str += input->toString();
  }
  str += ")";
  return str;
}

void CallTypedExpr::accept(
    const ITypedExprVisitor& visitor,
    ITypedExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

folly::dynamic CallTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("CallTypedExpr");
  obj["functionName"] = name_;
  return obj;
}

// static
TypedExprPtr CallTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto type = core::deserializeType(obj, context);
  auto inputs = deserializeInputs(obj, context);

  return std::make_shared<CallTypedExpr>(
      std::move(type), std::move(inputs), obj["functionName"].asString());
}

TypedExprPtr FieldAccessTypedExpr::rewriteInputNames(
    const std::unordered_map<std::string, TypedExprPtr>& mapping) const {
  if (inputs().empty()) {
    auto it = mapping.find(name_);
    return it != mapping.end()
        ? it->second
        : std::make_shared<FieldAccessTypedExpr>(type(), name_);
  }

  auto newInputs = rewriteInputsRecursive(mapping);
  VELOX_CHECK_EQ(1, newInputs.size());
  // Only rewrite name if input in InputTypedExpr. Rewrite in other
  // cases(like dereference) is unsound.
  if (!is_instance_of<InputTypedExpr>(newInputs[0])) {
    return std::make_shared<FieldAccessTypedExpr>(type(), newInputs[0], name_);
  }
  auto it = mapping.find(name_);
  auto newName = name_;
  if (it != mapping.end()) {
    if (auto name =
            std::dynamic_pointer_cast<const FieldAccessTypedExpr>(it->second)) {
      newName = name->name();
    }
  }
  return std::make_shared<FieldAccessTypedExpr>(type(), newInputs[0], newName);
}

std::string FieldAccessTypedExpr::toString() const {
  std::stringstream ss;
  ss << std::quoted(name(), '"', '"');
  if (inputs().empty()) {
    return fmt::format("{}", ss.str());
  }

  return fmt::format("{}[{}]", inputs()[0]->toString(), ss.str());
}

void FieldAccessTypedExpr::accept(
    const ITypedExprVisitor& visitor,
    ITypedExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

folly::dynamic FieldAccessTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("FieldAccessTypedExpr");
  obj["fieldName"] = name_;
  return obj;
}

// static
TypedExprPtr FieldAccessTypedExpr::create(
    const folly::dynamic& obj,
    void* context) {
  auto type = core::deserializeType(obj, context);
  auto inputs = deserializeInputs(obj, context);
  VELOX_CHECK_LE(inputs.size(), 1);

  auto name = obj["fieldName"].asString();

  if (inputs.empty()) {
    return std::make_shared<FieldAccessTypedExpr>(std::move(type), name);
  } else {
    return std::make_shared<FieldAccessTypedExpr>(
        std::move(type), std::move(inputs[0]), name);
  }
}

void DereferenceTypedExpr::accept(
    const ITypedExprVisitor& visitor,
    ITypedExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

folly::dynamic DereferenceTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("DereferenceTypedExpr");
  obj["fieldIndex"] = index_;
  return obj;
}

// static
TypedExprPtr DereferenceTypedExpr::create(
    const folly::dynamic& obj,
    void* context) {
  auto type = core::deserializeType(obj, context);
  auto inputs = deserializeInputs(obj, context);
  VELOX_CHECK_EQ(inputs.size(), 1);

  uint32_t index = obj["fieldIndex"].asInt();

  return std::make_shared<DereferenceTypedExpr>(
      std::move(type), std::move(inputs[0]), index);
}

namespace {
TypePtr toRowType(
    const std::vector<std::string>& names,
    const std::vector<TypedExprPtr>& expressions) {
  std::vector<TypePtr> types;
  types.reserve(expressions.size());
  for (const auto& expr : expressions) {
    types.push_back(expr->type());
  }

  auto namesCopy = names;
  return ROW(std::move(namesCopy), std::move(types));
}
} // namespace

ConcatTypedExpr::ConcatTypedExpr(
    const std::vector<std::string>& names,
    const std::vector<TypedExprPtr>& inputs)
    : ITypedExpr{ExprKind::kConcat, toRowType(names, inputs), inputs} {}

std::string ConcatTypedExpr::toString() const {
  std::string str{};
  str += "CONCAT(";
  for (size_t i = 0; i < inputs().size(); ++i) {
    auto& input = inputs().at(i);
    if (i != 0) {
      str += ",";
    }
    str += input->toString();
  }
  str += ")";
  return str;
}

void ConcatTypedExpr::accept(
    const ITypedExprVisitor& visitor,
    ITypedExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

folly::dynamic ConcatTypedExpr::serialize() const {
  return ITypedExpr::serializeBase("ConcatTypedExpr");
}

// static
TypedExprPtr ConcatTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto type = core::deserializeType(obj, context);
  auto inputs = deserializeInputs(obj, context);

  return std::make_shared<ConcatTypedExpr>(
      type->asRow().names(), std::move(inputs));
}

TypedExprPtr LambdaTypedExpr::rewriteInputNames(
    const std::unordered_map<std::string, TypedExprPtr>& mapping) const {
  for (const auto& name : signature_->names()) {
    if (mapping.count(name)) {
      VELOX_USER_FAIL("Ambiguous variable: {}", name);
    }
  }
  return std::make_shared<LambdaTypedExpr>(
      signature_, body_->rewriteInputNames(mapping));
}

void LambdaTypedExpr::accept(
    const ITypedExprVisitor& visitor,
    ITypedExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

folly::dynamic LambdaTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("LambdaTypedExpr");
  obj["signature"] = signature_->serialize();
  obj["body"] = body_->serialize();
  return obj;
}

// static
TypedExprPtr LambdaTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto signature = ISerializable::deserialize<Type>(obj["signature"]);
  auto body = ISerializable::deserialize<ITypedExpr>(obj["body"], context);

  return std::make_shared<LambdaTypedExpr>(
      asRowType(signature), std::move(body));
}

std::string CastTypedExpr::toString() const {
  if (isTryCast_) {
    return fmt::format(
        "try_cast({} as {})", inputs()[0]->toString(), type()->toString());
  } else {
    return fmt::format(
        "cast({} as {})", inputs()[0]->toString(), type()->toString());
  }
}

void CastTypedExpr::accept(
    const ITypedExprVisitor& visitor,
    ITypedExprVisitorContext& context) const {
  visitor.visit(*this, context);
}

folly::dynamic CastTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("CastTypedExpr");
  obj["isTryCast"] = isTryCast_;
  return obj;
}

// static
TypedExprPtr CastTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto type = core::deserializeType(obj, context);
  auto inputs = deserializeInputs(obj, context);

  return std::make_shared<CastTypedExpr>(
      std::move(type), std::move(inputs), obj["isTryCast"].asBool());
}

} // namespace facebook::velox::core
