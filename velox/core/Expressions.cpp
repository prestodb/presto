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
#include "velox/common/encode/Base64.h"
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
  registry.Register("FieldAccessTypedExpr", core::FieldAccessTypedExpr::create);
  registry.Register("InputTypedExpr", core::InputTypedExpr::create);
  registry.Register("LambdaTypedExpr", core::LambdaTypedExpr::create);
}

folly::dynamic InputTypedExpr::serialize() const {
  return ITypedExpr::serializeBase("InputTypedExpr");
}

// static
TypedExprPtr InputTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto type = core::deserializeType(obj, context);

  return std::make_shared<InputTypedExpr>(std::move(type));
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
    auto value = variant::create(obj["value"]);
    return std::make_shared<ConstantTypedExpr>(std::move(type), value);
  }

  auto encodedData = obj["valueVector"].asString();
  auto serializedData = encoding::Base64::decode(encodedData);
  std::istringstream dataStream(serializedData);

  auto* pool = static_cast<memory::MemoryPool*>(context);

  return std::make_shared<ConstantTypedExpr>(restoreVector(dataStream, pool));
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

folly::dynamic LambdaTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("LambdaTypedExpr");
  obj["signature"] = signature_->serialize();
  obj["body"] = body_->serialize();
  return obj;
}

// static
TypedExprPtr LambdaTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto signature = ISerializable::deserialize<Type>(obj["signature"]);
  auto body = ISerializable::deserialize<ITypedExpr>(obj["body"]);

  return std::make_shared<LambdaTypedExpr>(
      asRowType(signature), std::move(body));
}

folly::dynamic CastTypedExpr::serialize() const {
  auto obj = ITypedExpr::serializeBase("CastTypedExpr");
  obj["nullOnFailure"] = nullOnFailure_;
  return obj;
}

// static
TypedExprPtr CastTypedExpr::create(const folly::dynamic& obj, void* context) {
  auto type = core::deserializeType(obj, context);
  auto inputs = deserializeInputs(obj, context);

  return std::make_shared<CastTypedExpr>(
      std::move(type), std::move(inputs), obj["nullOnFailure"].asBool());
}

} // namespace facebook::velox::core
