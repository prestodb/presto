/*
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

#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include <boost/algorithm/string/case_conv.hpp>
#include "presto_cpp/main/types/ParseTypeSignature.h"
#include "presto_cpp/presto_protocol/Base64Util.h"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::core;
using facebook::velox::TypeKind;

namespace facebook::presto {
namespace {

template <typename T>
std::string toJsonString(const T& value) {
  return ((json)value).dump();
}

std::optional<std::string> mapDefaultFunctionName(
    const std::string& lowerCaseName) {
  static const char* kPrestoDefaultPrefix = "presto.default.";
  static const uint32_t kPrestoDefaultPrefixLength =
      strlen(kPrestoDefaultPrefix);

  if (lowerCaseName.compare(
          0, kPrestoDefaultPrefixLength, kPrestoDefaultPrefix) == 0) {
    return lowerCaseName.substr(kPrestoDefaultPrefixLength);
  }

  return std::nullopt;
}

std::string mapScalarFunction(const std::string& name) {
  static const std::unordered_map<std::string, std::string> kFunctionNames = {
      // see com.facebook.presto.common.function.OperatorType
      {"presto.default.$operator$add", "plus"},
      {"presto.default.$operator$between", "between"},
      {"presto.default.$operator$divide", "divide"},
      {"presto.default.$operator$equal", "eq"},
      {"presto.default.$operator$greater_than", "gt"},
      {"presto.default.$operator$greater_than_or_equal", "gte"},
      {"presto.default.$operator$is_distinct_from", "distinct_from"},
      {"presto.default.$operator$less_than", "lt"},
      {"presto.default.$operator$less_than_or_equal", "lte"},
      {"presto.default.$operator$modulus", "mod"},
      {"presto.default.$operator$multiply", "multiply"},
      {"presto.default.$operator$negation", "negate"},
      {"presto.default.$operator$not_equal", "neq"},
      {"presto.default.$operator$subtract", "minus"},
      {"presto.default.$operator$subscript", "subscript"},
      {"presto.default.random", "rand"}};

  std::string lowerCaseName = boost::to_lower_copy(name);

  auto it = kFunctionNames.find(lowerCaseName);
  if (it != kFunctionNames.end()) {
    return it->second;
  }

  auto mappedName = mapDefaultFunctionName(lowerCaseName);
  if (mappedName.has_value()) {
    return mappedName.value();
  }

  return lowerCaseName;
}

std::string mapAggregateFunctionName(const std::string& name) {
  std::string lowerCaseName = boost::to_lower_copy(name);

  auto mappedName = mapDefaultFunctionName(lowerCaseName);
  if (mappedName.has_value()) {
    return mappedName.value();
  }

  return lowerCaseName;
}

std::string getFunctionName(const protocol::Signature& signature) {
  switch (signature.kind) {
    case protocol::FunctionKind::SCALAR:
      return mapScalarFunction(signature.name);
    case protocol::FunctionKind::AGGREGATE:
      return mapAggregateFunctionName(signature.name);
    default:
      return signature.name;
  }
}

} // namespace

velox::variant VeloxExprConverter::getConstantValue(
    const velox::TypePtr& type,
    const protocol::Block& block) const {
  auto valueVector = protocol::readBlock(type, block.data, pool_);

  auto typeKind = type->kind();
  if (valueVector->isNullAt(0)) {
    return velox::variant(typeKind);
  }

  switch (typeKind) {
    case TypeKind::BIGINT:
      return valueVector->as<velox::SimpleVector<int64_t>>()->valueAt(0);
    case TypeKind::INTEGER:
      return valueVector->as<velox::SimpleVector<int32_t>>()->valueAt(0);
    case TypeKind::SMALLINT:
      return valueVector->as<velox::SimpleVector<int16_t>>()->valueAt(0);
    case TypeKind::TINYINT:
      return valueVector->as<velox::SimpleVector<int8_t>>()->valueAt(0);
    case TypeKind::TIMESTAMP:
      return valueVector->as<velox::SimpleVector<velox::Timestamp>>()->valueAt(
          0);
    case TypeKind::DATE:
      return valueVector->as<velox::SimpleVector<velox::Date>>()->valueAt(0);
    case TypeKind::INTERVAL_DAY_TIME:
      return valueVector->as<velox::SimpleVector<velox::IntervalDayTime>>()
          ->valueAt(0);
    case TypeKind::BOOLEAN:
      return valueVector->as<velox::SimpleVector<bool>>()->valueAt(0);
    case TypeKind::DOUBLE:
      return valueVector->as<velox::SimpleVector<double>>()->valueAt(0);
    case TypeKind::REAL:
      return valueVector->as<velox::SimpleVector<float>>()->valueAt(0);
    case TypeKind::VARCHAR:
      return velox::variant(
          valueVector->as<velox::SimpleVector<velox::StringView>>()->valueAt(
              0));
    default:
      throw std::invalid_argument(
          "Unexpected Block type: " + mapTypeKindToName(typeKind));
  }
}

std::vector<std::shared_ptr<const ITypedExpr>> VeloxExprConverter::toVeloxExpr(
    std::vector<std::shared_ptr<protocol::RowExpression>> pexpr) const {
  std::vector<std::shared_ptr<const ITypedExpr>> reply;
  reply.reserve(pexpr.size());
  for (auto arg : pexpr) {
    reply.emplace_back(toVeloxExpr(arg));
  }

  return reply;
}

namespace {
/// Converts cast and try_cast functions to CastTypedExpr with nullOnFailure
/// flag set to false and true appropriately.
/// Removed cast to Re2JRegExp type. Velox doesn't have such type and uses
/// different mechanism (stateful vector functions) to avoid re-compiling
/// regular expressions needlessly.
std::optional<std::shared_ptr<const ITypedExpr>> tryConvertCast(
    const protocol::Signature& signature,
    const std::string& returnType,
    const std::vector<std::shared_ptr<const ITypedExpr>>& args) {
  static const char* kCast = "presto.default.$operator$cast";
  static const char* kTryCast = "presto.default.try_cast";

  static const char* kRe2JRegExp = "Re2JRegExp";

  if (signature.kind != protocol::FunctionKind::SCALAR) {
    return std::nullopt;
  }

  bool nullOnFailure;
  if (signature.name.compare(kCast) == 0) {
    nullOnFailure = false;
  } else if (signature.name.compare(kTryCast) == 0) {
    nullOnFailure = true;
  } else {
    return std::nullopt;
  }

  if (returnType == kRe2JRegExp) {
    return args[0];
  }

  auto type = parseTypeSignature(returnType);
  return std::make_shared<CastTypedExpr>(type, args, nullOnFailure);
}

std::optional<std::shared_ptr<const ITypedExpr>> tryConvertTry(
    const protocol::Signature& signature,
    const std::string& returnType,
    const std::vector<std::shared_ptr<const ITypedExpr>>& args) {
  static const char* kTry = "presto.default.$internal$try";

  if (signature.kind != protocol::FunctionKind::SCALAR) {
    return std::nullopt;
  }

  if (signature.name.compare(kTry) != 0) {
    return std::nullopt;
  }

  VELOX_CHECK_EQ(args.size(), 1);

  auto lambda = std::dynamic_pointer_cast<const LambdaTypedExpr>(args[0]);
  VELOX_CHECK(lambda);
  VELOX_CHECK_EQ(lambda->signature()->size(), 0);

  auto type = parseTypeSignature(returnType);
  std::vector<std::shared_ptr<const ITypedExpr>> newArgs = {lambda->body()};
  return std::make_shared<CallTypedExpr>(type, newArgs, "try");
}

std::optional<std::shared_ptr<const ITypedExpr>> tryConvertLiteralArray(
    const protocol::Signature& signature,
    const std::string& returnType,
    const std::vector<std::shared_ptr<const ITypedExpr>>& args,
    velox::memory::MemoryPool* pool) {
  static const char* kLiteralArray = "presto.default.$literal$array";
  static const char* kFromBase64 = "from_base64";

  if (signature.kind != protocol::FunctionKind::SCALAR) {
    return std::nullopt;
  }

  if (signature.name.compare(0, strlen(kLiteralArray), kLiteralArray) != 0) {
    return std::nullopt;
  }

  VELOX_CHECK_EQ(args.size(), 1);

  auto call = std::dynamic_pointer_cast<const CallTypedExpr>(args[0]);
  VELOX_CHECK_NOT_NULL(call);
  if (call->name() != kFromBase64) {
    return std::nullopt;
  }

  auto type = parseTypeSignature(returnType);

  auto encoded =
      std::dynamic_pointer_cast<const ConstantTypedExpr>(call->inputs()[0]);
  VELOX_CHECK_NOT_NULL(encoded);
  auto encodedString = encoded->value().value<velox::StringView>();
  auto elementsVector =
      protocol::readBlock(type->asArray().elementType(), encodedString, pool);

  velox::BufferPtr offsets =
      velox::AlignedBuffer::allocate<velox::vector_size_t>(1, pool, 0);
  velox::BufferPtr sizes = velox::AlignedBuffer::allocate<velox::vector_size_t>(
      1, pool, elementsVector->size());
  auto arrayVector = std::make_shared<velox::ArrayVector>(
      pool, type, nullptr, 1, offsets, sizes, elementsVector);

  return std::make_shared<ConstantTypedExpr>(
      velox::BaseVector::wrapInConstant(1, 0, arrayVector));
}
} // namespace

std::optional<std::shared_ptr<const ITypedExpr>>
VeloxExprConverter::tryConvertDate(
    const protocol::CallExpression& pexpr) const {
  static const char* kDate = "presto.default.date";

  auto builtin = std::static_pointer_cast<protocol::BuiltInFunctionHandle>(
      pexpr.functionHandle);
  auto signature = builtin->signature;
  if (signature.name != kDate) {
    return std::nullopt;
  }

  VELOX_CHECK_EQ(pexpr.arguments.size(), 1);
  std::vector<std::shared_ptr<const ITypedExpr>> args;
  // The argument to date function should be an expression that evaluates to
  // a VARCHAR or TIMESTAMP (with an optional timezone) type.
  args.emplace_back(toVeloxExpr(pexpr.arguments[0]));

  auto returnType = parseTypeSignature(pexpr.returnType);
  return std::make_shared<CastTypedExpr>(returnType, args, false);
}

std::optional<std::shared_ptr<const ITypedExpr>>
VeloxExprConverter::tryConvertLike(
    const protocol::CallExpression& pexpr) const {
  static const char* kLike = "presto.default.like";
  static const char* kLikePatternType = "presto.default.like_pattern";
  static const char* kLikeReturnType = "LikePattern";
  static const char* kCast = "presto.default.$operator$cast";

  auto builtin = std::static_pointer_cast<protocol::BuiltInFunctionHandle>(
      pexpr.functionHandle);
  auto signature = builtin->signature;
  if (signature.name != kLike) {
    return std::nullopt;
  }

  VELOX_CHECK_EQ(pexpr.arguments.size(), 2);

  std::vector<std::shared_ptr<const ITypedExpr>> args;
  // The first argument to like is an expression that should evaluate to a
  // varchar type.
  args.emplace_back(toVeloxExpr(pexpr.arguments[0]));

  // The second argument in Presto like is either of cast('<pattern>' as
  // LikePattern) or like_pattern('<pattern>', '<escape-char>'). However, Velox
  // function like requires <pattern> and <escape-char> to be its 2nd and 3rd
  // arguments.
  auto likePatternExpr =
      std::dynamic_pointer_cast<const protocol::CallExpression>(
          pexpr.arguments[1]);
  VELOX_CHECK_NOT_NULL(likePatternExpr);
  auto likePatternBuiltin =
      std::static_pointer_cast<protocol::BuiltInFunctionHandle>(
          likePatternExpr->functionHandle);
  auto likePatternSignature = likePatternBuiltin->signature;
  if (likePatternSignature.name == kCast) {
    VELOX_CHECK_EQ(likePatternExpr->returnType, kLikeReturnType);
    args.emplace_back(toVeloxExpr(likePatternExpr->arguments[0]));
  } else if (likePatternSignature.name == kLikePatternType) {
    VELOX_CHECK_EQ(likePatternExpr->arguments.size(), 2);
    args.emplace_back(toVeloxExpr(likePatternExpr->arguments[0]));
    args.emplace_back(toVeloxExpr(likePatternExpr->arguments[1]));
  } else {
    VELOX_FAIL(
        "Unexpected like signature: {}", toJsonString(pexpr.arguments[1]));
  }

  // Construct the returnType and CallTypedExpr for 'like'
  auto returnType = parseTypeSignature(pexpr.returnType);
  return std::make_shared<CallTypedExpr>(
      returnType, args, getFunctionName(signature));
}

std::shared_ptr<const ITypedExpr> VeloxExprConverter::toVeloxExpr(
    const protocol::CallExpression& pexpr) const {
  auto handle = pexpr.functionHandle;
  VELOX_CHECK_EQ(
      handle->_type,
      "$static",
      "Unsupported function handle: {}",
      handle->_type);

  // Handle some special parsing needed for 'like' operator signatures.
  auto like = tryConvertLike(pexpr);
  if (like.has_value()) {
    return like.value();
  }

  // 'date' operators need to be converted to a cast expression for date.
  auto date = tryConvertDate(pexpr);
  if (date.has_value()) {
    return date.value();
  }

  auto args = toVeloxExpr(pexpr.arguments);
  auto builtin =
      std::static_pointer_cast<protocol::BuiltInFunctionHandle>(handle);
  auto signature = builtin->signature;

  auto cast = tryConvertCast(signature, pexpr.returnType, args);
  if (cast.has_value()) {
    return cast.value();
  }

  auto tryExpr = tryConvertTry(signature, pexpr.returnType, args);
  if (tryExpr.has_value()) {
    return tryExpr.value();
  }

  auto literal =
      tryConvertLiteralArray(signature, pexpr.returnType, args, pool_);
  if (literal.has_value()) {
    return literal.value();
  }

  auto returnType = parseTypeSignature(pexpr.returnType);
  return std::make_shared<CallTypedExpr>(
      returnType, args, getFunctionName(signature));
}

std::shared_ptr<const ConstantTypedExpr> VeloxExprConverter::toVeloxExpr(
    std::shared_ptr<protocol::ConstantExpression> pexpr) const {
  const auto type = parseTypeSignature(pexpr->type);
  switch (type->kind()) {
    case TypeKind::ROW: {
      if (isTimestampWithTimeZoneType(type)) {
        auto valueVector =
            protocol::readBlock(type, pexpr->valueBlock.data, pool_);
        return std::make_shared<ConstantTypedExpr>(
            std::make_shared<velox::ConstantVector<velox::ComplexType>>(
                pool_, 1, 0, valueVector));
      }
    }
    case TypeKind::ARRAY: {
      auto valueVector =
          protocol::readBlock(type, pexpr->valueBlock.data, pool_);
      return std::make_shared<ConstantTypedExpr>(
          std::make_shared<velox::ConstantVector<velox::ComplexType>>(
              pool_, 1, 0, valueVector));
    }
    default: {
      const auto value = getConstantValue(type, pexpr->valueBlock);

      return std::make_shared<ConstantTypedExpr>(value);
    }
  }
}

namespace {
bool isTrueConstant(const std::shared_ptr<const ITypedExpr>& expression) {
  if (auto constExpression =
          std::dynamic_pointer_cast<const ConstantTypedExpr>(expression)) {
    return constExpression->type()->kind() == TypeKind::BOOLEAN &&
        constExpression->value().value<bool>();
  }
  return false;
}

std::shared_ptr<const CallTypedExpr> makeEqualsExpr(
    const std::shared_ptr<const ITypedExpr>& a,
    const std::shared_ptr<const ITypedExpr>& b) {
  std::vector<std::shared_ptr<const ITypedExpr>> inputs{a, b};
  return std::make_shared<CallTypedExpr>(
      velox::BOOLEAN(), std::move(inputs), "eq");
}

std::shared_ptr<const CastTypedExpr> makeCastExpr(
    const std::shared_ptr<const ITypedExpr>& expr,
    const velox::TypePtr& type) {
  std::vector<std::shared_ptr<const ITypedExpr>> inputs{expr};
  return std::make_shared<CastTypedExpr>(type, std::move(inputs), false);
}

std::shared_ptr<const CallTypedExpr> convertSwitchExpr(
    const velox::TypePtr& returnType,
    std::vector<std::shared_ptr<const ITypedExpr>> args) {
  auto valueExpr = args.front();
  args.erase(args.begin());

  std::vector<std::shared_ptr<const ITypedExpr>> inputs;
  inputs.reserve((args.size() - 1) * 2);

  const bool valueIsTrue = isTrueConstant(valueExpr);

  for (const auto& arg : args) {
    if (auto call = std::dynamic_pointer_cast<const CallTypedExpr>(arg)) {
      if (call->name() == "when") {
        auto& condition = call->inputs()[0];
        if (valueIsTrue) {
          inputs.emplace_back(condition);
        } else {
          if (condition->type()->kindEquals(valueExpr->type())) {
            inputs.emplace_back(makeEqualsExpr(condition, valueExpr));
          } else {
            inputs.emplace_back(makeEqualsExpr(
                makeCastExpr(condition, valueExpr->type()), valueExpr));
          }
        }
        inputs.emplace_back(call->inputs()[1]);
        continue;
      }
    }

    inputs.emplace_back(arg);
  }

  return std::make_shared<CallTypedExpr>(
      returnType, std::move(inputs), "switch");
}

std::shared_ptr<const ITypedExpr> convertBindExpr(
    const std::vector<std::shared_ptr<const ITypedExpr>>& args) {
  VELOX_CHECK_GE(
      args.size(), 2, "BIND expression must have at least two arguments");

  // last argument must be a lambda
  auto lambda = std::dynamic_pointer_cast<const LambdaTypedExpr>(args.back());
  VELOX_CHECK(lambda, "Last argument of a BIND must be a lambda expression");

  // replace first N arguments of the lambda with bind variables
  std::unordered_map<std::string, std::string> mapping;
  mapping.reserve(args.size() - 1);

  const auto& signature = lambda->signature();

  for (auto i = 0; i < args.size() - 1; i++) {
    const auto& field =
        std::dynamic_pointer_cast<const FieldAccessTypedExpr>(args[i]);
    mapping.insert({signature->nameOf(i), field->name()});
  }

  auto numArgsLeft = signature->size() - (args.size() - 1);

  std::vector<std::string> newNames;
  newNames.reserve(numArgsLeft);
  std::vector<velox::TypePtr> newTypes;
  newTypes.reserve(numArgsLeft);
  for (auto i = 0; i < numArgsLeft; i++) {
    newNames.emplace_back(signature->nameOf(i + args.size() - 1));
    newTypes.emplace_back(signature->childAt(i + args.size() - 1));
  }

  auto newSignature = ROW(std::move(newNames), std::move(newTypes));

  return std::make_shared<LambdaTypedExpr>(
      newSignature, lambda->body()->rewriteInputNames(mapping));
}

template <TypeKind KIND>
velox::ArrayVectorPtr toArrayVector(
    std::vector<std::shared_ptr<const ITypedExpr>>::const_iterator begin,
    std::vector<std::shared_ptr<const ITypedExpr>>::const_iterator end,
    velox::memory::MemoryPool* pool) {
  using T = typename velox::TypeTraits<KIND>::NativeType;

  const auto size = end - begin;
  auto elements = std::dynamic_pointer_cast<velox::FlatVector<T>>(
      velox::BaseVector::create(velox::CppToType<T>::create(), size, pool));

  for (auto i = 0; i < size; ++i) {
    auto constant =
        dynamic_cast<const ConstantTypedExpr*>((*(begin + i)).get());
    if (constant == nullptr) {
      VELOX_UNSUPPORTED("IN predicate supports only constant list of values");
    }
    const auto& value = constant->value();
    if (value.isNull()) {
      elements->setNull(i, true);
    } else {
      if constexpr (std::is_same_v<T, velox::StringView>) {
        elements->set(i, velox::StringView(value.value<T>()));
      } else {
        elements->set(i, value.value<T>());
      }
    }
  }

  auto offsets = velox::allocateOffsets(size, pool);
  auto sizes = velox::allocateSizes(size, pool);

  auto rawSizes = sizes->asMutable<velox::vector_size_t>();
  rawSizes[0] = size;

  return std::make_shared<velox::ArrayVector>(
      pool,
      ARRAY(velox::Type::create<KIND>()),
      nullptr,
      1,
      offsets,
      sizes,
      elements);
}

std::shared_ptr<const ITypedExpr> convertInExpr(
    const std::vector<std::shared_ptr<const ITypedExpr>>& args,
    velox::memory::MemoryPool* pool) {
  auto numArgs = args.size();
  VELOX_USER_CHECK_GE(numArgs, 2);

  auto arrayVector = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      toArrayVector,
      args[0]->type()->kind(),
      args.begin() + 1,
      args.end(),
      pool);
  auto constantVector =
      std::make_shared<velox::ConstantVector<velox::ComplexType>>(
          pool, 1, 0, arrayVector);

  std::vector<std::shared_ptr<const ITypedExpr>> newArgs = {
      args[0], std::make_shared<const ConstantTypedExpr>(constantVector)};
  return std::make_shared<CallTypedExpr>(velox::BOOLEAN(), newArgs, "in");
}

std::shared_ptr<const ITypedExpr> convertDereferenceExpr(
    const velox::TypePtr& returnType,
    const std::vector<std::shared_ptr<const ITypedExpr>>& args) {
  VELOX_USER_CHECK_EQ(args.size(), 2);

  const auto& input = args[0];
  VELOX_USER_CHECK_EQ(input->type()->kind(), TypeKind::ROW);
  const auto& inputType = input->type()->asRow();

  // First argument is a struct. Second argument is a constant integer
  // zero-based index of the subfield in the struct.

  auto childIndexExpr = dynamic_cast<const ConstantTypedExpr*>(args[1].get());
  VELOX_USER_CHECK_NOT_NULL(
      childIndexExpr,
      "Second argument for dereference special form must be a constant integer");

  auto childIndex = childIndexExpr->value().value<int32_t>();

  VELOX_USER_CHECK_LT(childIndex, inputType.size());
  auto childName = inputType.names()[childIndex];

  return std::make_shared<FieldAccessTypedExpr>(returnType, input, childName);
}
} // namespace

std::shared_ptr<const ITypedExpr> VeloxExprConverter::toVeloxExpr(
    std::shared_ptr<protocol::SpecialFormExpression> pexpr) const {
  std::vector<std::shared_ptr<const ITypedExpr>> args =
      toVeloxExpr(pexpr->arguments);

  if (pexpr->form == protocol::Form::BIND) {
    return convertBindExpr(args);
  }

  if (pexpr->form == protocol::Form::IN) {
    return convertInExpr(args, pool_);
  }

  auto returnType = parseTypeSignature(pexpr->returnType);

  if (pexpr->form == protocol::Form::SWITCH) {
    return convertSwitchExpr(returnType, std::move(args));
  }

  if (pexpr->form == protocol::Form::DEREFERENCE) {
    return convertDereferenceExpr(returnType, args);
  }

  if (pexpr->form == protocol::Form::ROW_CONSTRUCTOR) {
    return std::make_shared<CallTypedExpr>(
        returnType, std::move(args), "row_constructor");
  }

  auto form = std::string(json(pexpr->form));
  return std::make_shared<CallTypedExpr>(
      returnType, args, mapScalarFunction(form));
}

std::shared_ptr<const FieldAccessTypedExpr> VeloxExprConverter::toVeloxExpr(
    std::shared_ptr<protocol::VariableReferenceExpression> pexpr) const {
  return std::make_shared<FieldAccessTypedExpr>(
      parseTypeSignature(pexpr->type), pexpr->name);
}

std::shared_ptr<const LambdaTypedExpr> VeloxExprConverter::toVeloxExpr(
    std::shared_ptr<protocol::LambdaDefinitionExpression> lambda) const {
  std::vector<velox::TypePtr> argumentTypes;
  argumentTypes.reserve(lambda->argumentTypes.size());
  for (auto& typeName : lambda->argumentTypes) {
    argumentTypes.emplace_back(parseTypeSignature(typeName));
  }

  auto signature = ROW(std::move(lambda->arguments), std::move(argumentTypes));
  return std::make_shared<LambdaTypedExpr>(
      signature, toVeloxExpr(lambda->body));
}

std::shared_ptr<const FieldAccessTypedExpr> VeloxExprConverter::toVeloxExpr(
    const protocol::VariableReferenceExpression& pexpr) const {
  return std::make_shared<FieldAccessTypedExpr>(
      parseTypeSignature(pexpr.type), pexpr.name);
}

std::shared_ptr<const ITypedExpr> VeloxExprConverter::toVeloxExpr(
    std::shared_ptr<protocol::RowExpression> pexpr) const {
  if (auto call = std::dynamic_pointer_cast<protocol::CallExpression>(pexpr)) {
    return toVeloxExpr(*call);
  }
  if (auto constant =
          std::dynamic_pointer_cast<protocol::ConstantExpression>(pexpr)) {
    return toVeloxExpr(constant);
  }
  if (auto special =
          std::dynamic_pointer_cast<protocol::SpecialFormExpression>(pexpr)) {
    return toVeloxExpr(special);
  }
  if (auto variable =
          std::dynamic_pointer_cast<protocol::VariableReferenceExpression>(
              pexpr)) {
    return toVeloxExpr(variable);
  }
  if (auto lambda =
          std::dynamic_pointer_cast<protocol::LambdaDefinitionExpression>(
              pexpr)) {
    return toVeloxExpr(lambda);
  }

  throw std::invalid_argument(
      "Unsupported RowExpression type: " + pexpr->_type);
}

} // namespace facebook::presto
