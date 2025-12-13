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
#include <unordered_map>
#include "presto_cpp/main/common/Configs.h"
#include "presto_cpp/main/common/Utils.h"
#include "presto_cpp/presto_protocol/Base64Util.h"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"
#include "velox/vector/FlatVector.h"
#ifdef PRESTO_ENABLE_REMOTE_FUNCTIONS
#include "presto_cpp/main/functions/remote/PrestoRestFunctionRegistration.h"
#endif

using namespace facebook::velox::core;
using facebook::velox::TypeKind;

namespace facebook::presto {
namespace {

template <typename T>
std::string toJsonString(const T& value) {
  return ((json)value).dump();
}

std::string mapScalarFunction(const std::string& name) {
  std::string lowerCaseName = boost::to_lower_copy(name);
  if (prestoOperatorMap().find(lowerCaseName) != prestoOperatorMap().end()) {
    return prestoOperatorMap().at(lowerCaseName);
  }

  return lowerCaseName;
}

std::string mapAggregateOrWindowFunction(const std::string& name) {
  static const std::string prestoDefaultNamespacePrefix =
      SystemConfig::instance()->prestoDefaultNamespacePrefix();
  static const std::unordered_map<std::string, std::string> kFunctionNames = {
      {"presto.default.$internal$max_data_size_for_stats",
       util::addDefaultNamespacePrefix(
           prestoDefaultNamespacePrefix, "max_data_size_for_stats")},
      {"presto.default.$internal$sum_data_size_for_stats",
       util::addDefaultNamespacePrefix(
           prestoDefaultNamespacePrefix, "sum_data_size_for_stats")},
  };
  std::string lowerCaseName = boost::to_lower_copy(name);
  auto it = kFunctionNames.find(name);
  if (it != kFunctionNames.end()) {
    return it->second;
  }
  return lowerCaseName;
}

std::string getFunctionName(const protocol::Signature& signature) {
  switch (signature.kind) {
    case protocol::FunctionKind::SCALAR:
      return mapScalarFunction(signature.name);
    case protocol::FunctionKind::AGGREGATE:
    case protocol::FunctionKind::WINDOW:
      return mapAggregateOrWindowFunction(signature.name);
    default:
      return signature.name;
  }
}

std::string getFunctionName(const protocol::SqlFunctionId& functionId) {
  // Example: "json.x4.eq;INTEGER;INTEGER".
  const auto nameEnd = functionId.find(';');
  // Assuming the possibility of missing ';' if there are no function arguments.
  return nameEnd != std::string::npos ? functionId.substr(0, nameEnd)
                                      : functionId;
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
    case TypeKind::HUGEINT:
      return valueVector->as<velox::SimpleVector<velox::int128_t>>()->valueAt(
          0);
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
    case TypeKind::VARBINARY:
      return velox::variant::binary(
          valueVector->as<velox::SimpleVector<velox::StringView>>()->valueAt(
              0));
    default:
      throw std::invalid_argument(
          fmt::format("Unexpected Block type: {}", typeKind));
  }
}

std::vector<TypedExprPtr> VeloxExprConverter::toVeloxExpr(
    std::vector<std::shared_ptr<protocol::RowExpression>> pexpr) const {
  std::vector<TypedExprPtr> reply;
  reply.reserve(pexpr.size());
  for (auto arg : pexpr) {
    reply.emplace_back(toVeloxExpr(arg));
  }

  return reply;
}

namespace {
static const char* kVarchar = "varchar";

/// Convert cast of varchar to substr if target type is varchar with max length.
/// Throw an exception for cast of varchar to varchar with max length.
std::optional<TypedExprPtr> convertCastToVarcharWithMaxLength(
    const std::string& returnType,
    const std::vector<TypedExprPtr>& args,
    bool nullOnFailure) {
  static const std::string prestoDefaultNamespacePrefix =
      SystemConfig::instance()->prestoDefaultNamespacePrefix();
  if (nullOnFailure) {
    VELOX_UNSUPPORTED(
        "TRY_CAST of varchar to {} is not supported.", returnType);
  }

  // Parse the max length from the return type string in the format of
  // varchar(max_length). Assume return type string is valid given
  // TypeParser.yy.
  char* end;
  const auto length =
      strtol(returnType.data() + strlen(kVarchar) + 1, &end, 10);
  VELOX_DCHECK(errno != ERANGE);
  VELOX_DCHECK(end == returnType.data() + returnType.size() - 1);

  VELOX_DCHECK_EQ(args.size(), 1);

  auto arg = args[0];
  // If the argument is of JSON type, convert it to VARCHAR before applying
  // substr.
  if (velox::isJsonType(arg->type())) {
    arg = std::make_shared<CastTypedExpr>(velox::VARCHAR(), arg, false);
  }
  return std::make_shared<CallTypedExpr>(
      arg->type(),
      std::vector<TypedExprPtr>{
          arg,
          std::make_shared<ConstantTypedExpr>(velox::BIGINT(), 1LL),
          std::make_shared<ConstantTypedExpr>(velox::BIGINT(), (int64_t)length),
      },
      util::addDefaultNamespacePrefix(prestoDefaultNamespacePrefix, "substr"));
}

/// Converts cast and try_cast functions to CastTypedExpr with nullOnFailure
/// flag set to false and true appropriately.
/// Removes cast to Re2JRegExp type. Velox doesn't have such type and uses
/// different mechanism (stateful vector functions) to avoid re-compiling
/// regular expressions needlessly.
/// Removes cast to CodePoints type. Velox doesn't have such type and uses
/// different mechanisms to implement trim functions efficiently.
/// Convert cast of varchar to substr if the target type is varchar with max
/// length. Throw an exception for cast of varchar to varchar with max length.
std::optional<TypedExprPtr> tryConvertCast(
    const protocol::Signature& signature,
    const std::string& returnType,
    const std::vector<TypedExprPtr>& args,
    const TypeParser* typeParser) {
  static const std::string prestoDefaultNamespacePrefix =
      SystemConfig::instance()->prestoDefaultNamespacePrefix();
  static const char* kCast = "presto.default.$operator$cast";
  static const char* kTryCast = "presto.default.try_cast";
  static const char* kJsonToArrayCast =
      "presto.default.$internal$json_string_to_array_cast";
  static const char* kJsonToMapCast =
      "presto.default.$internal$json_string_to_map_cast";
  static const char* kJsonToRowCast =
      "presto.default.$internal$json_string_to_row_cast";

  static const char* kRe2JRegExp = "Re2JRegExp";
  static const char* kJsonPath = "JsonPath";
  static const char* kCodePoints = "CodePoints";

  if (signature.kind != protocol::FunctionKind::SCALAR) {
    return std::nullopt;
  }

  bool nullOnFailure;
  if (signature.name.compare(kCast) == 0) {
    nullOnFailure = false;
  } else if (signature.name.compare(kTryCast) == 0) {
    nullOnFailure = true;
  } else if (
      signature.name.compare(kJsonToArrayCast) == 0 ||
      signature.name.compare(kJsonToMapCast) == 0 ||
      signature.name.compare(kJsonToRowCast) == 0) {
    auto type = typeParser->parse(returnType);
    return std::make_shared<CastTypedExpr>(
        type,
        std::vector<TypedExprPtr>{std::make_shared<CallTypedExpr>(
            velox::JSON(),
            args,
            util::addDefaultNamespacePrefix(
                prestoDefaultNamespacePrefix, "json_parse"))},
        false);
  } else {
    return std::nullopt;
  }

  if (returnType == kRe2JRegExp) {
    return args[0];
  }

  if (returnType == kJsonPath) {
    return args[0];
  }

  if (returnType == kCodePoints) {
    return args[0];
  }

  // When the return type is varchar with max length, truncate if only the
  // argument type is varchar, or varchar with max length or json. Non-varchar
  // argument types are not truncated.
  if (returnType.find(kVarchar) == 0 &&
      args[0]->type()->kind() == TypeKind::VARCHAR &&
      returnType.size() > strlen(kVarchar)) {
    return convertCastToVarcharWithMaxLength(returnType, args, nullOnFailure);
  }

  auto type = typeParser->parse(returnType);
  return std::make_shared<CastTypedExpr>(type, args, nullOnFailure);
}

std::optional<TypedExprPtr> tryConvertTry(
    const protocol::Signature& signature,
    const std::string& returnType,
    const std::vector<TypedExprPtr>& args,
    const TypeParser* typeParser) {
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

  auto type = typeParser->parse(returnType);
  std::vector<TypedExprPtr> newArgs = {lambda->body()};
  return std::make_shared<CallTypedExpr>(type, newArgs, "try");
}

std::optional<TypedExprPtr> tryConvertLiteralArray(
    const protocol::Signature& signature,
    const std::string& returnType,
    const std::vector<TypedExprPtr>& args,
    velox::memory::MemoryPool* pool,
    const TypeParser* typeParser) {
  static const std::string prestoDefaultNamespacePrefix =
      SystemConfig::instance()->prestoDefaultNamespacePrefix();
  static const char* kLiteralArray = "presto.default.$literal$array";
  static const std::string kFromBase64 = util::addDefaultNamespacePrefix(
      prestoDefaultNamespacePrefix, "from_base64");

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

  auto type = typeParser->parse(returnType);

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

const std::unordered_map<std::string, std::string> prestoOperatorMap() {
  static const std::string prestoDefaultNamespacePrefix =
      SystemConfig::instance()->prestoDefaultNamespacePrefix();
  static const std::unordered_map<std::string, std::string> kPrestoOperatorMap =
      {
          // Operator overrides:
          // com.facebook.presto.common.function.OperatorType
          {"presto.default.$operator$add",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "plus")},
          {"presto.default.$operator$between",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "between")},
          {"presto.default.$operator$divide",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "divide")},
          {"presto.default.$operator$equal",
           util::addDefaultNamespacePrefix(prestoDefaultNamespacePrefix, "eq")},
          {"presto.default.$operator$greater_than",
           util::addDefaultNamespacePrefix(prestoDefaultNamespacePrefix, "gt")},
          {"presto.default.$operator$greater_than_or_equal",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "gte")},
          {"presto.default.$operator$is_distinct_from",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "distinct_from")},
          {"presto.default.$operator$less_than",
           util::addDefaultNamespacePrefix(prestoDefaultNamespacePrefix, "lt")},
          {"presto.default.$operator$less_than_or_equal",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "lte")},
          {"presto.default.$operator$modulus",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "mod")},
          {"presto.default.$operator$multiply",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "multiply")},
          {"presto.default.$operator$negation",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "negate")},
          {"presto.default.$operator$not_equal",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "neq")},
          {"presto.default.$operator$subtract",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "minus")},
          {"presto.default.$operator$subscript",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "subscript")},
          {"presto.default.$operator$xx_hash_64",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "xxhash64_internal")},
          {"presto.default.combine_hash",
           util::addDefaultNamespacePrefix(
               prestoDefaultNamespacePrefix, "combine_hash_internal")},
          // Special form function overrides.
          {"presto.default.in", "in"},
      };
  return kPrestoOperatorMap;
}

std::optional<TypedExprPtr> VeloxExprConverter::tryConvertDate(
    const protocol::CallExpression& pexpr) const {
  static const std::string prestoDefaultNamespacePrefix =
      SystemConfig::instance()->prestoDefaultNamespacePrefix();
  static const std::string kDate =
      util::addDefaultNamespacePrefix(prestoDefaultNamespacePrefix, "date");

  auto builtin = std::static_pointer_cast<protocol::BuiltInFunctionHandle>(
      pexpr.functionHandle);
  auto signature = builtin->signature;
  if (signature.name != kDate) {
    return std::nullopt;
  }

  VELOX_CHECK_EQ(pexpr.arguments.size(), 1);
  std::vector<TypedExprPtr> args;
  // The argument to date function should be an expression that evaluates to
  // a VARCHAR or TIMESTAMP (with an optional timezone) type.
  args.emplace_back(toVeloxExpr(pexpr.arguments[0]));

  auto returnType = typeParser_->parse(pexpr.returnType);
  return std::make_shared<CastTypedExpr>(returnType, args, false);
}

std::optional<TypedExprPtr> VeloxExprConverter::tryConvertLike(
    const protocol::CallExpression& pexpr) const {
  static const std::string prestoDefaultNamespacePrefix =
      SystemConfig::instance()->prestoDefaultNamespacePrefix();
  static const std::string kLike =
      util::addDefaultNamespacePrefix(prestoDefaultNamespacePrefix, "like");
  static const std::string kLikePatternType = util::addDefaultNamespacePrefix(
      prestoDefaultNamespacePrefix, "like_pattern");
  static const char* kLikeReturnType = "LikePattern";
  static const char* kCast = "presto.default.$operator$cast";

  auto builtin = std::static_pointer_cast<protocol::BuiltInFunctionHandle>(
      pexpr.functionHandle);
  auto signature = builtin->signature;
  if (signature.name != kLike) {
    return std::nullopt;
  }

  VELOX_CHECK_EQ(pexpr.arguments.size(), 2);

  std::vector<TypedExprPtr> args;
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
  auto returnType = typeParser_->parse(pexpr.returnType);
  return std::make_shared<CallTypedExpr>(
      returnType, args, getFunctionName(signature));
}

TypedExprPtr VeloxExprConverter::toVeloxExpr(
    const protocol::CallExpression& pexpr) const {
  if (auto builtin = std::dynamic_pointer_cast<protocol::BuiltInFunctionHandle>(
          pexpr.functionHandle)) {
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
    auto signature = builtin->signature;

    auto cast = tryConvertCast(signature, pexpr.returnType, args, typeParser_);
    if (cast.has_value()) {
      return cast.value();
    }

    auto tryExpr =
        tryConvertTry(signature, pexpr.returnType, args, typeParser_);
    if (tryExpr.has_value()) {
      return tryExpr.value();
    }

    auto literal = tryConvertLiteralArray(
        signature, pexpr.returnType, args, pool_, typeParser_);
    if (literal.has_value()) {
      return literal.value();
    }

    auto returnType = typeParser_->parse(pexpr.returnType);
    return std::make_shared<CallTypedExpr>(
        returnType, args, getFunctionName(signature));

  } else if (
      auto sqlFunctionHandle =
          std::dynamic_pointer_cast<protocol::SqlFunctionHandle>(
              pexpr.functionHandle)) {
    auto args = toVeloxExpr(pexpr.arguments);
    auto returnType = typeParser_->parse(pexpr.returnType);
    return std::make_shared<CallTypedExpr>(
        returnType, args, getFunctionName(sqlFunctionHandle->functionId));
  }
#ifdef PRESTO_ENABLE_REMOTE_FUNCTIONS
  else if (
      auto restFunctionHandle =
          std::dynamic_pointer_cast<protocol::RestFunctionHandle>(
              pexpr.functionHandle)) {
    auto args = toVeloxExpr(pexpr.arguments);
    auto returnType = typeParser_->parse(pexpr.returnType);

    functions::remote::rest::PrestoRestFunctionRegistration::getInstance()
        .registerFunction(*restFunctionHandle);
    return std::make_shared<CallTypedExpr>(
        returnType, args, getFunctionName(restFunctionHandle->functionId));
  }
#endif

  VELOX_FAIL("Unsupported function handle: {}", pexpr.functionHandle->_type);
}

std::shared_ptr<const ConstantTypedExpr> VeloxExprConverter::toVeloxExpr(
    std::shared_ptr<protocol::ConstantExpression> pexpr) const {
  const auto type = typeParser_->parse(pexpr->type);
  switch (type->kind()) {
    case TypeKind::ROW:
      [[fallthrough]];
    case TypeKind::ARRAY:
      [[fallthrough]];
    case TypeKind::MAP: {
      auto valueVector =
          protocol::readBlock(type, pexpr->valueBlock.data, pool_);
      return std::make_shared<ConstantTypedExpr>(
          velox::BaseVector::wrapInConstant(1, 0, valueVector));
    }
    default: {
      const auto value = getConstantValue(type, pexpr->valueBlock);

      return std::make_shared<ConstantTypedExpr>(type, value);
    }
  }
}

namespace {
bool isTrueConstant(const TypedExprPtr& expression) {
  if (auto constExpression =
          std::dynamic_pointer_cast<const ConstantTypedExpr>(expression)) {
    return constExpression->type()->kind() == TypeKind::BOOLEAN &&
        constExpression->value().value<bool>();
  }
  return false;
}

std::shared_ptr<const CallTypedExpr> makeEqualsExpr(
    const TypedExprPtr& a,
    const TypedExprPtr& b) {
  static const std::string prestoDefaultNamespacePrefix =
      SystemConfig::instance()->prestoDefaultNamespacePrefix();
  std::vector<TypedExprPtr> inputs{a, b};
  return std::make_shared<CallTypedExpr>(
      velox::BOOLEAN(),
      std::move(inputs),
      util::addDefaultNamespacePrefix(prestoDefaultNamespacePrefix, "eq"));
}

std::shared_ptr<const CastTypedExpr> makeCastExpr(
    const TypedExprPtr& expr,
    const velox::TypePtr& type) {
  std::vector<TypedExprPtr> inputs{expr};
  return std::make_shared<CastTypedExpr>(type, std::move(inputs), false);
}

std::shared_ptr<const CallTypedExpr> convertSwitchExpr(
    const velox::TypePtr& returnType,
    std::vector<TypedExprPtr> args) {
  auto valueExpr = args.front();
  args.erase(args.begin());

  std::vector<TypedExprPtr> inputs;
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

TypedExprPtr convertBindExpr(const std::vector<TypedExprPtr>& args) {
  VELOX_CHECK_GE(
      args.size(), 2, "BIND expression must have at least two arguments");

  // last argument must be a lambda
  auto lambda = std::dynamic_pointer_cast<const LambdaTypedExpr>(args.back());
  VELOX_CHECK(lambda, "Last argument of a BIND must be a lambda expression");

  // replace first N arguments of the lambda with bind variables
  std::unordered_map<std::string, TypedExprPtr> mapping;
  mapping.reserve(args.size() - 1);

  const auto& signature = lambda->signature();

  for (auto i = 0; i < args.size() - 1; i++) {
    mapping.insert({signature->nameOf(i), args[i]});
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

velox::ArrayVectorPtr wrapInArray(const velox::VectorPtr& elements) {
  auto* pool = elements->pool();
  auto size = elements->size();
  auto offsets = velox::allocateOffsets(size, pool);
  auto sizes = velox::allocateSizes(size, pool);

  auto rawSizes = sizes->asMutable<velox::vector_size_t>();
  rawSizes[0] = size;

  return std::make_shared<velox::ArrayVector>(
      pool, ARRAY(elements->type()), nullptr, 1, offsets, sizes, elements);
}

velox::ArrayVectorPtr toArrayOfComplexTypeVector(
    const velox::TypePtr& elementType,
    std::vector<TypedExprPtr>::const_iterator begin,
    std::vector<TypedExprPtr>::const_iterator end,
    velox::memory::MemoryPool* pool) {
  const auto size = end - begin;
  auto elements = velox::BaseVector::create(elementType, size, pool);

  for (auto i = 0; i < size; ++i) {
    auto constant =
        dynamic_cast<const ConstantTypedExpr*>((*(begin + i)).get());
    if (constant == nullptr) {
      return nullptr;
    }
    elements->copy(constant->valueVector().get(), i, 0, 1);
  }

  return wrapInArray(elements);
}

template <TypeKind KIND>
velox::ArrayVectorPtr toArrayVector(
    const velox::TypePtr& elementType,
    std::vector<TypedExprPtr>::const_iterator begin,
    std::vector<TypedExprPtr>::const_iterator end,
    velox::memory::MemoryPool* pool) {
  using T = typename velox::TypeTraits<KIND>::NativeType;

  const auto size = end - begin;
  auto elements = std::dynamic_pointer_cast<velox::FlatVector<T>>(
      velox::BaseVector::create(elementType, size, pool));

  for (auto i = 0; i < size; ++i) {
    auto constant =
        dynamic_cast<const ConstantTypedExpr*>((*(begin + i)).get());
    if (constant == nullptr) {
      return nullptr;
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

  return wrapInArray(elements);
}

TypedExprPtr convertInExpr(
    const std::vector<TypedExprPtr>& args,
    velox::memory::MemoryPool* pool) {
  auto numArgs = args.size();
  VELOX_USER_CHECK_GE(numArgs, 2);

  const auto typeKind = args[0]->type()->kind();

  velox::ArrayVectorPtr arrayVector;
  switch (typeKind) {
    case velox::TypeKind::ARRAY:
      [[fallthrough]];
    case velox::TypeKind::MAP:
      [[fallthrough]];
    case velox::TypeKind::ROW:
      arrayVector = toArrayOfComplexTypeVector(
          args[0]->type(), args.begin() + 1, args.end(), pool);
      break;
    default:
      arrayVector = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          toArrayVector,
          typeKind,
          args[0]->type(),
          args.begin() + 1,
          args.end(),
          pool);
  }

  if (arrayVector == nullptr) {
    return std::make_shared<CallTypedExpr>(velox::BOOLEAN(), args, "in");
  }

  auto constantVector =
      std::make_shared<velox::ConstantVector<velox::ComplexType>>(
          pool, 1, 0, arrayVector);

  std::vector<TypedExprPtr> newArgs = {
      args[0], std::make_shared<const ConstantTypedExpr>(constantVector)};
  return std::make_shared<CallTypedExpr>(velox::BOOLEAN(), newArgs, "in");
}

TypedExprPtr convertDereferenceExpr(
    const velox::TypePtr& returnType,
    const std::vector<TypedExprPtr>& args) {
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

  return std::make_shared<DereferenceTypedExpr>(returnType, input, childIndex);
}
} // namespace

TypedExprPtr VeloxExprConverter::toVeloxExpr(
    std::shared_ptr<protocol::SpecialFormExpression> pexpr) const {
  auto args = toVeloxExpr(pexpr->arguments);

  if (pexpr->form == protocol::Form::BIND) {
    return convertBindExpr(args);
  }

  if (pexpr->form == protocol::Form::IN) {
    return convertInExpr(args, pool_);
  }

  auto returnType = typeParser_->parse(pexpr->returnType);

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

  if (pexpr->form == protocol::Form::NULL_IF) {
    VELOX_UNREACHABLE("NULL_IF not supported in specialForm");
  }

  auto form = std::string(json(pexpr->form));
  return std::make_shared<CallTypedExpr>(
      returnType, args, mapScalarFunction(form));
}

std::shared_ptr<const FieldAccessTypedExpr> VeloxExprConverter::toVeloxExpr(
    std::shared_ptr<protocol::VariableReferenceExpression> pexpr) const {
  return std::make_shared<FieldAccessTypedExpr>(
      typeParser_->parse(pexpr->type), pexpr->name);
}

std::shared_ptr<const LambdaTypedExpr> VeloxExprConverter::toVeloxExpr(
    std::shared_ptr<protocol::LambdaDefinitionExpression> lambda) const {
  std::vector<velox::TypePtr> argumentTypes;
  argumentTypes.reserve(lambda->argumentTypes.size());
  for (auto& typeName : lambda->argumentTypes) {
    argumentTypes.emplace_back(typeParser_->parse(typeName));
  }

  // TODO(spershin): In some cases we can visit this method with the same lambda
  // more than once and having zero arguments and non-zero types would trigger a
  // check down the stack.
  // So, we make sure we don't mutate lambda here, while we investigate how that
  // can happen and validate such behavior or fix a bug.
  auto argCopy = lambda->arguments;
  auto signature = ROW(std::move(argCopy), std::move(argumentTypes));
  return std::make_shared<LambdaTypedExpr>(
      signature, toVeloxExpr(lambda->body));
}

std::shared_ptr<const FieldAccessTypedExpr> VeloxExprConverter::toVeloxExpr(
    const protocol::VariableReferenceExpression& pexpr) const {
  return std::make_shared<FieldAccessTypedExpr>(
      typeParser_->parse(pexpr.type), pexpr.name);
}

TypedExprPtr VeloxExprConverter::toVeloxExpr(
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
