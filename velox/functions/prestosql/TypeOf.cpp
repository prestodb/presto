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
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/types/HyperLogLogType.h"
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/functions/prestosql/types/UuidType.h"

namespace facebook::velox::functions {
namespace {

std::string typeName(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return "boolean";
    case TypeKind::TINYINT:
      return "tinyint";
    case TypeKind::SMALLINT:
      return "smallint";
    case TypeKind::INTEGER:
      if (type->isDate()) {
        return "date";
      }
      if (type->isIntervalYearMonth()) {
        return "interval year to month";
      }
      return "integer";
    case TypeKind::BIGINT:
      if (isTimestampWithTimeZoneType(type)) {
        return "timestamp with time zone";
      }
      if (type->isIntervalDayTime()) {
        return "interval day to second";
      }
      if (type->isDecimal()) {
        const auto& shortDecimal = type->asShortDecimal();
        return fmt::format(
            "decimal({},{})", shortDecimal.precision(), shortDecimal.scale());
      }
      return "bigint";
    case TypeKind::HUGEINT: {
      if (isUuidType(type)) {
        return "uuid";
      } else if (isIPAddressType(type)) {
        return "ipaddress";
      }
      VELOX_USER_CHECK(
          type->isDecimal(),
          "Expected decimal type. Got: {}",
          type->toString());
      const auto& longDecimal = type->asLongDecimal();
      return fmt::format(
          "decimal({},{})", longDecimal.precision(), longDecimal.scale());
    }
    case TypeKind::REAL:
      return "real";
    case TypeKind::DOUBLE:
      return "double";
    case TypeKind::VARCHAR:
      if (isJsonType(type)) {
        return "json";
      }
      return "varchar";
    case TypeKind::VARBINARY:
      if (isHyperLogLogType(type)) {
        return "HyperLogLog";
      }
      return "varbinary";
    case TypeKind::TIMESTAMP:
      return "timestamp";
    case TypeKind::ARRAY:
      return fmt::format("array({})", typeName(type->childAt(0)));
    case TypeKind::MAP:
      return fmt::format(
          "map({}, {})",
          typeName(type->childAt(0)),
          typeName(type->childAt(1)));
    case TypeKind::ROW: {
      const auto& rowType = type->asRow();
      std::ostringstream out;
      out << "row(";
      for (auto i = 0; i < type->size(); ++i) {
        if (i > 0) {
          out << ", ";
        }
        if (!rowType.nameOf(i).empty()) {
          out << "\"" << rowType.nameOf(i) << "\" ";
        }
        out << typeName(type->childAt(i));
      }
      out << ")";
      return out.str();
    }
    case TypeKind::UNKNOWN:
      return "unknown";
    default:
      VELOX_UNSUPPORTED("Unsupported type: {}", type->toString());
  }
}

class TypeOfFunction : public exec::VectorFunction {
 public:
  TypeOfFunction(const TypePtr& type) : typeName_{typeName(type)} {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto localResult = BaseVector::createConstant(
        VARCHAR(), typeName_, rows.size(), context.pool());
    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T -> varchar
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("varchar")
                .argumentType("T")
                .build()};
  }

  static std::shared_ptr<exec::VectorFunction> create(
      const std::string& /*name*/,
      const std::vector<exec::VectorFunctionArg>& inputArgs,
      const core::QueryConfig& /*config*/) {
    try {
      return std::make_shared<TypeOfFunction>(inputArgs[0].type);
    } catch (...) {
      return std::make_shared<exec::AlwaysFailingVectorFunction>(
          std::current_exception());
    }
  }

 private:
  const std::string typeName_;
};
} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION_WITH_METADATA(
    udf_typeof,
    TypeOfFunction::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    TypeOfFunction::create);

} // namespace facebook::velox::functions
