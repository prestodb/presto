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

#include "velox/substrait/VeloxToSubstraitExpr.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::substrait {

namespace {
std::shared_ptr<VeloxToSubstraitExprConvertor> exprConvertor_;

template <TypeKind sourceKind>
void convertVectorValue(
    google::protobuf::Arena& arena,
    const velox::VectorPtr& vectorValue,
    ::substrait::Expression_Literal_Struct* litValue,
    ::substrait::Expression_Literal* substraitField) {
  const TypePtr& childType = vectorValue->type();

  using T = typename TypeTraits<sourceKind>::NativeType;

  auto childToFlatVec = vectorValue->asFlatVector<T>();

  //  Get the batchSize and convert each value in it.
  vector_size_t flatVecSize = childToFlatVec->size();
  for (int64_t i = 0; i < flatVecSize; i++) {
    substraitField = litValue->add_fields();
    if (childToFlatVec->isNullAt(i)) {
      // Process the null value.
      substraitField->MergeFrom(
          exprConvertor_->toSubstraitNullLiteral(arena, childType->kind()));
    } else {
      substraitField->MergeFrom(exprConvertor_->toSubstraitNotNullLiteral(
          arena, static_cast<const variant>(childToFlatVec->valueAt(i))));
    }
  }
}

} // namespace

const ::substrait::Expression& VeloxToSubstraitExprConvertor::toSubstraitExpr(
    google::protobuf::Arena& arena,
    const core::TypedExprPtr& expr,
    const RowTypePtr& inputType) {
  ::substrait::Expression* substraitExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression>(&arena);
  if (auto constExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(expr)) {
    substraitExpr->mutable_literal()->MergeFrom(
        toSubstraitExpr(arena, constExpr));
    return *substraitExpr;
  }
  if (auto callTypeExpr =
          std::dynamic_pointer_cast<const core::CallTypedExpr>(expr)) {
    substraitExpr->MergeFrom(toSubstraitExpr(arena, callTypeExpr, inputType));
    return *substraitExpr;
  }
  if (auto fieldExpr =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expr)) {
    substraitExpr->mutable_selection()->MergeFrom(
        toSubstraitExpr(arena, fieldExpr, inputType));

    return *substraitExpr;
  }
  if (auto castExpr =
          std::dynamic_pointer_cast<const core::CastTypedExpr>(expr)) {
    substraitExpr->mutable_cast()->MergeFrom(
        toSubstraitExpr(arena, castExpr, inputType));

    return *substraitExpr;
  }

  VELOX_UNSUPPORTED("Unsupport Expr '{}' in Substrait", expr->toString());
}

const ::substrait::Expression_Cast&
VeloxToSubstraitExprConvertor::toSubstraitExpr(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::CastTypedExpr>& castExpr,
    const RowTypePtr& inputType) {
  ::substrait::Expression_Cast* substraitCastExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Cast>(
          &arena);
  std::vector<core::TypedExprPtr> castExprInputs = castExpr->inputs();

  substraitCastExpr->mutable_type()->MergeFrom(
      typeConvertor_->toSubstraitType(arena, castExpr->type()));

  for (auto& arg : castExprInputs) {
    substraitCastExpr->mutable_input()->MergeFrom(
        toSubstraitExpr(arena, arg, inputType));
  }
  return *substraitCastExpr;
}

const ::substrait::Expression_FieldReference&
VeloxToSubstraitExprConvertor::toSubstraitExpr(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::FieldAccessTypedExpr>& fieldExpr,
    const RowTypePtr& inputType) {
  ::substrait::Expression_FieldReference* substraitFieldExpr =
      google::protobuf::Arena::CreateMessage<
          ::substrait::Expression_FieldReference>(&arena);

  std::string exprName = fieldExpr->name();

  ::substrait::Expression_ReferenceSegment_StructField* directStruct =
      substraitFieldExpr->mutable_direct_reference()->mutable_struct_field();

  directStruct->set_field(inputType->getChildIdx(exprName));

  return *substraitFieldExpr;
}

const ::substrait::Expression& VeloxToSubstraitExprConvertor::toSubstraitExpr(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::CallTypedExpr>& callTypeExpr,
    const RowTypePtr& inputType) {
  ::substrait::Expression* substraitExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression>(&arena);

  auto inputs = callTypeExpr->inputs();
  auto& functionName = callTypeExpr->name();

  // The processing is different for different function names.
  // TODO add support for if Expr and switch Expr
  if (functionName != "if" && functionName != "switch") {
    ::substrait::Expression_ScalarFunction* scalarExpr =
        substraitExpr->mutable_scalar_function();

    // TODO need to change yaml file to register function, now is dummy.

    scalarExpr->set_function_reference(functionMap_[functionName]);

    for (auto& arg : inputs) {
      scalarExpr->add_args()->MergeFrom(toSubstraitExpr(arena, arg, inputType));
    }

    scalarExpr->mutable_output_type()->MergeFrom(
        typeConvertor_->toSubstraitType(arena, callTypeExpr->type()));

  } else {
    VELOX_NYI("Unsupported function name '{}'", functionName);
  }

  return *substraitExpr;
}

const ::substrait::Expression_Literal&
VeloxToSubstraitExprConvertor::toSubstraitExpr(
    google::protobuf::Arena& arena,
    const std::shared_ptr<const core::ConstantTypedExpr>& constExpr,
    ::substrait::Expression_Literal_Struct* litValue) {
  if (constExpr->hasValueVector()) {
    return toSubstraitLiteral(arena, constExpr->valueVector(), litValue);
  } else {
    return toSubstraitLiteral(arena, constExpr->value());
  }
}

const ::substrait::Expression_Literal&
VeloxToSubstraitExprConvertor::toSubstraitLiteral(
    google::protobuf::Arena& arena,
    const velox::variant& variantValue) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);

  if (variantValue.isNull()) {
    literalExpr->MergeFrom(toSubstraitNullLiteral(arena, variantValue.kind()));
  } else {
    literalExpr->MergeFrom(toSubstraitNotNullLiteral(arena, variantValue));
  }
  return *literalExpr;
}

const ::substrait::Expression_Literal&
VeloxToSubstraitExprConvertor::toSubstraitLiteral(
    google::protobuf::Arena& arena,
    const velox::VectorPtr& vectorValue,
    ::substrait::Expression_Literal_Struct* litValue) {
  ::substrait::Expression_Literal* substraitField =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);

  VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
      convertVectorValue,
      vectorValue->type()->kind(),
      arena,
      vectorValue,
      litValue,
      substraitField);

  return *substraitField;
}

const ::substrait::Expression_Literal&
VeloxToSubstraitExprConvertor::toSubstraitNotNullLiteral(
    google::protobuf::Arena& arena,
    const velox::variant& variantValue) {
  ::substrait::Expression_Literal* literalExpr =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  switch (variantValue.kind()) {
    case velox::TypeKind::DOUBLE: {
      literalExpr->set_fp64(variantValue.value<TypeKind::DOUBLE>());
      break;
    }
    case velox::TypeKind::BIGINT: {
      literalExpr->set_i64(variantValue.value<TypeKind::BIGINT>());
      break;
    }
    case velox::TypeKind::INTEGER: {
      literalExpr->set_i32(variantValue.value<TypeKind::INTEGER>());
      break;
    }
    case velox::TypeKind::BOOLEAN: {
      literalExpr->set_boolean(variantValue.value<TypeKind::BOOLEAN>());
      break;
    }
    default:
      VELOX_NYI(
          "Unsupported constant Type '{}' ",
          mapTypeKindToName(variantValue.kind()));
  }

  literalExpr->set_nullable(false);

  return *literalExpr;
}

const ::substrait::Expression_Literal&
VeloxToSubstraitExprConvertor::toSubstraitNullLiteral(
    google::protobuf::Arena& arena,
    const velox::TypeKind& typeKind) {
  ::substrait::Expression_Literal* substraitField =
      google::protobuf::Arena::CreateMessage<::substrait::Expression_Literal>(
          &arena);
  switch (typeKind) {
    case velox::TypeKind::BOOLEAN: {
      ::substrait::Type_Boolean* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_Boolean>(
              &arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_bool_(nullValue);
      break;
    }
    case velox::TypeKind::TINYINT: {
      ::substrait::Type_I8* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I8>(&arena);

      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_i8(nullValue);
      break;
    }
    case velox::TypeKind::SMALLINT: {
      ::substrait::Type_I16* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I16>(&arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_i16(nullValue);
      break;
    }
    case velox::TypeKind::INTEGER: {
      ::substrait::Type_I32* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I32>(&arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_i32(nullValue);
      break;
    }
    case velox::TypeKind::BIGINT: {
      ::substrait::Type_I64* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I64>(&arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_i64(nullValue);
      break;
    }
    case velox::TypeKind::VARCHAR: {
      ::substrait::Type_VarChar* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_VarChar>(
              &arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_varchar(nullValue);
      break;
    }
    case velox::TypeKind::REAL: {
      ::substrait::Type_FP32* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_FP32>(
              &arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_fp32(nullValue);
      break;
    }
    case velox::TypeKind::DOUBLE: {
      ::substrait::Type_FP64* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_FP64>(
              &arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitField->mutable_null()->set_allocated_fp64(nullValue);
      break;
    }
    case velox::TypeKind::UNKNOWN: {
      ::substrait::Type_UserDefined* nullValue =
          google::protobuf::Arena::CreateMessage<::substrait::Type_UserDefined>(
              &arena);
      nullValue->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      nullValue->set_type_reference(0);
      substraitField->mutable_null()->set_allocated_user_defined(nullValue);

      break;
    }
    default: {
      VELOX_UNSUPPORTED("Unsupported type '{}'", mapTypeKindToName(typeKind));
    }
  }
  substraitField->set_nullable(true);
  return *substraitField;
}

} // namespace facebook::velox::substrait
