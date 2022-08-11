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

#include "velox/substrait/VeloxToSubstraitType.h"

#include "velox/expression/Expr.h"

namespace facebook::velox::substrait {

const ::substrait::Type& VeloxToSubstraitTypeConvertor::toSubstraitType(
    google::protobuf::Arena& arena,
    const velox::TypePtr& type) {
  ::substrait::Type* substraitType =
      google::protobuf::Arena::CreateMessage<::substrait::Type>(&arena);
  switch (type->kind()) {
    case velox::TypeKind::BOOLEAN: {
      auto substraitBool =
          google::protobuf::Arena::CreateMessage<::substrait::Type_Boolean>(
              &arena);
      substraitBool->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_bool_(substraitBool);

      break;
    }
    case velox::TypeKind::TINYINT: {
      auto substraitI8 =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I8>(&arena);
      substraitI8->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_i8(substraitI8);
      break;
    }
    case velox::TypeKind::SMALLINT: {
      auto substraitI16 =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I16>(&arena);
      substraitI16->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_i16(substraitI16);
      break;
    }
    case velox::TypeKind::INTEGER: {
      auto substraitI32 =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I32>(&arena);
      substraitI32->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_i32(substraitI32);
      break;
    }
    case velox::TypeKind::BIGINT: {
      auto substraitI64 =
          google::protobuf::Arena::CreateMessage<::substrait::Type_I64>(&arena);
      substraitI64->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_i64(substraitI64);
      break;
    }
    case velox::TypeKind::REAL: {
      auto substraitFp32 =
          google::protobuf::Arena::CreateMessage<::substrait::Type_FP32>(
              &arena);
      substraitFp32->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_fp32(substraitFp32);
      break;
    }
    case velox::TypeKind::DOUBLE: {
      auto substraitFp64 =
          google::protobuf::Arena::CreateMessage<::substrait::Type_FP64>(
              &arena);
      substraitFp64->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_fp64(substraitFp64);
      break;
    }
    case velox::TypeKind::VARCHAR: {
      auto substraitString =
          google::protobuf::Arena::CreateMessage<::substrait::Type_String>(
              &arena);
      substraitString->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_string(substraitString);
      break;
    }
    case velox::TypeKind::VARBINARY: {
      auto substraitVarBinary =
          google::protobuf::Arena::CreateMessage<::substrait::Type_Binary>(
              &arena);
      substraitVarBinary->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_binary(substraitVarBinary);
      break;
    }
    case velox::TypeKind::TIMESTAMP: {
      auto substraitTimestamp =
          google::protobuf::Arena::CreateMessage<::substrait::Type_Timestamp>(
              &arena);
      substraitTimestamp->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_timestamp(substraitTimestamp);
      break;
    }
    case velox::TypeKind::ARRAY: {
      ::substrait::Type_List* substraitList =
          google::protobuf::Arena::CreateMessage<::substrait::Type_List>(
              &arena);

      substraitList->mutable_type()->MergeFrom(
          toSubstraitType(arena, type->asArray().elementType()));

      substraitList->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);

      substraitType->set_allocated_list(substraitList);

      break;
    }
    case velox::TypeKind::MAP: {
      ::substrait::Type_Map* substraitMap =
          google::protobuf::Arena::CreateMessage<::substrait::Type_Map>(&arena);

      substraitMap->mutable_key()->MergeFrom(
          toSubstraitType(arena, type->asMap().keyType()));
      substraitMap->mutable_value()->MergeFrom(
          toSubstraitType(arena, type->asMap().valueType()));

      substraitMap->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);

      substraitType->set_allocated_map(substraitMap);

      break;
    }
    case velox::TypeKind::ROW: {
      ::substrait::Type_Struct* substraitStruct =
          google::protobuf::Arena::CreateMessage<::substrait::Type_Struct>(
              &arena);
      for (const auto& child : type->asRow().children()) {
        substraitStruct->set_nullability(
            ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
        substraitStruct->add_types()->MergeFrom(toSubstraitType(arena, child));
      }
      substraitStruct->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_struct_(substraitStruct);
      break;
    }
    case velox::TypeKind::UNKNOWN: {
      auto substraitUserDefined =
          google::protobuf::Arena::CreateMessage<::substrait::Type_UserDefined>(
              &arena);
      substraitUserDefined->set_type_reference(0);
      substraitUserDefined->set_nullability(
          ::substrait::Type_Nullability_NULLABILITY_NULLABLE);
      substraitType->set_allocated_user_defined(substraitUserDefined);
      break;
    }
    case velox::TypeKind::FUNCTION:
    case velox::TypeKind::OPAQUE:
    case velox::TypeKind::INVALID:
    default:
      VELOX_UNSUPPORTED("Unsupported velox type '{}'", type->toString());
  }
  return *substraitType;
}

const ::substrait::NamedStruct&
VeloxToSubstraitTypeConvertor::toSubstraitNamedStruct(
    google::protobuf::Arena& arena,
    const velox::RowTypePtr& rowType) {
  ::substrait::NamedStruct* substraitNamedStruct =
      google::protobuf::Arena::CreateMessage<::substrait::NamedStruct>(&arena);

  const auto size = rowType->size();
  if (size != 0) {
    const auto& names = rowType->names();
    const auto& veloxTypes = rowType->children();

    auto substraitType = substraitNamedStruct->mutable_struct_();

    substraitType->set_nullability(
        ::substrait::Type_Nullability_NULLABILITY_NULLABLE);

    for (int64_t i = 0; i < size; ++i) {
      const auto& name = names.at(i);
      const auto& veloxType = veloxTypes.at(i);
      substraitNamedStruct->add_names(name);

      substraitType->add_types()->MergeFrom(toSubstraitType(arena, veloxType));
    }
  }

  return *substraitNamedStruct;
}

} // namespace facebook::velox::substrait
