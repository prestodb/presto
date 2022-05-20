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
      substraitType->set_allocated_bool_(
          google::protobuf::Arena::CreateMessage<::substrait::Type_Boolean>(
              &arena));
      break;
    }
    case velox::TypeKind::TINYINT: {
      substraitType->set_allocated_i8(
          google::protobuf::Arena::CreateMessage<::substrait::Type_I8>(&arena));
      break;
    }
    case velox::TypeKind::SMALLINT: {
      substraitType->set_allocated_i16(
          google::protobuf::Arena::CreateMessage<::substrait::Type_I16>(
              &arena));
      break;
    }
    case velox::TypeKind::INTEGER: {
      substraitType->set_allocated_i32(
          google::protobuf::Arena::CreateMessage<::substrait::Type_I32>(
              &arena));
      break;
    }
    case velox::TypeKind::BIGINT: {
      substraitType->set_allocated_i64(
          google::protobuf::Arena::CreateMessage<::substrait::Type_I64>(
              &arena));
      break;
    }
    case velox::TypeKind::REAL: {
      substraitType->set_allocated_fp32(
          google::protobuf::Arena::CreateMessage<::substrait::Type_FP32>(
              &arena));
      break;
    }
    case velox::TypeKind::DOUBLE: {
      substraitType->set_allocated_fp64(
          google::protobuf::Arena::CreateMessage<::substrait::Type_FP64>(
              &arena));
      break;
    }
    case velox::TypeKind::VARCHAR: {
      substraitType->set_allocated_varchar(
          google::protobuf::Arena::CreateMessage<::substrait::Type_VarChar>(
              &arena));
      break;
    }
    case velox::TypeKind::VARBINARY: {
      substraitType->set_allocated_binary(
          google::protobuf::Arena::CreateMessage<::substrait::Type_Binary>(
              &arena));
      break;
    }
    case velox::TypeKind::TIMESTAMP: {
      substraitType->set_allocated_timestamp(
          google::protobuf::Arena::CreateMessage<::substrait::Type_Timestamp>(
              &arena));
      break;
    }
    case velox::TypeKind::ARRAY: {
      ::substrait::Type_List* sTList =
          google::protobuf::Arena::CreateMessage<::substrait::Type_List>(
              &arena);

      sTList->mutable_type()->MergeFrom(
          toSubstraitType(arena, type->asArray().elementType()));

      substraitType->set_allocated_list(sTList);

      break;
    }
    case velox::TypeKind::MAP: {
      ::substrait::Type_Map* sMap =
          google::protobuf::Arena::CreateMessage<::substrait::Type_Map>(&arena);

      sMap->mutable_key()->MergeFrom(
          toSubstraitType(arena, type->asMap().keyType()));
      sMap->mutable_value()->MergeFrom(
          toSubstraitType(arena, type->asMap().valueType()));

      substraitType->set_allocated_map(sMap);

      break;
    }
    case velox::TypeKind::UNKNOWN:
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

  const int64_t size = rowType->size();
  const std::vector<std::string>& names = rowType->names();
  const std::vector<TypePtr>& veloxTypes = rowType->children();
  ::substrait::Type_Struct* substraitType =
      substraitNamedStruct->mutable_struct_();

  for (int64_t i = 0; i < size; ++i) {
    const std::string& name = names.at(i);
    const TypePtr& veloxType = veloxTypes.at(i);
    substraitNamedStruct->add_names(name);

    substraitType->add_types()->MergeFrom(toSubstraitType(arena, veloxType));
  }
  return *substraitNamedStruct;
}

} // namespace facebook::velox::substrait
