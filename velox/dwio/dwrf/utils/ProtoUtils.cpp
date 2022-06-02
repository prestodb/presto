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

#include "velox/dwio/dwrf/utils/ProtoUtils.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook::velox::dwrf {

namespace {

template <TypeKind T>
class SchemaType {};

#define CREATE_TYPE_TRAIT(Kind, SchemaKind)       \
  template <>                                     \
  struct SchemaType<TypeKind::Kind> {             \
    static constexpr proto::Type_Kind kind =      \
        proto::Type_Kind::Type_Kind_##SchemaKind; \
  };

CREATE_TYPE_TRAIT(BOOLEAN, BOOLEAN)
CREATE_TYPE_TRAIT(TINYINT, BYTE)
CREATE_TYPE_TRAIT(SMALLINT, SHORT)
CREATE_TYPE_TRAIT(INTEGER, INT)
CREATE_TYPE_TRAIT(BIGINT, LONG)
CREATE_TYPE_TRAIT(REAL, FLOAT)
CREATE_TYPE_TRAIT(DOUBLE, DOUBLE)
CREATE_TYPE_TRAIT(VARCHAR, STRING)
CREATE_TYPE_TRAIT(VARBINARY, BINARY)
CREATE_TYPE_TRAIT(TIMESTAMP, TIMESTAMP)
CREATE_TYPE_TRAIT(INTERVAL_DAY_TIME, LONG)
CREATE_TYPE_TRAIT(ARRAY, LIST)
CREATE_TYPE_TRAIT(MAP, MAP)
CREATE_TYPE_TRAIT(ROW, STRUCT)

#undef CREATE_TYPE_TRAIT

} // namespace

void ProtoUtils::writeType(
    const Type& type,
    proto::Footer& footer,
    proto::Type* parent) {
  auto self = footer.add_types();
  if (parent) {
    parent->add_subtypes(footer.types_size() - 1);
  }
  auto kind =
      VELOX_STATIC_FIELD_DYNAMIC_DISPATCH(SchemaType, kind, type.kind());
  self->set_kind(kind);
  switch (type.kind()) {
    case TypeKind::ROW: {
      auto& row = type.asRow();
      for (size_t i = 0; i < row.size(); ++i) {
        self->add_fieldnames(row.nameOf(i));
        writeType(*row.childAt(i), footer, self);
      }
      break;
    }
    case TypeKind::ARRAY:
      writeType(*type.asArray().elementType(), footer, self);
      break;
    case TypeKind::MAP: {
      auto& map = type.asMap();
      writeType(*map.keyType(), footer, self);
      writeType(*map.valueType(), footer, self);
      break;
    }
    default:
      DWIO_ENSURE(type.isPrimitiveType());
      break;
  }
}

std::shared_ptr<const Type> ProtoUtils::fromFooter(
    const proto::Footer& footer,
    std::function<bool(uint32_t)> selector,
    uint32_t index) {
  const auto& type = footer.types(index);
  switch (type.kind()) {
    case proto::Type_Kind_BOOLEAN:
    case proto::Type_Kind_BYTE:
    case proto::Type_Kind_SHORT:
    case proto::Type_Kind_INT:
    case proto::Type_Kind_LONG:
    case proto::Type_Kind_FLOAT:
    case proto::Type_Kind_DOUBLE:
    case proto::Type_Kind_STRING:
    case proto::Type_Kind_BINARY:
    case proto::Type_Kind_TIMESTAMP:
      return createScalarType(static_cast<TypeKind>(type.kind()));
    case proto::Type_Kind_LIST:
      return ARRAY(fromFooter(footer, selector, type.subtypes(0)));
    case proto::Type_Kind_MAP:
      return MAP(
          fromFooter(footer, selector, type.subtypes(0)),
          fromFooter(footer, selector, type.subtypes(1)));
    case proto::Type_Kind_UNION: {
      DWIO_RAISE("union type is deprecated");
    }
    case proto::Type_Kind_STRUCT: {
      std::vector<std::shared_ptr<const Type>> tl;
      std::vector<std::string> names;
      for (int32_t i = 0; i < type.subtypes_size(); ++i) {
        auto typeId = type.subtypes(i);
        if (selector(typeId)) {
          auto child = fromFooter(footer, selector, typeId);
          names.push_back(type.fieldnames(i));
          tl.push_back(std::move(child));
        }
      }
      // NOTE: There are empty dwrf files in data warehouse that has empty
      // struct as the root type. So the assumption that struct has at least one
      // child doesn't hold.
      return ROW(std::move(names), std::move(tl));
    }
    default:
      DWIO_RAISE("unknown type");
  }
}

} // namespace facebook::velox::dwrf
