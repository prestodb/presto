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

#pragma once

#include "velox/row/UnsafeRow.h"

namespace facebook::velox::row {

struct UnsafeRowStaticUtilities {
  /**
   * @tparam SqlType
   * @return whether the type is fixed width.
   */
  template <typename SqlType>
  constexpr static inline bool isFixedWidth() {
    return TypeTraits<simpleSqlTypeToTypeKind<SqlType>()>::isFixedWidth;
  }

  /**
   * Converts the Velox simple SqlType to TypeKind. We do not need to use a
   * templated function to convert complex types (e.g. Array, Map) because
   * they are already templated.
   * @tparam SqlType
   * @return TypeKind
   */
  template <typename SqlType>
  constexpr static inline TypeKind simpleSqlTypeToTypeKind() {
    if constexpr (std::is_same_v<SqlType, BooleanType>) {
      return TypeKind::BOOLEAN;
    } else if constexpr (std::is_same_v<SqlType, TinyintType>) {
      return TypeKind::TINYINT;
    } else if constexpr (std::is_same_v<SqlType, SmallintType>) {
      return TypeKind::SMALLINT;
    } else if constexpr (std::is_same_v<SqlType, IntegerType>) {
      return TypeKind::INTEGER;
    } else if constexpr (std::is_same_v<SqlType, BigintType>) {
      return TypeKind::BIGINT;
    } else if constexpr (std::is_same_v<SqlType, RealType>) {
      return TypeKind::REAL;
    } else if constexpr (std::is_same_v<SqlType, DoubleType>) {
      return TypeKind::DOUBLE;
    } else if constexpr (std::is_same_v<SqlType, VarcharType>) {
      return TypeKind::VARCHAR;
    } else if constexpr (std::is_same_v<SqlType, VarbinaryType>) {
      return TypeKind::VARBINARY;
    } else if constexpr (std::is_same_v<SqlType, TimestampType>) {
      return TypeKind::TIMESTAMP;
    } else {
      return TypeKind::INVALID;
    }
  }
};

/**
 * A templated UnsafeRow parser.
 * @tparam SqlTypes The row schema.
 */
template <typename... SqlTypes>
struct UnsafeRowStaticParser {
  template <size_t idx>
  using type_at =
      typename std::tuple_element<idx, std::tuple<SqlTypes...>>::type;

  /**
   * The const UnsafeRow for parsing.
   */
  const UnsafeRow row;

  UnsafeRowStaticParser<SqlTypes...>(std::string_view data)
      : row(UnsafeRow(
            const_cast<char*>(data.data()),
            std::tuple_size<std::tuple<SqlTypes...>>::value)) {}

  /**
   *
   * @tparam idx
   * @return
   */
  template <size_t idx>
  const std::string_view dataAt() const {
    using CurrentType = type_at<idx>;

    constexpr bool isFixedWidth =
        UnsafeRowStaticUtilities::isFixedWidth<CurrentType>();
    using NativeType =
        typename TypeTraits<UnsafeRowStaticUtilities::simpleSqlTypeToTypeKind<
            CurrentType>()>::NativeType;
    if constexpr (!std::is_same_v<NativeType, void>) {
      return row.readDataAt(idx, isFixedWidth, sizeof(NativeType));
    }
    return row.readDataAt(idx, isFixedWidth);
  }

  /**
   * @param idx
   * @return whether the element is null at the given index
   */
  bool isNullAt(size_t idx) const {
    return row.isNullAt(idx);
  }
};

/**
 * A dynamic UnsafeRowParser that uses TypePtr.
 */
struct UnsafeRowDynamicParser {
  /**
   * The row type schema.
   */
  std::vector<TypePtr> types;

  /**
   * The const UnsafeRow being parsed.
   */
  const UnsafeRow row;

  UnsafeRowDynamicParser(std::vector<TypePtr>& types, std::string_view& data)
      : types(types),
        row(UnsafeRow(const_cast<char*>(data.data()), types.size())) {}

  /**
   * @param idx
   * @return the unsafe row data at the given index.
   */
  const std::string_view dataAt(size_t idx) const {
    return row.readDataAt(idx, types.at(idx));
  }

  /**
   * @param idx
   * @return the element type at the given index.
   */
  const TypePtr typeAt(size_t idx) const {
    return types.at(idx);
  }

  /**
   * @param idx
   * @return whether the element is null at the given index
   */
  bool isNullAt(size_t idx) const {
    return row.isNullAt(idx);
  }
};

} // namespace facebook::velox::row
