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

#include <string>

#include "velox/type/Type.h"

namespace facebook::velox::connector::hive::iceberg {

struct IcebergMetadataColumn {
  int id;
  std::string name;
  std::shared_ptr<const Type> type;
  std::string doc;

  IcebergMetadataColumn(
      int _id,
      const std::string& _name,
      std::shared_ptr<const Type> _type,
      const std::string& _doc)
      : id(_id), name(_name), type(_type), doc(_doc) {}

  static std::shared_ptr<IcebergMetadataColumn> icebergDeleteFilePathColumn() {
    return std::make_shared<IcebergMetadataColumn>(
        2147483546,
        "file_path",
        VARCHAR(),
        "Path of a file in which a deleted row is stored");
  }

  static std::shared_ptr<IcebergMetadataColumn> icebergDeletePosColumn() {
    return std::make_shared<IcebergMetadataColumn>(
        2147483545,
        "pos",
        BIGINT(),
        "Ordinal position of a deleted row in the data file");
  }
};

} // namespace facebook::velox::connector::hive::iceberg
