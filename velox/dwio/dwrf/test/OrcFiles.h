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
#include <memory>
#include <string>

#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/type/Type.h"
#include "velox/type/fbhive/HiveTypeParser.h"

namespace facebook::velox::dwrf {

inline const std::string getExampleFilePath(const std::string& fileName) {
  return facebook::velox::test::getDataFilePath(
      "velox/dwio/dwrf/test", "examples/" + fileName);
}

const std::string& getStructFile();

const std::string& getFMSmallFile();

const std::string& getFMLargeFile();

const std::shared_ptr<const RowType>& getFlatmapSchema();

} // namespace facebook::velox::dwrf
