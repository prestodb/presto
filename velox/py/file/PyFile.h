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
#include "velox/dwio/common/Options.h"

namespace facebook::velox::py {

class PyFile {
 public:
  PyFile(const std::string& filePath, dwio::common::FileFormat fileFormat)
      : filePath_(filePath), fileFormat_(fileFormat) {}

  std::string toString() const;

  static PyFile createParquet(const std::string& filePath) {
    return PyFile(filePath, dwio::common::FileFormat::PARQUET);
  }

  static PyFile createDwrf(const std::string& filePath) {
    return PyFile(filePath, dwio::common::FileFormat::DWRF);
  }

  static PyFile createNimble(const std::string& filePath) {
    return PyFile(filePath, dwio::common::FileFormat::NIMBLE);
  }

  static PyFile createOrc(const std::string& filePath) {
    return PyFile(filePath, dwio::common::FileFormat::ORC);
  }

  static PyFile createJson(const std::string& filePath) {
    return PyFile(filePath, dwio::common::FileFormat::JSON);
  }

  static PyFile createText(const std::string& filePath) {
    return PyFile(filePath, dwio::common::FileFormat::TEXT);
  }

 private:
  const std::string filePath_;
  const dwio::common::FileFormat fileFormat_;
};

} // namespace facebook::velox::py
