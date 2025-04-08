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
#include "velox/python/type/PyType.h"

namespace facebook::velox::py {

class PyFile {
 public:
  PyFile(const std::string& filePath, dwio::common::FileFormat fileFormat)
      : filePath_(filePath), fileFormat_(fileFormat) {}

  PyFile(std::string filePath, std::string formatString);

  std::string toString() const;

  std::string filePath() const {
    return filePath_;
  }

  dwio::common::FileFormat fileFormat() const {
    return fileFormat_;
  }

  /// Returns the schema (RowType) in the given file. This function will open
  /// and read metadata from the file using the corresponding reader.
  PyType getSchema();

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

  bool equals(const PyFile& other) const {
    return filePath_ == other.filePath_ && fileFormat_ == other.fileFormat_;
  }

 private:
  const std::string filePath_;
  const dwio::common::FileFormat fileFormat_;

  // Stores the file schema. Lazily instantiated by getSchema(), as it requires
  // the file to be opened and read.
  TypePtr fileSchema_;
};

} // namespace facebook::velox::py
