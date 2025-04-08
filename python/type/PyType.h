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

#include <pybind11/embed.h>
#include "velox/type/Type.h"

namespace facebook::velox::py {

class PyType {
 public:
  explicit PyType(const TypePtr& type = nullptr) : type_(type) {}

  std::string toString() const {
    if (!type_) {
      return "[nullptr]";
    }
    return type_->toString();
  }

  TypePtr type() const {
    return type_;
  }

  bool equivalent(const PyType& other) const {
    return type_->equivalent(*other.type());
  }

  // Factory functions:

  static PyType createRowType(
      const std::vector<std::string>& names,
      const std::vector<PyType>& pyTypes) {
    std::vector<TypePtr> types;
    for (const auto& pyType : pyTypes) {
      types.emplace_back(pyType.type());
    }

    if (names.empty()) {
      return PyType{ROW(std::move(types))};
    }
    return PyType{ROW(folly::copy(names), std::move(types))};
  }

  static PyType createMapType(const PyType& keyType, const PyType& valueType) {
    return PyType{MAP(keyType.type(), valueType.type())};
  }

  static PyType createArrayType(const PyType& elementsType) {
    return PyType{ARRAY(elementsType.type())};
  }

  static PyType createBigint() {
    return PyType{BIGINT()};
  }

  static PyType createInteger() {
    return PyType{INTEGER()};
  }

  static PyType createSmallint() {
    return PyType{SMALLINT()};
  }

  static PyType createTinyint() {
    return PyType{TINYINT()};
  }

  static PyType createBoolean() {
    return PyType{BOOLEAN()};
  }

  static PyType createReal() {
    return PyType{REAL()};
  }

  static PyType createDouble() {
    return PyType{DOUBLE()};
  }

  static PyType createDate() {
    return PyType{DATE()};
  }

  static PyType createVarchar() {
    return PyType{VARCHAR()};
  }

  static PyType createVarbinary() {
    return PyType{VARBINARY()};
  }

 private:
  TypePtr type_;
};

} // namespace facebook::velox::py
