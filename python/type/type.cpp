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

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "velox/python/type/PyType.h"

namespace py = pybind11;

PYBIND11_MODULE(type, m) {
  using namespace facebook;

  py::class_<velox::py::PyType>(m, "Type")
      .def("__str__", &velox::py::PyType::toString)
      .def("__eq__", [](velox::py::PyType& u, velox::py::PyType& v) {
        return u.equivalent(v);
      });

  m.def("BIGINT", &velox::py::PyType::createBigint);
  m.def("INTEGER", &velox::py::PyType::createInteger);
  m.def("SMALLINT", &velox::py::PyType::createSmallint);
  m.def("TINYINT", &velox::py::PyType::createTinyint);
  m.def("BOOLEAN", &velox::py::PyType::createBoolean);

  m.def("REAL", &velox::py::PyType::createReal);
  m.def("DOUBLE", &velox::py::PyType::createDouble);

  m.def("VARCHAR", &velox::py::PyType::createVarchar);
  m.def("VARBINARY", &velox::py::PyType::createVarbinary);

  m.def("DATE", &velox::py::PyType::createDate);

  m.def("ARRAY", &velox::py::PyType::createArrayType, py::arg("elements_type"));
  m.def(
      "MAP",
      &velox::py::PyType::createMapType,
      py::arg("key_type"),
      py::arg("value_type"));
  m.def(
      "ROW",
      &velox::py::PyType::createRowType,
      py::arg("names") = std::vector<std::string>{},
      py::arg("types") = std::vector<velox::py::PyType>{});
}
