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

#include "velox/py/file/PyFile.h"

namespace py = pybind11;

PYBIND11_MODULE(file, m) {
  using namespace facebook;

  // File wrapper abstraction.
  py::class_<velox::py::PyFile>(m, "File")
      .def(
          py::init<const std::string&, const std::string&>(),
          py::arg("path"),
          py::arg("format_str"),
          py::doc(R"(
        Constructs a File object given a path and a file format string.

        Args:
          path: The file or directory path.
          aliases: A string containing the lowercase name of the format.
      )"))
      .def("__eq__", &velox::py::PyFile::equals, py::doc(R"(
        Returns if two PyFiles are the same.
      )"))
      .def("__str__", &velox::py::PyFile::toString, py::doc(R"(
        Returns a short and recursive description of the file.
      )"));

  m.def("PARQUET", &velox::py::PyFile::createParquet);
  m.def("DWRF", &velox::py::PyFile::createDwrf);
  m.def("NIMBLE", &velox::py::PyFile::createNimble);
  m.def("ORC", &velox::py::PyFile::createOrc);
  m.def("JSON", &velox::py::PyFile::createJson);
  m.def("TEXT", &velox::py::PyFile::createText);
}
