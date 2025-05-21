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

#include "velox/python/init/PyInit.h"
#include "velox/python/vector/PyVector.h"

namespace py = pybind11;

PYBIND11_MODULE(vector, m) {
  using namespace facebook;

  py::module::import("pyvelox.type");

  py::class_<velox::py::PyVector>(m, "Vector")
      .def("__str__", &velox::py::PyVector::toString, py::doc(R"(
        Returns a summarized description of the Vector and its type.
      )"))
      .def("__getitem__", &velox::py::PyVector::operator[], py::doc(R"(
        Returns a the value of an element serialized as a string.
      )"))
      .def("__len__", &velox::py::PyVector::size, py::doc(R"(
        Number of elements in the Vector.
      )"))
      .def("__eq__", &velox::py::PyVector::equals, py::doc(R"(
        Returns if two PyVectors have the same size and contents.
      )"))
      .def("type", &velox::py::PyVector::type, py::doc(R"(
        Returns the Type of the Vector.
      )"))
      .def("size", &velox::py::PyVector::size, py::doc(R"(
        Number of elements in the Vector.
      )"))
      .def("child_at", &velox::py::PyVector::childAt, py::doc(R"(
        Returns the vector's child at position `idx`. Throws if the
        vector is not a RowVector.

        Args:
          index: The index of the child element in the RowVector.
      )"))
      .def("null_count", &velox::py::PyVector::nullCount, py::doc(R"(
        Number of null elements in the Vector.
      )"))
      .def("is_null_at", &velox::py::PyVector::isNullAt, py::doc(R"(
        If the Vector has a null element at `idx`.

        Args:
          index: The vector element to check.
      )"))
      .def("print_all", &velox::py::PyVector::printAll, py::doc(R"(
        Returns a string containg all elements in the Vector.
      )"))
      .def("print_detailed", &velox::py::PyVector::printDetailed, py::doc(R"(
        Returns a descriptive string containing details about the
        Vector and all its elements.
      )"))
      .def(
          "summarize_to_text",
          &velox::py::PyVector::summarizeToText,
          py::doc(R"(
        Returns a human-readable summarize of the Vector.
      )"))
      .def(
          "save_to_file",
          &velox::py::PyVector::saveToFile,
          py::arg("file_path"),
          py::doc(R"(
        Serializes and writes the contents of the vector to a file in
        filePath` using velox::VectorSaver.
      )"))
      .def(
          "compare",
          &velox::py::PyVector::compare,
          py::arg("other"),
          py::arg("index"),
          py::arg("other_index"),
          py::doc(R"(
        Compares elements across Vectors.

        Args:
          index: Index on the current Vector to compare.
          other: Vector to compare to.
          other_index: Index on `other` to compare to.

        Returns:
          0 if elements are the same, non-zero otherwise.
      )"));

  velox::py::initializeVeloxMemory();

  static auto rootPool = velox::memory::memoryManager()->addRootPool();
  static auto leafPool = rootPool->addLeafChild("py_velox_restore_vector");

  m.def(
      "restore_from_file",
      [&](const std::string& filePath) {
        return velox::py::PyVector::restoreFromFile(filePath, leafPool);
      },
      py::doc(R"(
    Builds a new vector by reading and deserializing the content on the file
    `filePath`. The file must have been serialized using `vector.saveToFile()`.

    saveToFile() and restoreFromFile() and meant to be used for debugging
    purposes, and not for actual data storage. Use Parquet, DWRF or Nimble for
    that.
  )"));
}
