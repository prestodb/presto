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

#include <arrow/array.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/python/pyarrow.h>
#include <arrow/record_batch.h>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "velox/python/init/PyInit.h"
#include "velox/python/vector/PyVector.h"
#include "velox/vector/arrow/Bridge.h"

namespace py = pybind11;

/// This module adds two functions `to_velox()` and `to_arrow()` that allow the
/// conversion between Velox Vectors and Arrow Arrays from a Python program. It
/// works by extracting the Arrow C structures from the Arrow C++ Array, then
/// using Velox's Arrow bridge to convert it to a Velox Vector (and vice-versa).
PYBIND11_MODULE(arrow, m) {
  using namespace facebook;

  py::module::import("pyvelox.vector");

  arrow::py::import_pyarrow();
  velox::py::initializeVeloxMemory();

  static auto rootPool = velox::memory::memoryManager()->addRootPool();
  static auto leafPool = rootPool->addLeafChild("py_velox_arrow_pool");

  /// Converts a pyarrow.Array into a pyvelox.vector.Vector using Velox's arrow
  /// bridge.
  m.def(
      "to_velox",
      [](py::object& batchObject) {
        ArrowSchema schema;
        ArrowArray data;
        std::shared_ptr<arrow::Array> array;
        std::shared_ptr<arrow::RecordBatch> batch;

        if (arrow::py::is_array(batchObject.ptr())) {
          array = *arrow::py::unwrap_array(batchObject.ptr());
          auto statusType = arrow::ExportType(*array->type(), &schema);
          auto statusArray = arrow::ExportArray(*array, &data);

          if (!statusArray.ok() || !statusType.ok()) {
            throw std::runtime_error("Unable to convert Arrow Array to C ABI.");
          }
        } else if (arrow::py::is_batch(batchObject.ptr())) {
          batch = *arrow::py::unwrap_batch(batchObject.ptr());
          auto statusType = arrow::ExportSchema(*batch->schema(), &schema);
          auto statusArray = arrow::ExportRecordBatch(*batch, &data);

          if (!statusArray.ok() || !statusType.ok()) {
            throw std::runtime_error(
                "Unable to convert Arrow RecorBatch to C ABI.");
          }
        } else {
          throw std::runtime_error("Unknown input Arrow structure.");
        }
        return velox::py::PyVector{
            velox::importFromArrowAsViewer(schema, data, leafPool.get()),
            leafPool};
      },
      R"pbdoc(
Converts an arrow object to a velox vector.

:param vector: Input arrow object.

:examples:

.. doctest::

    >>> array = pyarrow.array([1, 2, 3, 4, 5, 6])
    >>> vector = to_velox(array)

)pbdoc");

  /// Converts a pyvelox.vector.Vector to a pyarrow.Array using Velox's arrow
  /// bridge.
  m.def(
      "to_arrow",
      [](velox::py::PyVector& vector) {
        ArrowSchema schema;
        ArrowArray data;

        velox::exportToArrow(vector.vector(), schema);
        velox::exportToArrow(vector.vector(), data, leafPool.get());

        auto arrowType = *arrow::ImportType(&schema);
        auto arrowArray = *arrow::ImportArray(&data, arrowType);
        return py::reinterpret_steal<py::object>(
            arrow::py::wrap_array(arrowArray));
      },
      R"pbdoc(
Converts a velox vector to an arrow object.

:param vector: Input arrow object.

:examples:

.. doctest::

    >>> import pyvelox.legacy as pv
    >>> vec = pv.from_list([1, 2, 3, 4, 5])
    >>> arrow = to_arrow(vec)

)pbdoc");
}
