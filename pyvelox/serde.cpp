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

#include "serde.h"
#include "context.h"

#include <velox/vector/VectorSaver.h>

namespace facebook::velox::py {

namespace py = pybind11;

namespace {
VectorPtr pyRestoreVectorFromFileHelper(const char* filePath) {
  using namespace facebook::velox;
  memory::MemoryPool* pool = PyVeloxContext::getSingletonInstance().pool();
  return restoreVectorFromFile(filePath, pool);
}
} // namespace

void addSerdeBindings(py::module& m, bool asModuleLocalDefinitions) {
  using namespace facebook::velox;

  m.def(
      "save_vector",
      &saveVectorToFile,
      R"delimiter(
        Serializes the vector into binary format and writes it to a new file.

        Parameters
        ----------
        vector : Union[FlatVector, ConstantVector, DictionaryVector]
              The vector to be saved.
        file_path: str
              The path to which the vector will be saved.

        Returns
        -------
        None

        Examples
        --------

        >>> import pyvelox.pyvelox as pv
        >>> vec = pv.from_list([1, 2, 3])
        >>> pv.save_vector(vec, '/tmp/flatvector.bin')
      )delimiter",
      py::arg("vector"),
      py::arg("file_path"));
  m.def(
      "load_vector",
      &pyRestoreVectorFromFileHelper,
      R"delimiter(
        Reads and deserializes a vector from a file stored by save_vector.

        Parameters
        ----------
        file_path: str
              The path from which the vector will be loaded.

        Returns
        -------
        Union[FlatVector, ConstantVector, DictionaryVector]

        Examples
        --------

        >>> import pyvelox.pyvelox as pv
        >>> pv.load_vector('/tmp/flatvector.bin')
        <pyvelox.pyvelox.FlatVector_BIGINT object at 0x7f8f6f818bb0>
      )delimiter",
      py::arg("file_path"));
}

} // namespace facebook::velox::py
