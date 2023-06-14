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

#include "conversion.h"
#include <velox/vector/arrow/Abi.h>
#include <velox/vector/arrow/Bridge.h>
#include "context.h"

namespace facebook::velox::py {

namespace py = pybind11;

void addConversionBindings(py::module& m, bool asModuleLocalDefinitions) {
  m.def("export_to_arrow", [](VectorPtr& inputVector) {
    auto arrowArray = std::make_unique<ArrowArray>();
    auto pool_ = PyVeloxContext::getSingletonInstance().pool();
    facebook::velox::exportToArrow(inputVector, *arrowArray, pool_);

    auto arrowSchema = std::make_unique<ArrowSchema>();
    facebook::velox::exportToArrow(inputVector, *arrowSchema);

    py::module arrow_module = py::module::import("pyarrow");
    py::object array_class = arrow_module.attr("Array");
    return array_class.attr("_import_from_c")(
        reinterpret_cast<uintptr_t>(arrowArray.get()),
        reinterpret_cast<uintptr_t>(arrowSchema.get()));
  });

  m.def("import_from_arrow", [](py::object inputArrowArray) {
    auto arrowArray = std::make_unique<ArrowArray>();
    auto arrowSchema = std::make_unique<ArrowSchema>();
    inputArrowArray.attr("_export_to_c")(
        reinterpret_cast<uintptr_t>(arrowArray.get()),
        reinterpret_cast<uintptr_t>(arrowSchema.get()));
    auto pool_ = PyVeloxContext::getSingletonInstance().pool();
    return importFromArrowAsOwner(*arrowSchema, *arrowArray, pool_);
  });
}
} // namespace facebook::velox::py
