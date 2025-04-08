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

#include <pybind11/pybind11.h>

namespace facebook::velox::py {

namespace py = pybind11;

/// Adds bindings for arrow-velox conversion functions to module m.
///
/// @param m Module to add bindings to.
/// @param asModuleLocalDefinitions If true then these bindings are only
///  visible inside the module. Refer to
///  https://pybind11.readthedocs.io/en/stable/advanced/classes.html#module-local-class-bindings
///  for further details.
void addConversionBindings(py::module& m, bool asModuleLocalDefinitions = true);

} // namespace facebook::velox::py
