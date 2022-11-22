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

#include "pyvelox.h" // @manual

namespace facebook::velox::py {
using namespace velox;
namespace py = pybind11;

std::string serializeType(const std::shared_ptr<const velox::Type>& type) {
  const auto& obj = type->serialize();
  return folly::json::serialize(obj, velox::getSerializationOptions());
}

#ifdef CREATE_PYVELOX_MODULE
PYBIND11_MODULE(pyvelox, m) {
  m.doc() = R"pbdoc(
        PyVelox native code module
        -----------------------
       )pbdoc";

  addVeloxBindings(m);

  m.attr("__version__") = "dev";
}
#endif
} // namespace facebook::velox::py
