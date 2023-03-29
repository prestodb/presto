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

#include "pyvelox.h"
#include "signatures.h"

namespace facebook::velox::py {
using namespace velox;
namespace py = pybind11;

static std::string serializeType(
    const std::shared_ptr<const velox::Type>& type) {
  const auto& obj = type->serialize();
  return folly::json::serialize(obj, velox::getSerializationOptions());
}

static VectorPtr evaluateExpression(
    std::shared_ptr<const facebook::velox::core::IExpr>& expr,
    std::vector<std::string> names,
    std::vector<VectorPtr>& inputs) {
  using namespace facebook::velox;
  if (names.size() != inputs.size()) {
    throw py::value_error("Must specify the same number of names as inputs");
  }
  vector_size_t numRows = inputs.empty() ? 0 : inputs[0]->size();
  std::vector<std::shared_ptr<const Type>> types;
  types.reserve(inputs.size());
  for (auto vector : inputs) {
    types.push_back(vector->type());
    if (vector->size() != numRows) {
      throw py::value_error("Inputs must have matching number of rows");
    }
  }
  auto rowType = ROW(std::move(names), std::move(types));
  memory::MemoryPool* pool = PyVeloxContext::getInstance().pool();
  RowVectorPtr rowVector = std::make_shared<RowVector>(
      pool, rowType, BufferPtr{nullptr}, numRows, inputs);
  core::TypedExprPtr typed = core::Expressions::inferTypes(expr, rowType, pool);
  exec::ExprSet set({typed}, PyVeloxContext::getInstance().execCtx());
  exec::EvalCtx evalCtx(
      PyVeloxContext::getInstance().execCtx(), &set, rowVector.get());
  SelectivityVector rows(numRows);
  std::vector<VectorPtr> result;
  set.eval(rows, evalCtx, result);
  return result[0];
}

static void addExpressionBindings(
    py::module& m,
    bool asModuleLocalDefinitions) {
  using namespace facebook::velox;
  functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();

  // PyBind11's classes cannot be const, but the parse functions return const
  // shared_ptrs, so we wrap in a non-const class.
  struct IExprWrapper {
    std::shared_ptr<const core::IExpr> expr;
  };

  py::class_<IExprWrapper>(
      m, "Expression", py::module_local(asModuleLocalDefinitions))
      .def(
          "__str__",
          [](IExprWrapper& e) { return e.expr->toString(); },
          "Returns the string representation of the expression")
      .def(
          "getInputs",
          [](IExprWrapper& e) {
            const std::vector<std::shared_ptr<const core::IExpr>>& inputs =
                e.expr->getInputs();
            std::vector<IExprWrapper> wrapped_inputs;
            wrapped_inputs.resize(inputs.size());
            for (const std::shared_ptr<const core::IExpr>& input : inputs) {
              wrapped_inputs.push_back({input});
            }
            return wrapped_inputs;
          },
          "Returns a list of expressions that the inputs to this expression")
      .def(
          "evaluate",
          [](IExprWrapper& e,
             std::vector<std::string> names,
             std::vector<VectorPtr>& inputs) {
            return evaluateExpression(e.expr, names, inputs);
          },
          "Evaluates the expression for a given set of inputs. Inputs are specified with a list of names and a list of vectors, with each vector having the corresponding name")
      .def(
          "evaluate",
          [](IExprWrapper& e,
             std::unordered_map<std::string, VectorPtr> name_input_map) {
            std::vector<std::string> names;
            std::vector<VectorPtr> inputs;
            names.reserve(name_input_map.size());
            inputs.reserve(name_input_map.size());
            for (const std::pair<std::string, VectorPtr>& pair :
                 name_input_map) {
              names.push_back(pair.first);
              inputs.push_back(pair.second);
            }
            return evaluateExpression(e.expr, names, inputs);
          },
          "Evaluates the expression, taking in a map from names to input vectors")
      .def_static("from_string", [](std::string& str) {
        parse::ParseOptions opts;
        return IExprWrapper{parse::parseExpr(str, opts)};
      });
}

#ifdef CREATE_PYVELOX_MODULE
PYBIND11_MODULE(pyvelox, m) {
  m.doc() = R"pbdoc(
      PyVelox native code module
      --------------------------

      .. currentmodule:: pyvelox.pyvelox

      .. autosummary::
         :toctree: _generate

  )pbdoc";

  addVeloxBindings(m);
  addSignatureBindings(m);
  m.attr("__version__") = "dev";
}
#endif
} // namespace facebook::velox::py
