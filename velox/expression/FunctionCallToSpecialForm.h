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

#include "velox/expression/Expr.h"
#include "velox/type/Type.h"

namespace facebook::velox::exec {
class FunctionCallToSpecialForm {
 public:
  virtual ~FunctionCallToSpecialForm() {}

  /// Returns the output Type of the SpecialForm given the input argument Types.
  /// Throws if the input Types do not match what's expected for the SpecialForm
  /// or if the SpecialForm cannot infer the return Type based on the input
  /// arguments, e.g. Try.
  virtual TypePtr resolveType(const std::vector<TypePtr>& argTypes) = 0;

  /// Given the output Type, the child expresssions, and whether or not to track
  /// CPU usage, returns the SpecialForm.
  virtual ExprPtr constructSpecialForm(
      const TypePtr& type,
      std::vector<ExprPtr>&& compiledChildren,
      bool trackCpuUsage) = 0;
};

/// Returns the output Type of the SpecialForm associated with the functionName
/// given the input argument Types. If functionName is not the name of a known
/// SpecialForm, returns nullptr. Note that some SpecialForms may throw on
/// invalid arguments or if they don't support type resolution, e.g. Try.
TypePtr resolveTypeForSpecialForm(
    const std::string& functionName,
    const std::vector<TypePtr>& argTypes);

/// Returns the SpeicalForm associated with the functionName.  If functionName
/// is not the name of a known SpecialForm, returns nulltpr.
ExprPtr constructSpecialForm(
    const std::string& functionName,
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage);

/// Returns true iff a FunctionCallToSpeicalForm object has been registered for
/// the given functionName.
bool isFunctionCallToSpecialFormRegistered(const std::string& functionName);
} // namespace facebook::velox::exec
