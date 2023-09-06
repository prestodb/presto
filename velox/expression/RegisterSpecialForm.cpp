
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

#include "velox/expression/RegisterSpecialForm.h"

#include "velox/expression/CastExpr.h"
#include "velox/expression/CoalesceExpr.h"
#include "velox/expression/ConjunctExpr.h"
#include "velox/expression/FunctionCallToSpecialForm.h"
#include "velox/expression/RowConstructor.h"
#include "velox/expression/SpecialFormRegistry.h"
#include "velox/expression/SwitchExpr.h"
#include "velox/expression/TryExpr.h"

namespace facebook::velox::exec {
void registerFunctionCallToSpecialForms() {
  registerFunctionCallToSpecialForm(
      "and", std::make_unique<ConjunctCallToSpecialForm>(true /* isAnd */));
  registerFunctionCallToSpecialForm(
      "cast", std::make_unique<CastCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      "try_cast", std::make_unique<TryCastCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      "coalesce", std::make_unique<CoalesceCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      "if", std::make_unique<IfCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      "or", std::make_unique<ConjunctCallToSpecialForm>(false /* isAnd */));
  registerFunctionCallToSpecialForm(
      "switch", std::make_unique<SwitchCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      "try", std::make_unique<TryCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      RowConstructorCallToSpecialForm::kRowConstructor,
      std::make_unique<RowConstructorCallToSpecialForm>());
}
} // namespace facebook::velox::exec
