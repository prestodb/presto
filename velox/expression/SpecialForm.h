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

namespace facebook::velox::exec {

class SpecialForm : public Expr {
 public:
  SpecialForm(
      TypePtr type,
      std::vector<ExprPtr> inputs,
      const std::string& name,
      bool supportsFlatNoNullsFastPath,
      bool trackCpuUsage)
      : Expr(
            std::move(type),
            std::move(inputs),
            name,
            true /* specialForm */,
            supportsFlatNoNullsFastPath,
            trackCpuUsage) {}
};
} // namespace facebook::velox::exec
