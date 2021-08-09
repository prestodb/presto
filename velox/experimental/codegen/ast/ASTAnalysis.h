/*
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

#include "velox/experimental/codegen/ast/AST.h"

namespace facebook::velox::codegen::analysis {

// output is null if any input is null
bool isDefaultNull(const ASTNode& node);

// output is null iff any input is null
bool isDefaultNullStrict(const ASTNode& node);

// output is null or false if any input is null
bool isFilterDefaultNull(const ASTNode& node);

}; // namespace facebook::velox::codegen::analysis
