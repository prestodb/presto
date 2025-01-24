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

#include <memory>
#include "presto_cpp/external/dsdgen/include/dsdgen-c/dist.h"

namespace facebook::velox {
class BaseVector;
using VectorPtr = std::shared_ptr<BaseVector>;
} // namespace facebook::velox

namespace facebook::presto::connector::tpcds {

struct TpcdsTableDef {
  const char* name;
  int fl_small;
  int fl_child;
  int first_column;
  int colIndex = 0;
  int rowIndex = 0;
  DSDGenContext* dsdGenContext;
  std::vector<velox::VectorPtr> children;
  bool IsNull(int32_t column);
};
} // namespace facebook::presto::connector::tpcds
