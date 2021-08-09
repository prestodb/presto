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

#include <iostream>
#include <memory>
#include <typeinfo>
#include <vector>

#include "velox/experimental/codegen/library_loader/FunctionTable.h"

// Sample functions for testing only
// Assume that the arguments to init/release are valid uint64 pointers

int function1(void* a, void* b) {
  return reinterpret_cast<int64_t>(a) + reinterpret_cast<int64_t>(b);
}
int function2(void* a, void* b) {
  return reinterpret_cast<int64_t>(a) - reinterpret_cast<int64_t>(b);
}
std::shared_ptr<FunctionTable> functionTable;
static uint64_t saved;
extern "C" {

bool init(void* param) {
  functionTable = std::make_shared<FunctionTable>();
  registerFunction("function1", &function1, *functionTable);
  registerFunction("function2", &function2, *functionTable);
  saved = *static_cast<uint64_t*>(param);
  return true;
}

void release(void* param) {
  *static_cast<uint64_t*>(param) = saved;
};

std::shared_ptr<FunctionTable> getFunctionTable() {
  return functionTable;
};
}
