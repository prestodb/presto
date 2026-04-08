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

#include "TestingTableFunctions.h"

// This file defines the plugin entry point for dynamically loading testing
// table functions. The library (.so/.dylib) provides a `void registerExtensions()`
// C function in the top-level namespace that will be called when the plugin
// is loaded.
//
// (note the extern "C" directive to prevent the compiler from mangling the
// symbol name).

extern "C" {
// The function registerExtensions is the entry point to execute the
// registration of the table functions and cannot be changed.
void registerExtensions() {
  facebook::presto::tvf::registerSimpleTableFunction("presto.default.simple_table_function");
  facebook::presto::tvf::registerRepeatFunction("presto.default.repeat");
  facebook::presto::tvf::registerIdentityFunction("presto.default.identity_function");
  facebook::presto::tvf::registerIdentityPassThroughFunction("presto.default.identity_pass_through_function");
  facebook::presto::tvf::registerEmptyOutputFunction("presto.default.empty_output");
  facebook::presto::tvf::registerEmptyOutputWithPassThroughFunction("presto.default.empty_output_with_pass_through");
  facebook::presto::tvf::registerEmptySourceFunction("presto.default.empty_source");
  facebook::presto::tvf::registerConstantFunction("presto.default.constant");
  facebook::presto::tvf::registerTestSingleInputFunction("presto.default.test_single_input_function");
  facebook::presto::tvf::registerPassThroughInputFunction("presto.default.pass_through");
  facebook::presto::tvf::registerTestInputsFunction("presto.default.test_inputs_function");

  facebook::presto::tvf::SimpleTableFunctionHandle::registerSerDe();
  facebook::presto::tvf::RepeatFunctionHandle::registerSerDe();
  facebook::presto::tvf::IdentityFunctionHandle::registerSerDe();
  facebook::presto::tvf::IdentityPassThroughFunctionHandle::registerSerDe();
  facebook::presto::tvf::EmptyOutputFunctionHandle::registerSerDe();
  facebook::presto::tvf::EmptyOutputWithPassThroughFunctionHandle::registerSerDe();
  facebook::presto::tvf::EmptySourceFunctionHandle::registerSerDe();
  facebook::presto::tvf::EmptySourceFunctionSplitHandle::registerSerDe();
  facebook::presto::tvf::ConstantFunctionHandle::registerSerDe();
  facebook::presto::tvf::ConstantFunctionSplitHandle::registerSerDe();
  facebook::presto::tvf::TestSingleInputFunctionHandle::registerSerDe();
  facebook::presto::tvf::PassThroughInputFunctionHandle::registerSerDe();
  facebook::presto::tvf::TestInputsFunctionHandle::registerSerDe();
}
}

// Made with Bob
