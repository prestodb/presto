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

namespace facebook::presto::hive::functions {

// Registers Hive-specific native functions into the 'hive.default' namespace.
// This method is safe to call multiple times; it performs one-time registration
// guarded by an internal call_once.
void registerHiveNativeFunctions();

} // namespace facebook::presto::hive::functions
