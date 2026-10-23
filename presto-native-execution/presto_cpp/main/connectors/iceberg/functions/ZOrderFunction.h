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

#include <string>

namespace facebook::presto::iceberg::functions {

/// Registers zorder(ROW(...)) -> VARBINARY under 'name'. The result
/// interleaves the bits of an order-preserving encoding of each field, so
/// sorting by it clusters rows that are close in every field. Produces the
/// same bytes as the Presto Java Iceberg connector's zorder function.
void registerZOrderFunction(const std::string& name);

} // namespace facebook::presto::iceberg::functions
