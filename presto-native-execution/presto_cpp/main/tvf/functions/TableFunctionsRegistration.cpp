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
#include "presto_cpp/main/tvf/functions/TableFunctionsRegistration.h"

namespace facebook::presto::tvf {

extern void registerExcludeColumns(const std::string& name);
extern void registerSequence(const std::string& name);
extern void registerRemoteAnn(const std::string& name);

void registerAllTableFunctions(const std::string& prefix) {
  registerExcludeColumns(prefix + "exclude_columns");
  registerSequence(prefix + "sequence");
  registerRemoteAnn(prefix + "remoteAnn");
}

} // namespace facebook::presto::tvf
