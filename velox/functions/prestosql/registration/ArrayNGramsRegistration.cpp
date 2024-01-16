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

#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/ArrayFunctions.h"

namespace facebook::velox::functions {
namespace {
template <typename T>
inline void registerArrayNGramsFunctions(const std::string& prefix) {
  registerFunction<ArrayNGramsFunction, Array<Array<T>>, Array<T>, int64_t>(
      {prefix + "ngrams"});
}

} // namespace
void registerArrayNGramsFunctions(const std::string& prefix) {
  registerArrayNGramsFunctions<int8_t>(prefix);
  registerArrayNGramsFunctions<int16_t>(prefix);
  registerArrayNGramsFunctions<int32_t>(prefix);
  registerArrayNGramsFunctions<int64_t>(prefix);
  registerArrayNGramsFunctions<int128_t>(prefix);
  registerArrayNGramsFunctions<float>(prefix);
  registerArrayNGramsFunctions<double>(prefix);
  registerArrayNGramsFunctions<bool>(prefix);
  registerArrayNGramsFunctions<Timestamp>(prefix);
  registerArrayNGramsFunctions<Date>(prefix);
  registerArrayNGramsFunctions<Varbinary>(prefix);
  registerArrayNGramsFunctions<Generic<T1>>(prefix);
  registerFunction<
      ArrayNGramsFunctionFunctionString,
      Array<Array<Varchar>>,
      Array<Varchar>,
      int64_t>({prefix + "ngrams"});
}
} // namespace facebook::velox::functions
