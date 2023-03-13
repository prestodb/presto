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
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/prestosql/URLFunctions.h"

namespace facebook::velox::functions {

void registerURLFunctions(const std::string& prefix) {
  registerFunction<UrlExtractHostFunction, Varchar, Varchar>(
      {prefix + "url_extract_host"});
  registerFunction<UrlExtractFragmentFunction, Varchar, Varchar>(
      {prefix + "url_extract_fragment"});
  registerFunction<UrlExtractPathFunction, Varchar, Varchar>(
      {prefix + "url_extract_path"});
  registerFunction<UrlExtractParameterFunction, Varchar, Varchar, Varchar>(
      {prefix + "url_extract_parameter"});
  registerFunction<UrlExtractProtocolFunction, Varchar, Varchar>(
      {prefix + "url_extract_protocol"});
  registerFunction<UrlExtractPortFunction, int64_t, Varchar>(
      {prefix + "url_extract_port"});
  registerFunction<UrlExtractQueryFunction, Varchar, Varchar>(
      {prefix + "url_extract_query"});
  registerFunction<UrlEncodeFunction, Varchar, Varchar>(
      {prefix + "url_encode"});
  registerFunction<UrlDecodeFunction, Varchar, Varchar>(
      {prefix + "url_decode"});
}
} // namespace facebook::velox::functions
