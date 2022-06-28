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
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/prestosql/RegexpReplace.h"
#include "velox/functions/prestosql/SplitPart.h"
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::functions {

namespace {
std::shared_ptr<exec::VectorFunction> makeRegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  return makeRe2Extract(name, inputArgs, /*emptyNoMatch=*/false);
}

void registerSimpleFunctions() {
  using namespace stringImpl;

  // Register string functions.
  registerFunction<ChrFunction, Varchar, int64_t>({"chr"});
  registerFunction<CodePointFunction, int32_t, Varchar>({"codepoint"});
  registerFunction<LengthFunction, int64_t, Varchar>({"length"});

  registerFunction<SubstrFunction, Varchar, Varchar, int64_t>({"substr"});
  registerFunction<SubstrFunction, Varchar, Varchar, int64_t, int64_t>(
      {"substr"});
  registerFunction<SubstrFunction, Varchar, Varchar, int32_t>({"substr"});
  registerFunction<SubstrFunction, Varchar, Varchar, int32_t, int32_t>(
      {"substr"});

  registerFunction<SplitPart, Varchar, Varchar, Varchar, int64_t>(
      {"split_part"});

  registerFunction<TrimFunction, Varchar, Varchar>({"trim"});
  registerFunction<LTrimFunction, Varchar, Varchar>({"ltrim"});
  registerFunction<RTrimFunction, Varchar, Varchar>({"rtrim"});

  registerFunction<udf_pad<true>, Varchar, Varchar, int64_t, Varchar>({"lpad"});
  registerFunction<udf_pad<false>, Varchar, Varchar, int64_t, Varchar>(
      {"rpad"});

  // Register hash functions.
  registerFunction<XxHash64Function, Varbinary, Varbinary>({"xxhash64"});
  registerFunction<Md5Function, Varbinary, Varbinary>({"md5"});
  registerFunction<Sha256Function, Varbinary, Varbinary>({"sha256"});
  registerFunction<Sha512Function, Varbinary, Varbinary>({"sha512"});

  registerFunction<ToHexFunction, Varchar, Varbinary>({"to_hex"});
  registerFunction<FromHexFunction, Varbinary, Varchar>({"from_hex"});
  registerFunction<ToBase64Function, Varchar, Varbinary>({"to_base64"});
  registerFunction<FromBase64Function, Varbinary, Varchar>({"from_base64"});
  exec::registerStatefulVectorFunction("like", likeSignatures(), makeLike);

  registerFunction<SplitPart, Varchar, Varchar, Varchar, int64_t>(
      {"split_part"});
  registerFunction<Re2RegexpReplacePresto, Varchar, Varchar, Varchar>(
      {"regexp_replace"});
  registerFunction<Re2RegexpReplacePresto, Varchar, Varchar, Varchar, Varchar>(
      {"regexp_replace"});
}
} // namespace

void registerStringFunctions() {
  registerSimpleFunctions();

  VELOX_REGISTER_VECTOR_FUNCTION(udf_lower, "lower");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_upper, "upper");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_split, "split");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_concat, "concat");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_strpos, "strpos");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_replace, "replace");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_reverse, "reverse");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_to_utf8, "to_utf8");

  // Regex functions
  exec::registerStatefulVectorFunction(
      "regexp_extract", re2ExtractSignatures(), makeRegexExtract);
  exec::registerStatefulVectorFunction(
      "regexp_extract_all", re2ExtractAllSignatures(), makeRe2ExtractAll);
  exec::registerStatefulVectorFunction(
      "regexp_like", re2SearchSignatures(), makeRe2Search);

  registerType("json", std::make_unique<const JsonTypeFactories>());
}
} // namespace facebook::velox::functions
