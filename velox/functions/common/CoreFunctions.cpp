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
#include "velox/functions/common/CoreFunctions.h"

#include "velox/functions/common/DateTimeFunctions.h"
#include "velox/functions/common/Hash.h"
#include "velox/functions/common/JsonExtractScalar.h"
#include "velox/functions/common/Rand.h"
#include "velox/functions/common/RegisterArithmetic.h"
#include "velox/functions/common/RegisterCheckedArithmetic.h"
#include "velox/functions/common/RegisterComparisons.h"
#include "velox/functions/common/StringFunctions.h"
#include "velox/functions/lib/RegistrationHelpers.h"

namespace facebook::velox::functions {

void registerFunctions() {
  // Register functions here.
  static const std::vector<std::string> EMPTY{};

  registerFunction<udf_rand, double>(EMPTY);

  registerUnaryScalar<udf_hash, int64_t>(EMPTY);

  registerFunction<udf_json_extract_scalar, Varchar, Varchar, Varchar>();

  // Register string functions.
  registerFunction<udf_chr, Varchar, int64_t>();
  registerFunction<udf_codepoint, int32_t, Varchar>();
  registerFunction<
      udf_xxhash64int<int64_t, Varchar>,
      int64_t,
      Varchar,
      int64_t>({"xxhash64"});
  registerFunction<udf_xxhash64int<int64_t, Varchar>, int64_t, Varchar>(
      {"xxhash64"});
  registerFunction<udf_xxhash64<Varbinary, Varbinary>, Varbinary, Varbinary>(
      {"xxhash64"});
  registerFunction<udf_md5<Varbinary, Varbinary>, Varbinary, Varbinary>(
      {"md5"});
  registerFunction<udf_md5_radix<Varchar, Varchar>, Varchar, Varchar, int32_t>(
      {"md5"});
  registerFunction<udf_md5_radix<Varchar, Varchar>, Varchar, Varchar, int64_t>(
      {"md5"});
  registerFunction<udf_md5_radix<Varchar, Varchar>, Varchar, Varchar>({"md5"});

  registerFunction<udf_to_unixtime, double, Timestamp>(
      {"to_unixtime", "to_unix_timestamp"});
  registerFunction<udf_from_unixtime, Timestamp, double>();

  registerArithmeticFunctions();
  registerCheckedArithmeticFunctions();
  registerComparisonFunctions();
}

} // namespace facebook::velox::functions
