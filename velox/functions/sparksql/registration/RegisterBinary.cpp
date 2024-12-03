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
#include "velox/functions/lib/RegistrationHelpers.h"
#include "velox/functions/prestosql/BinaryFunctions.h"
#include "velox/functions/sparksql/Hash.h"
#include "velox/functions/sparksql/MightContain.h"
#include "velox/functions/sparksql/String.h"

namespace facebook::velox::functions::sparksql {

void registerBinaryFunctions(const std::string& prefix) {
  registerFunction<CRC32Function, int64_t, Varbinary>({prefix + "crc32"});
  exec::registerStatefulVectorFunction(
      prefix + "hash", hashSignatures(), makeHash, hashMetadata());
  exec::registerStatefulVectorFunction(
      prefix + "hash_with_seed",
      hashWithSeedSignatures(),
      makeHashWithSeed,
      hashMetadata());
  exec::registerStatefulVectorFunction(
      prefix + "xxhash64", xxhash64Signatures(), makeXxHash64, hashMetadata());
  exec::registerStatefulVectorFunction(
      prefix + "xxhash64_with_seed",
      xxhash64WithSeedSignatures(),
      makeXxHash64WithSeed,
      hashMetadata());
  registerFunction<Md5Function, Varchar, Varbinary>({prefix + "md5"});
  registerFunction<BloomFilterMightContainFunction, bool, Varbinary, int64_t>(
      {prefix + "might_contain"});
  registerFunction<Sha1HexStringFunction, Varchar, Varbinary>(
      {prefix + "sha1"});
  registerFunction<Sha2HexStringFunction, Varchar, Varbinary, int32_t>(
      {prefix + "sha2"});
}

} // namespace facebook::velox::functions::sparksql
