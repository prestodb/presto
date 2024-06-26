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
#include "velox/functions/prestosql/BinaryFunctions.h"

namespace facebook::velox::functions {

namespace {
void registerSimpleFunctions(const std::string& prefix) {
  // Register hash functions.
  registerFunction<CRC32Function, int64_t, Varbinary>({prefix + "crc32"});
  registerFunction<XxHash64Function, Varbinary, Varbinary>(
      {prefix + "xxhash64"});
  registerFunction<Md5Function, Varbinary, Varbinary>({prefix + "md5"});
  registerFunction<Sha1Function, Varbinary, Varbinary>({prefix + "sha1"});
  registerFunction<Sha256Function, Varbinary, Varbinary>({prefix + "sha256"});
  registerFunction<Sha512Function, Varbinary, Varbinary>({prefix + "sha512"});
  registerFunction<HmacSha1Function, Varbinary, Varbinary, Varbinary>(
      {prefix + "hmac_sha1"});
  registerFunction<HmacSha256Function, Varbinary, Varbinary, Varbinary>(
      {prefix + "hmac_sha256"});
  registerFunction<HmacSha512Function, Varbinary, Varbinary, Varbinary>(
      {prefix + "hmac_sha512"});
  registerFunction<HmacMd5Function, Varbinary, Varbinary, Varbinary>(
      {prefix + "hmac_md5"});
  registerFunction<SpookyHashV232Function, Varbinary, Varbinary>(
      {prefix + "spooky_hash_v2_32"});
  registerFunction<SpookyHashV264Function, Varbinary, Varbinary>(
      {prefix + "spooky_hash_v2_64"});

  registerFunction<ToHexFunction, Varchar, Varbinary>({prefix + "to_hex"});
  registerFunction<FromHexFunction, Varbinary, Varchar>({prefix + "from_hex"});
  registerFunction<ToBase64Function, Varchar, Varbinary>(
      {prefix + "to_base64"});

  registerFunction<FromBase64Function, Varbinary, Varchar>(
      {prefix + "from_base64"});
  registerFunction<FromBase64Function, Varbinary, Varbinary>(
      {prefix + "from_base64"});

  registerFunction<ToBase64UrlFunction, Varchar, Varbinary>(
      {prefix + "to_base64url"});
  registerFunction<FromBase64UrlFunction, Varbinary, Varchar>(
      {prefix + "from_base64url"});

  registerFunction<FromBigEndian32, int32_t, Varbinary>(
      {prefix + "from_big_endian_32"});
  registerFunction<ToBigEndian32, Varbinary, int32_t>(
      {prefix + "to_big_endian_32"});
  registerFunction<FromBigEndian64, int64_t, Varbinary>(
      {prefix + "from_big_endian_64"});
  registerFunction<ToBigEndian64, Varbinary, int64_t>(
      {prefix + "to_big_endian_64"});
  registerFunction<ToIEEE754Bits64, Varbinary, double>(
      {prefix + "to_ieee754_64"});
  registerFunction<FromIEEE754Bits64, double, Varbinary>(
      {prefix + "from_ieee754_64"});
  registerFunction<ToIEEE754Bits32, Varbinary, float>(
      {prefix + "to_ieee754_32"});
  registerFunction<FromIEEE754Bits32, float, Varbinary>(
      {prefix + "from_ieee754_32"});
  registerFunction<
      LPadVarbinaryFunction,
      Varbinary,
      Varbinary,
      int64_t,
      Varbinary>({prefix + "lpad"});
  registerFunction<
      RPadVarbinaryFunction,
      Varbinary,
      Varbinary,
      int64_t,
      Varbinary>({prefix + "rpad"});
}
} // namespace

void registerBinaryFunctions(const std::string& prefix) {
  registerSimpleFunctions(prefix);
}
} // namespace facebook::velox::functions
