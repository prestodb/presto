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
#pragma once

#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"

namespace facebook::velox::functions {

template <typename T>
struct IPPrefixFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<IPPrefix>& result,
      const arg_type<IPAddress>& ip,
      const arg_type<int64_t>& prefixBits) {
    folly::ByteArray16 addrBytes;
    memcpy(&addrBytes, &ip, ipaddress::kIPAddressBytes);
    std::reverse(addrBytes.begin(), addrBytes.end());

    result = makeIPPrefix(folly::IPAddressV6(addrBytes), prefixBits);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<IPPrefix>& result,
      const arg_type<Varchar>& ipString,
      const arg_type<int64_t>& prefixBits) {
    auto tryIp = folly::IPAddress::tryFromString(ipString);
    if (tryIp.hasError()) {
      VELOX_USER_FAIL("Cannot cast value to IPADDRESS: {}", ipString);
    }

    result = makeIPPrefix(
        folly::IPAddress::createIPv6(folly::IPAddress(tryIp.value())),
        prefixBits);
  }

 private:
  static std::tuple<int128_t, int8_t> makeIPPrefix(
      const folly::IPAddressV6& v6Addr,
      int64_t prefixBits) {
    if (v6Addr.isIPv4Mapped()) {
      VELOX_USER_CHECK(
          0 <= prefixBits && prefixBits <= ipaddress::kIPV4Bits,
          "IPv4 subnet size must be in range [0, 32]");
    } else {
      VELOX_USER_CHECK(
          0 <= prefixBits && prefixBits <= ipaddress::kIPV6Bits,
          "IPv6 subnet size must be in range [0, 128]");
    }
    auto canonicalBytes = v6Addr.isIPv4Mapped()
        ? v6Addr.createIPv4().mask(prefixBits).createIPv6().toByteArray()
        : v6Addr.mask(prefixBits).toByteArray();

    int128_t intAddr;
    std::reverse(canonicalBytes.begin(), canonicalBytes.end());
    memcpy(&intAddr, &canonicalBytes, ipaddress::kIPAddressBytes);
    return std::make_tuple(intAddr, static_cast<int8_t>(prefixBits));
  }
};

void registerIPAddressFunctions(const std::string& prefix) {
  registerIPAddressType();
  registerIPPrefixType();
  registerFunction<IPPrefixFunction, IPPrefix, IPAddress, int64_t>(
      {prefix + "ip_prefix"});
  registerFunction<IPPrefixFunction, IPPrefix, Varchar, int64_t>(
      {prefix + "ip_prefix"});
}

} // namespace facebook::velox::functions
