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
#include "velox/functions/prestosql/types/IPAddressRegistration.h"
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/IPPrefixRegistration.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"

namespace facebook::velox::functions {
namespace {

inline bool isIPv4(int128_t ip) {
  int128_t ipV4 = 0x0000FFFF00000000;
  uint128_t mask = 0xFFFFFFFFFFFFFFFF;
  constexpr int kIPV6HalfBits = 64;
  mask = (mask << kIPV6HalfBits) | 0xFFFFFFFF00000000;
  return (ip & mask) == ipV4;
}

inline int128_t getIPSubnetMax(int128_t ip, uint8_t prefix) {
  uint128_t mask = 1;
  if (isIPv4(ip)) {
    ip |= (mask << (ipaddress::kIPV4Bits - prefix)) - 1;
    return ip;
  }

  // Special case: Overflow to all 0 subtracting 1 does not work.
  if (prefix == 0) {
    return -1;
  }

  ip |= (mask << (ipaddress::kIPV6Bits - prefix)) - 1;
  return ip;
}
} // namespace

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

template <typename T>
struct IPSubnetMinFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<IPAddress>& result,
      const arg_type<IPPrefix>& ipPrefix) {
    // IPPrefix type stores the smallest(canonical) IP already
    result = *ipPrefix.template at<0>();
  }
};

template <typename T>
struct IPSubnetMaxFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<IPAddress>& result,
      const arg_type<IPPrefix>& ipPrefix) {
    result =
        getIPSubnetMax(*ipPrefix.template at<0>(), *ipPrefix.template at<1>());
  }
};

template <typename T>
struct IPSubnetRangeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<IPAddress>>& result,
      const arg_type<IPPrefix>& ipPrefix) {
    result.push_back(*ipPrefix.template at<0>());
    result.push_back(
        getIPSubnetMax(*ipPrefix.template at<0>(), *ipPrefix.template at<1>()));
  }
};

template <typename T>
struct IPSubnetOfFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(
      out_type<bool>& result,
      const arg_type<IPPrefix>& ipPrefix,
      const arg_type<IPAddress>& ip) {
    result = isSubnetOf(ipPrefix, *ip);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<bool>& result,
      const arg_type<IPPrefix>& ipPrefix,
      const arg_type<IPPrefix>& ipPrefix2) {
    result = (*ipPrefix2.template at<1>() >= *ipPrefix.template at<1>()) &&
        isSubnetOf(ipPrefix, *ipPrefix2.template at<0>());
  }

 private:
  static bool isSubnetOf(const arg_type<IPPrefix>& ipPrefix, int128_t checkIP) {
    uint128_t mask = 1;
    const uint8_t prefix = *ipPrefix.template at<1>();
    if (isIPv4(*ipPrefix.template at<0>())) {
      checkIP &= ((mask << (ipaddress::kIPV4Bits - prefix)) - 1) ^
          static_cast<uint128_t>(-1);
    } else {
      // Special case: Overflow to all 0 subtracting 1 does not work.
      if (prefix == 0) {
        checkIP = 0;
      } else {
        checkIP &= ((mask << (ipaddress::kIPV6Bits - prefix)) - 1) ^
            static_cast<uint128_t>(-1);
      }
    }

    return (*ipPrefix.template at<0>() == checkIP);
  }
};

void registerIPAddressFunctions(const std::string& prefix) {
  registerIPAddressType();
  registerIPPrefixType();
  registerFunction<IPPrefixFunction, IPPrefix, IPAddress, int64_t>(
      {prefix + "ip_prefix"});
  registerFunction<IPPrefixFunction, IPPrefix, Varchar, int64_t>(
      {prefix + "ip_prefix"});
  registerFunction<IPSubnetMinFunction, IPAddress, IPPrefix>(
      {prefix + "ip_subnet_min"});
  registerFunction<IPSubnetMaxFunction, IPAddress, IPPrefix>(
      {prefix + "ip_subnet_max"});
  registerFunction<IPSubnetRangeFunction, Array<IPAddress>, IPPrefix>(
      {prefix + "ip_subnet_range"});
  registerFunction<IPSubnetOfFunction, bool, IPPrefix, IPAddress>(
      {prefix + "is_subnet_of"});
  registerFunction<IPSubnetOfFunction, bool, IPPrefix, IPPrefix>(
      {prefix + "is_subnet_of"});
}

} // namespace facebook::velox::functions
