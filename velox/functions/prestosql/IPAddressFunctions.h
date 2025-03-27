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

template <typename T>
struct IPPrefixCollapseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<IPPrefix>>& result,
      const arg_type<Array<IPPrefix>>& ipPrefixes) {
    if (ipPrefixes.size() == 0) {
      return;
    }

    std::vector<std::tuple<int128_t, int8_t>> prefixes;
    prefixes.reserve(ipPrefixes.size());

    for (const auto& ipPrefix : ipPrefixes) {
      if (ipPrefix.has_value()) {
        prefixes.push_back(std::make_tuple(
            *ipPrefix->template at<0>(), *ipPrefix->template at<1>()));
      } else {
        // ip_prefix_collapse does not support null elements. Thus we throw here
        // with the same error message as Presto java.
        VELOX_USER_FAIL("ip_prefix_collapse does not support null elements");
      }
    }

    std::sort(
        prefixes.begin(), prefixes.end(), [](const auto& a, const auto& b) {
          // First compare by the first tuple to see if we can order the
          // ipaddresses.
          auto ipCompare = IPADDRESS()->compare(std::get<0>(a), std::get<0>(b));
          if (ipCompare != 0) {
            return ipCompare < 0;
          }

          // Compare the prefix bits if the ip addresses are the same.
          return std::get<1>(a) < std::get<1>(b);
        });

    // If the length of the prefixes is 1 and it is not null, we can simply
    // return.
    if (prefixes.size() == 1) {
      writeIpPrefix(result, prefixes);
      return;
    }

    // All IPAddresses must be the same IP version
    const bool isFirstIpV4 = isIPv4(std::get<0>(prefixes.front()));
    for (size_t i = 1; i < prefixes.size(); i++) {
      const bool isIpV4 = isIPv4(std::get<0>(prefixes[i]));
      if (isFirstIpV4 != isIpV4) {
        VELOX_USER_FAIL("All IPPREFIX elements must be the same IP version.");
      }
    }

    auto mergedRanges = mergeIpRanges(prefixes);
    const auto ipMaxBitLength =
        isFirstIpV4 ? ipaddress::kIPV4Bits : ipaddress::kIPV6Bits;

    for (auto& range : mergedRanges) {
      writeIpPrefix(
          result,
          generateMinIpPrefixes(
              std::get<0>(range), std::get<1>(range), ipMaxBitLength));
    }
  }

 private:
  FOLLY_ALWAYS_INLINE static void writeIpPrefix(
      exec::ArrayWriter<IPPrefix>& writer,
      const std::vector<std::tuple<int128_t, int8_t>>& ipprefixes) {
    for (auto& ipprefix : ipprefixes) {
      writer.add_item() = ipprefix;
    }
  }

  FOLLY_ALWAYS_INLINE static int64_t bitLength(int128_t num) {
    // Handle the case when the number is zero
    if (num == 0) {
      return 0;
    }

    // Work with the absolute value of the number
    uint128_t abs_num =
        (num < 0) ? static_cast<uint128_t>(-num) : static_cast<uint128_t>(num);

    // Find the position of the highest bit using logarithm (base 2)
    return static_cast<int64_t>(std::log2(abs_num)) + 1;
  }

  FOLLY_ALWAYS_INLINE static int64_t getLowestSetBit(int128_t x) {
    if (x == 0) {
      return -1; // No set bits
    }

    // Check the lower 64 bits
    static constexpr uint64_t mask = 0xFFFFFFFFFFFFFFFF;
    if (x & mask) {
      return __builtin_ctzll(x & mask);
    }

    // Check the upper 64 bits
    return __builtin_ctzll(x >> 64) + 64;
  }

  FOLLY_ALWAYS_INLINE static int64_t findRangeBits(
      int128_t firstIpAddress,
      int128_t lastIpAddress) {
    // The number of IP addresses in the range
    constexpr int128_t kOne = 1;
    const int128_t ipCount = lastIpAddress - firstIpAddress + kOne;

    // We have two possibilities for determining the right prefix boundary

    // Case 1. Find the largest possible prefix that firstIpAddress can be.
    //     Say we have an input range of 192.168.0.0 to 192.184.0.0.
    //     The number of IP addresses in the range is 1048576 = 2^20, so we
    //     would need a /12 (32-20). to cover that many IP addresses but the
    //     largest valid prefix that can start from 192.168.0.0 is /13.
    const int64_t firstAddressMaxBits = getLowestSetBit(firstIpAddress);

    // Case 2. Find the largest prefix length to cover N IP addresses.
    //     The number of IP addresses within a valid prefix must be a power of 2
    //     but the IP count in our IP ranges may not be a power of 2. If it
    //     isn't exactly a power of 2, we find the highest power of 2 that the
    //     doesn't overrun the ipCount.

    // If ipCount's bitLength is greater than the number of IP addresses (i.e.,
    // not a power of 2), then use 1 bit less.
    const int64_t ipCountBitLength = bitLength(ipCount);

    const int128_t numIpAddress = static_cast<int128_t>(1) << ipCountBitLength;
    const int64_t ipRangeMaxBits =
        numIpAddress > ipCount ? ipCountBitLength - 1 : ipCountBitLength;
    return std::min(firstAddressMaxBits, ipRangeMaxBits);
  }

  FOLLY_ALWAYS_INLINE static std::vector<std::tuple<int128_t, int8_t>>
  generateMinIpPrefixes(
      int128_t firstIpAddress,
      int128_t lastIpAddress,
      uint32_t ipVersionMaxBits) {
    std::vector<std::tuple<int128_t, int8_t>> ipPrefixSlices;
    // i.e., while firstIpAddress <= lastIpAddress
    while (IPADDRESS()->compare(firstIpAddress, lastIpAddress) <= 0) {
      // find the number of bits for the next prefix in the range
      const auto rangeBits = findRangeBits(firstIpAddress, lastIpAddress);

      const auto prefixLength = ipVersionMaxBits - rangeBits;

      VELOX_USER_CHECK(
          prefixLength >= 0 && prefixLength <= ipVersionMaxBits,
          fmt::format(
              "Recieved invalid ipprefix:{} prefix length: {}",
              firstIpAddress,
              prefixLength));

      ipPrefixSlices.emplace_back(firstIpAddress, prefixLength);

      int128_t ipCount = static_cast<int128_t>(1)
          << static_cast<int128_t>(ipVersionMaxBits - prefixLength);
      firstIpAddress += ipCount;
    }
    return ipPrefixSlices;
  }

  FOLLY_ALWAYS_INLINE static std::vector<std::pair<int128_t, int128_t>>
  mergeIpRanges(const std::vector<std::tuple<int128_t, int8_t>>& prefixes) {
    std::vector<std::pair<int128_t, int128_t>> mergedRanges;
    mergedRanges.reserve(prefixes.size());

    int128_t firstIpAddress = std::get<0>(prefixes.front());
    int128_t lastIpAddress = getIPSubnetMax(
        std::get<0>(prefixes.front()), std::get<1>(prefixes.front()));

    /*
      There are four cases to cover for two IP ranges where range1.startIp <=
      range2.startIp

      1. Could be equal/duplicates.
          [-------]
          [-------]
          In this case, we just ignore the second one.

      2. Second could be subnet/contained within first.
          [-------]  OR  [-------]  OR  [-------]
            [---]        [----]            [----]
          In this case we ignore the second one.

      3. Second could be adjacent/contiguous with the first.
          [-------]
                    [-------]
          In this case we extend the range to include the last IP address of the
          second one.

      4. Second can be disjoint from the first.
          [-------]
                      [-------]
          In this case the first range is finalized, and the second range
          becomes the current one.
    */

    for (size_t i = 1; i < prefixes.size(); i++) {
      int128_t nextFirstIpAddress = std::get<0>(prefixes[i]);
      int128_t nextLastIpAddress =
          getIPSubnetMax(std::get<0>(prefixes[i]), std::get<1>(prefixes[i]));

      // If nextFirstIpAddress <= lastIpAddress then there is overlap.
      // However, based on the properties of the input sorted array, this will
      // always mean that the next* range is a subnet of [firstIpAddress,
      // lastIpAddress]. We just ignore these prefixes since they are already
      // covered (case 1 and case 2).
      //
      // i.e. nextFirstIpAddress > lastIpAddress -- the next range does not
      // overlap the first
      if (IPADDRESS()->compare(lastIpAddress, nextFirstIpAddress) < 0) {
        // If they are not contiguous (case 4), finalize the range.
        // Otherwise, extend the current range (case 3).
        if (IPADDRESS()->compare(
                lastIpAddress + static_cast<int128_t>(1), nextFirstIpAddress) !=
            0) {
          mergedRanges.emplace_back(firstIpAddress, lastIpAddress);
          firstIpAddress = nextFirstIpAddress;
        }
        lastIpAddress = nextLastIpAddress;
      }
    }

    mergedRanges.emplace_back(firstIpAddress, lastIpAddress);
    return mergedRanges;
  }
};

template <typename T>
struct IPPrefixSubnetsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<IPPrefix>>& result,
      const arg_type<IPPrefix>& prefix,
      const arg_type<int64_t>& newPrefixLength) {
    const bool inputIsIpV4 = isIPv4(*prefix.template at<0>());

    if (newPrefixLength < 0 || (inputIsIpV4 && newPrefixLength > 32) ||
        (!inputIsIpV4 && newPrefixLength > 128)) {
      VELOX_USER_FAIL(fmt::format(
          "Invalid prefix length for IPv{}: {}",
          inputIsIpV4 ? 4 : 6,
          newPrefixLength));
    }

    int8_t inputPrefixLength = *prefix.template at<1>();
    // An IP prefix is a 'network', or group of contiguous IP addresses. The
    // common format for describing IP prefixes is
    // uses 2 parts separated by a '/': (1) the IP address part and the (2)
    // prefix length part (also called subnet size or CIDR). For example,
    // in 9.255.255.0/24, 9.255.255.0 is the IP address part and 24 is the
    // prefix length. The prefix length describes how many IP addresses the
    // prefix contains in terms of the leading number of bits required. A higher
    // number of bits means smaller number of IP addresses. Subnets inherently
    // mean smaller groups of IP addresses. We can only disaggregate a prefix if
    // the prefix length is the same length or longer (more-specific) than the
    // length of the input prefix. E.g., if the input prefix is 9.255.255.0/24,
    // the prefix length can be /24, /25, /26, etc... but not 23 or larger value
    // than 24.

    // If inputPrefixLength > newPrefixLength, there are no new prefixes and we
    // will return an empty array.
    uint128_t newPrefixCount = inputPrefixLength <= newPrefixLength
        ? 1 << (newPrefixLength - inputPrefixLength)
        : 0;
    if (newPrefixCount == 0) {
      return;
    }

    if (newPrefixCount == 1) {
      writeResults(result, *prefix.template at<0>(), *prefix.template at<1>());
      return;
    }

    const int64_t ipVersionMaxBits =
        inputIsIpV4 ? ipaddress::kIPV4Bits : ipaddress::kIPV6Bits;
    const uint128_t newPrefixIpCount = static_cast<uint128_t>(1)
        << (ipVersionMaxBits - newPrefixLength);

    int128_t currentIpAddress = *prefix.template at<0>();

    for (uint128_t i = 0; i < newPrefixCount; i++) {
      writeResults(result, currentIpAddress, newPrefixLength);
      currentIpAddress += newPrefixIpCount;
    }
    return;
  }

 private:
  FOLLY_ALWAYS_INLINE static void writeResults(
      exec::ArrayWriter<IPPrefix>& result,
      int128_t ipaddress,
      int8_t prefixLength) {
    result.add_item() = std::make_tuple(ipaddress, prefixLength);
  }
};

template <typename T>
struct IsPrivateIPFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(
      out_type<bool>& result,
      const arg_type<IPAddress>& ipadddress) {
    const bool isIpV4 = isIPv4(*ipadddress);
    const int128_t ipAsBigInt = *ipadddress;

    const auto& rangesToCheck =
        isIpV4 ? privateIPv4AddressRanges() : privateIPv6AddressRanges();

    for (const auto& range : rangesToCheck) {
      const int128_t startIp = range.first;
      const int128_t endIp = range.second;

      if (ipAsBigInt < startIp) {
        result = false;
        return;
      }

      if (ipAsBigInt <= endIp) {
        result = true;
        return;
      }
    }
    result = false;
    return;
  }

 private:
  FOLLY_ALWAYS_INLINE static const std::vector<std::string>&
  getPrivatePrefixes() {
    const static std::vector<std::string> kPrivatePrefixes = {
        // IPv4 private ranges
        {"0.0.0.0/8"}, // RFC1122: "This host on this network"
        {"10.0.0.0/8"}, // RFC1918: Private-Use
        {"100.64.0.0/10"}, // RFC6598: Shared Address Space
        {"127.0.0.0/8"}, // RFC1122: Loopback
        {"169.254.0.0/16"}, // RFC3927: Link Local
        {"172.16.0.0/12"}, // RFC1918: Private-Use
        {"192.0.0.0/24"}, // RFC6890: IETF Protocol Assignments
        {"192.0.2.0/24"}, // RFC5737: Documentation (TEST-NET-1)
        {"192.88.99.0/24"}, // RFC3068: 6to4 Relay anycast
        {"192.168.0.0/16"}, // RFC1918: Private-Use
        {"198.18.0.0/15"}, // RFC2544: Benchmarking
        {"198.51.100.0/24"}, // RFC5737: Documentation (TEST-NET-2)
        {"203.0.113.0/24"}, // RFC5737: Documentation (TEST-NET-3)
        {"240.0.0.0/4"}, // RFC1112: Reserved
        // IPv6 private ranges
        {"::/127"}, // RFC4291: Unspecified address and Loopback address
        {"64:ff9b:1::/48"}, // RFC8215: IPv4-IPv6 Translation
        {"100::/64"}, // RFC6666: Discard-Only Address Block
        {"2001:2::/48"}, // RFC5180, RFC Errata 1752: Benchmarking
        {"2001:db8::/32"}, // RFC3849: Documentation
        {"2001::/23"}, // RFC2928: IETF Protocol Assignments
        {"5f00::/16"}, // RFC-ietf-6man-sids-06: Segment Routing (SRv6)
        {"fe80::/10"}, // RFC4291: Link-Local Unicast
        {"fc00::/7"}, // RFC4193, RFC8190: Unique Local
    };
    return kPrivatePrefixes;
  }

  FOLLY_ALWAYS_INLINE static std::vector<std::pair<int128_t, int128_t>>
  generatePrivateIPAddressRanges(bool isIpv4) {
    std::vector<std::pair<int128_t, int128_t>> privateIpV4AddressRanges;
    std::vector<std::pair<int128_t, int128_t>> privateIpV6AddressRanges;
    const auto& privatePrefixes = getPrivatePrefixes();
    for (const auto& prefix : privatePrefixes) {
      auto tryIpPrefix = ipaddress::tryParseIpPrefixString(prefix);
      if (tryIpPrefix.hasError()) {
        VELOX_USER_FAIL(
            "Invalid IP prefix string: {}. Error: {}",
            prefix,
            tryIpPrefix.error());
      }
      const auto& ipPrefix = tryIpPrefix.value();

      const int128_t startingIpAsBigInt = ipPrefix.first;
      const int128_t endingIpAsBigInt =
          getIPSubnetMax(ipPrefix.first, ipPrefix.second);

      if (isIPv4(ipPrefix.first)) {
        privateIpV4AddressRanges.emplace_back(
            startingIpAsBigInt, endingIpAsBigInt);
      } else {
        privateIpV6AddressRanges.emplace_back(
            startingIpAsBigInt, endingIpAsBigInt);
      }
    }
    std::sort(
        privateIpV4AddressRanges.begin(),
        privateIpV4AddressRanges.end(),
        [](const auto& a, const auto& b) { return a.first < b.first; });
    std::sort(
        privateIpV6AddressRanges.begin(),
        privateIpV6AddressRanges.end(),
        [](const auto& a, const auto& b) { return a.first < b.first; });
    return isIpv4 ? privateIpV4AddressRanges : privateIpV6AddressRanges;
  }

  FOLLY_ALWAYS_INLINE static const std::vector<std::pair<int128_t, int128_t>>&
  privateIPv6AddressRanges() {
    const static std::vector<std::pair<int128_t, int128_t>>
        kPrivateIPv6AddressRanges =
            generatePrivateIPAddressRanges(/*isIpv4=*/false);
    return kPrivateIPv6AddressRanges;
  }

  FOLLY_ALWAYS_INLINE static const std::vector<std::pair<int128_t, int128_t>>&
  privateIPv4AddressRanges() {
    const static std::vector<std::pair<int128_t, int128_t>>
        kPrivateIPv4AddressRanges =
            generatePrivateIPAddressRanges(/*isIpv4=*/true);
    return kPrivateIPv4AddressRanges;
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
  registerFunction<IPPrefixCollapseFunction, Array<IPPrefix>, Array<IPPrefix>>(
      {prefix + "ip_prefix_collapse"});
  registerFunction<IPPrefixSubnetsFunction, Array<IPPrefix>, IPPrefix, int64_t>(
      {prefix + "ip_prefix_subnets"});
  registerFunction<IsPrivateIPFunction, bool, IPAddress>(
      {prefix + "is_private_ip"});
}

} // namespace facebook::velox::functions
