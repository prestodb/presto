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

#include <folly/small_vector.h>

#include "velox/common/base/Status.h"
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"

namespace facebook::velox {

namespace ipaddress {
constexpr uint8_t kIPV4Bits = 32;
constexpr uint8_t kIPV6Bits = 128;
constexpr int kIPPrefixLengthIndex = 16;
constexpr int kIPPrefixBytes = 17;
constexpr auto kIpRowIndex = "ip";
constexpr auto kIpPrefixRowIndex = "prefix";

namespace {
auto splitIpSlashCidr(folly::StringPiece ipSlashCidr) {
  folly::small_vector<folly::StringPiece, 2> vec;
  folly::split('/', ipSlashCidr, vec);
  return vec;
}

Status handleFailedToCreateNetworkError(
    folly::StringPiece ipaddress,
    folly::CIDRNetworkError error) {
  if (threadSkipErrorDetails()) {
    return Status::UserError();
  }

  switch (error) {
    case folly::CIDRNetworkError::INVALID_DEFAULT_CIDR: {
      return Status::UserError(
          "defaultCidr must be <= std::numeric_limits<uint8_t>::max()");
    }
    case folly::CIDRNetworkError::INVALID_IP_SLASH_CIDR: {
      return Status::UserError(
          "Invalid IP address string received. Received string:{} of length:{}",
          ipaddress,
          ipaddress.size());
    }
    case folly::CIDRNetworkError::INVALID_IP: {
      const auto vec = splitIpSlashCidr(ipaddress);
      return Status::UserError(
          "Invalid IP address '{}'", vec.size() > 0 ? vec.at(0) : "");
    }
    case folly::CIDRNetworkError::INVALID_CIDR: {
      auto const vec = splitIpSlashCidr(ipaddress);
      return Status::UserError(
          "Mask value '{}' not a valid mask", vec.size() > 1 ? vec.at(1) : "");
    }
    case folly::CIDRNetworkError::CIDR_MISMATCH: {
      const auto vec = splitIpSlashCidr(ipaddress);
      if (!vec.empty()) {
        const auto subnet = folly::IPAddress::tryFromString(vec.at(0)).value();
        return Status::UserError(
            "CIDR value '{}' is > network bit count '{}'",
            vec.size() == 2 ? vec.at(1)
                            : folly::to<std::string>(
                                  subnet.isV4() ? ipaddress::kIPV4Bits
                                                : ipaddress::kIPV6Bits),
            subnet.bitCount());
      }
      return Status::UserError(
          "Invalid IP address of size:{} received", ipaddress.size());
    }
    default:
      return Status::UserError(
          "Unknown parsing error when parsing IP address: {} ", ipaddress);
  }
}
} // namespace

inline folly::Expected<std::pair<int128_t, int8_t>, Status>
tryParseIpPrefixString(folly::StringPiece ipprefixString) {
  // Ensure '/' is present
  if (ipprefixString.find('/') == std::string::npos) {
    return folly::makeUnexpected(
        threadSkipErrorDetails()
            ? Status::UserError()
            : Status::UserError(
                  "Invalid CIDR IP address specified. Expected IP/PREFIX format, got: {}",
                  ipprefixString));
  }

  auto tryCdirNetwork = folly::IPAddress::tryCreateNetwork(
      ipprefixString, /*defaultCidr*/ -1, /*applyMask*/ false);
  if (tryCdirNetwork.hasError()) {
    return folly::makeUnexpected(handleFailedToCreateNetworkError(
        ipprefixString, std::move(tryCdirNetwork.error())));
  }

  folly::ByteArray16 addrBytes;
  const auto& cdirNetwork = tryCdirNetwork.value();
  if (cdirNetwork.first.isIPv4Mapped() || cdirNetwork.first.isV4()) {
    // Validate that the prefix value is <= 32 for ipv4
    if (cdirNetwork.second > ipaddress::kIPV4Bits) {
      return folly::makeUnexpected(
          threadSkipErrorDetails()
              ? Status::UserError()
              : Status::UserError(
                    "CIDR value '{}' is > network bit count '{}'",
                    cdirNetwork.second,
                    ipaddress::kIPV4Bits));
    }
    auto ipv4Addr = folly::IPAddress::createIPv4(cdirNetwork.first);
    auto ipv4AddrWithMask = ipv4Addr.mask(cdirNetwork.second);
    auto ipv6Addr = ipv4AddrWithMask.createIPv6();
    addrBytes = ipv6Addr.toByteArray();
  } else {
    // Validate that the prefix value is <= 128 for ipv6
    if (cdirNetwork.second > ipaddress::kIPV6Bits) {
      return folly::makeUnexpected(
          threadSkipErrorDetails()
              ? Status::UserError()
              : Status::UserError(
                    "CIDR value '{}' is > network bit count '{}'",
                    cdirNetwork.second,
                    ipaddress::kIPV6Bits));
    }
    auto ipv6Addr = folly::IPAddress::createIPv6(cdirNetwork.first);
    auto ipv6AddrWithMask = ipv6Addr.mask(cdirNetwork.second);
    addrBytes = ipv6AddrWithMask.toByteArray();
  }

  int128_t intAddr;
  // Similar to IPAdressType, assume Velox is always on little endian systems
  std::reverse(addrBytes.begin(), addrBytes.end());
  memcpy(&intAddr, &addrBytes, ipaddress::kIPAddressBytes);
  return std::make_pair(intAddr, cdirNetwork.second);
}
}; // namespace ipaddress

class IPPrefixType : public RowType {
  IPPrefixType()
      : RowType(
            {ipaddress::kIpRowIndex, ipaddress::kIpPrefixRowIndex},
            {HUGEINT(), TINYINT()}) {}

 public:
  static const std::shared_ptr<const IPPrefixType>& get() {
    static const std::shared_ptr<const IPPrefixType> instance{
        new IPPrefixType()};

    return instance;
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "IPPREFIX";
  }

  std::string toString() const override {
    return name();
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = "Type";
    obj["type"] = name();
    return obj;
  }

  const std::vector<TypeParameter>& parameters() const override {
    static const std::vector<TypeParameter> kEmpty = {};
    return kEmpty;
  }
};

FOLLY_ALWAYS_INLINE bool isIPPrefixType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return IPPrefixType::get() == type;
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const IPPrefixType> IPPREFIX() {
  return IPPrefixType::get();
}

struct IPPrefixT {
  using type = Row<int128_t, int8_t>;
  static constexpr const char* typeName = "ipprefix";
};

using IPPrefix = CustomType<IPPrefixT>;

void registerIPPrefixType();

} // namespace facebook::velox
