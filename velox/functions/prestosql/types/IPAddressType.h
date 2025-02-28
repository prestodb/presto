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

#include <folly/IPAddress.h>

#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"

namespace facebook::velox {

namespace ipaddress {
constexpr int kIPV4AddressBytes = 4;
constexpr int kIPV4ToV6FFIndex = 10;
constexpr int kIPV4ToV6Index = 12;
constexpr int kIPAddressBytes = 16;

inline folly::ByteArray16 toIPv6ByteArray(const int128_t& ipAddr) {
  folly::ByteArray16 bytes{{0}};
  memcpy(bytes.data(), &ipAddr, sizeof(ipAddr));
  // Reverse because the velox is always on little endian system
  // and the byte array needs to be big endian (network byte order)
  std::reverse(bytes.begin(), bytes.end());
  return bytes;
}

inline folly::Expected<int128_t, folly::IPAddressFormatError>
tryGetIPv6asInt128FromString(const std::string& ipAddressStr) {
  auto maybeIp = folly::IPAddress::tryFromString(ipAddressStr);
  if (maybeIp.hasError()) {
    return folly::makeUnexpected(maybeIp.error());
  }

  int128_t intAddr;
  folly::IPAddress addr = maybeIp.value();
  auto addrBytes = folly::IPAddress::createIPv6(addr).toByteArray();
  std::reverse(addrBytes.begin(), addrBytes.end());
  memcpy(&intAddr, &addrBytes, kIPAddressBytes);
  return intAddr;
}
} // namespace ipaddress

class IPAddressType : public HugeintType {
  IPAddressType() : HugeintType(/*providesCustomComparison*/ true) {}

 public:
  static const std::shared_ptr<const IPAddressType>& get() {
    static const std::shared_ptr<const IPAddressType> instance{
        new IPAddressType()};

    return instance;
  }

  int32_t compare(const int128_t& left, const int128_t& right) const override {
    const auto leftAddrBytes = ipaddress::toIPv6ByteArray(left);
    const auto rightAddrBytes = ipaddress::toIPv6ByteArray(right);
    return memcmp(
        leftAddrBytes.begin(),
        rightAddrBytes.begin(),
        ipaddress::kIPAddressBytes);
  }

  uint64_t hash(const int128_t& value) const override {
    return folly::hasher<int128_t>()(value);
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "IPADDRESS";
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
};

FOLLY_ALWAYS_INLINE bool isIPAddressType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return IPAddressType::get() == type;
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const IPAddressType> IPADDRESS() {
  return IPAddressType::get();
}

// Type used for function registration.
struct IPAddressT {
  using type = int128_t;
  static constexpr const char* typeName = "ipaddress";
};

using IPAddress = CustomType<IPAddressT, /*providesCustomComparison*/ true>;

} // namespace facebook::velox
