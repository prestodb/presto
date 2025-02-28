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

inline folly::Expected<int8_t, Status> tryIpPrefixLengthFromIPAddressType(
    const int128_t& intIpAddr) {
  folly::ByteArray16 addrBytes = {0};
  memcpy(&addrBytes, &intIpAddr, sizeof(intIpAddr));
  std::reverse(addrBytes.begin(), addrBytes.end());
  auto tryV6Addr = folly::IPAddressV6::tryFromBinary(addrBytes);
  if (tryV6Addr.hasError()) {
    return folly::makeUnexpected(
        threadSkipErrorDetails()
            ? Status::UserError()
            : Status::UserError("Received invalid ip address"));
  }

  return tryV6Addr.value().isIPv4Mapped() ? ipaddress::kIPV4Bits
                                          : ipaddress::kIPV6Bits;
}

inline folly::Expected<std::pair<int128_t, int8_t>, Status>
tryParseIpPrefixString(folly::StringPiece ipprefixString) {
  // Ensure '/' is present
  if (ipprefixString.find('/') == std::string::npos) {
    return folly::makeUnexpected(
        threadSkipErrorDetails()
            ? Status::UserError()
            : Status::UserError(
                  "Cannot cast value to IPPREFIX: {}", ipprefixString));
  }

  auto tryCdirNetwork = folly::IPAddress::tryCreateNetwork(
      ipprefixString, /*defaultCidr*/ -1, /*applyMask*/ false);

  if (tryCdirNetwork.hasError()) {
    return folly::makeUnexpected(
        threadSkipErrorDetails()
            ? Status::UserError()
            : Status::UserError(
                  "Cannot cast value to IPPREFIX: {}", ipprefixString));
  }

  auto [ip, prefix] = tryCdirNetwork.value();
  if (prefix > ((ip.isIPv4Mapped() || ip.isV4()) ? ipaddress::kIPV4Bits
                                                 : ipaddress::kIPV6Bits)) {
    return folly::makeUnexpected(
        threadSkipErrorDetails()
            ? Status::UserError()
            : Status::UserError(
                  "Cannot cast value to IPPREFIX: {}", ipprefixString));
  }

  auto addrBytes = (ip.isIPv4Mapped() || ip.isV4())
      ? folly::IPAddress::createIPv4(ip).mask(prefix).createIPv6().toByteArray()
      : folly::IPAddress::createIPv6(ip).mask(prefix).toByteArray();

  std::reverse(addrBytes.begin(), addrBytes.end());

  int128_t intAddr = 0;
  static_assert(sizeof(intAddr) == ipaddress::kIPAddressBytes);
  memcpy(&intAddr, &addrBytes, ipaddress::kIPAddressBytes);
  return std::make_pair(intAddr, prefix);
}
}; // namespace ipaddress

class IPPrefixType : public RowType {
  IPPrefixType()
      : RowType(
            {ipaddress::kIpRowIndex, ipaddress::kIpPrefixRowIndex},
            {IPADDRESS(), TINYINT()}) {}

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

} // namespace facebook::velox
