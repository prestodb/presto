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

#include "velox/functions/prestosql/types/IPAddressRegistration.h"

#include "velox/expression/CastExpr.h"
#include "velox/functions/prestosql/types/IPAddressType.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"

namespace facebook::velox {
namespace {
class IPAddressCastOperator : public exec::CastOperator {
 public:
  bool isSupportedFromType(const TypePtr& other) const override {
    switch (other->kind()) {
      case TypeKind::VARBINARY:
      case TypeKind::VARCHAR:
        return true;
      case TypeKind::ROW:
        if (isIPPrefixType(other)) {
          return true;
        }
        [[fallthrough]];
      default:
        return false;
    }
  }

  bool isSupportedToType(const TypePtr& other) const override {
    switch (other->kind()) {
      case TypeKind::VARBINARY:
      case TypeKind::VARCHAR:
        return true;
      case TypeKind::ROW:
        if (isIPPrefixType(other)) {
          return true;
        }
        [[fallthrough]];
      default:
        return false;
    }
  }

  void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);

    if (input.typeKind() == TypeKind::VARCHAR) {
      castFromString(input, context, rows, *result);
    } else if (input.typeKind() == TypeKind::VARBINARY) {
      castFromVarbinary(input, context, rows, *result);
    } else if (
        input.typeKind() == TypeKind::ROW && isIPPrefixType(input.type())) {
      castFromIPPrefix(input, context, rows, *result);
    } else {
      VELOX_UNSUPPORTED(
          "Cast from {} to IPAddress not supported", resultType->toString());
    }
  }

  void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);

    if (resultType->kind() == TypeKind::VARCHAR) {
      castToString(input, context, rows, *result);
    } else if (resultType->kind() == TypeKind::VARBINARY) {
      castToVarbinary(input, context, rows, *result);
    } else if (
        resultType->kind() == TypeKind::ROW && isIPPrefixType(resultType)) {
      castToIPPrefix(input, context, rows, *result);
    } else {
      VELOX_UNSUPPORTED(
          "Cast from IPAddress to {} not supported", resultType->toString());
    }
  }

 private:
  static void castToString(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<StringView>>();
    const auto* ipaddresses = input.as<SimpleVector<int128_t>>();
    folly::ByteArray16 addrBytes;

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto intAddr = ipaddresses->valueAt(row);
      memcpy(&addrBytes, &intAddr, ipaddress::kIPAddressBytes);

      std::reverse(addrBytes.begin(), addrBytes.end());
      folly::IPAddressV6 v6Addr(addrBytes);

      exec::StringWriter result(flatResult, row);
      if (v6Addr.isIPv4Mapped()) {
        result.append(v6Addr.createIPv4().str());
      } else {
        result.append(v6Addr.str());
      }
      result.finalize();
    });
  }

  static void castFromString(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<int128_t>>();
    const auto* ipAddressStrings = input.as<SimpleVector<StringView>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto ipAddressString = ipAddressStrings->valueAt(row);
      auto maybeIpAsInt128 =
          ipaddress::tryGetIPv6asInt128FromString(ipAddressString);

      if (maybeIpAsInt128.hasError()) {
        if (threadSkipErrorDetails()) {
          context.setStatus(row, Status::UserError());
        } else {
          context.setStatus(
              row,
              Status::UserError("Invalid IP address '{}'", ipAddressString));
        }
        return;
      }
      flatResult->set(row, maybeIpAsInt128.value());
    });
  }

  static void castToVarbinary(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<StringView>>();
    const auto* ipaddresses = input.as<SimpleVector<int128_t>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto intAddr = ipaddresses->valueAt(row);
      folly::ByteArray16 addrBytes;
      memcpy(&addrBytes, &intAddr, ipaddress::kIPAddressBytes);
      std::reverse(addrBytes.begin(), addrBytes.end());

      exec::StringWriter result(flatResult, row);
      result.resize(ipaddress::kIPAddressBytes);
      memcpy(result.data(), &addrBytes, ipaddress::kIPAddressBytes);
      result.finalize();
    });
  }

  static void castToIPPrefix(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* rowVectorResult = result.as<RowVector>();
    auto intIpAddrVec =
        rowVectorResult->childAt(0)->asChecked<FlatVector<int128_t>>();
    auto intPrefixVec =
        rowVectorResult->childAt(1)->asChecked<FlatVector<int8_t>>();

    DecodedVector decoded(input, rows);
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto ipAddrVal = decoded.valueAt<int128_t>(row);
      const auto tryPrefixLength =
          ipaddress::tryIpPrefixLengthFromIPAddressType(ipAddrVal);
      if (FOLLY_UNLIKELY(tryPrefixLength.hasError())) {
        context.setStatus(row, std::move(tryPrefixLength).error());
        return;
      }

      intIpAddrVec->set(row, ipAddrVal);
      intPrefixVec->set(row, tryPrefixLength.value());
    });
  }

  static void castFromIPPrefix(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<int128_t>>();
    const auto* ipprefix = input.as<RowVector>();
    const auto* ipaddr = ipprefix->childAt(ipaddress::kIpRowIndex)
                             ->asChecked<SimpleVector<int128_t>>();

    context.applyToSelectedNoThrow(
        rows, [&](auto row) { flatResult->set(row, ipaddr->valueAt(row)); });
  }

  static void castFromVarbinary(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<int128_t>>();
    const auto* ipAddressBinaries = input.as<SimpleVector<StringView>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      int128_t intAddr;
      folly::ByteArray16 addrBytes = {};
      const auto ipAddressBinary = ipAddressBinaries->valueAt(row);

      if (ipAddressBinary.size() == ipaddress::kIPV4AddressBytes) {
        addrBytes[ipaddress::kIPV4ToV6FFIndex] = 0xFF;
        addrBytes[ipaddress::kIPV4ToV6FFIndex + 1] = 0xFF;
        memcpy(
            &addrBytes[ipaddress::kIPV4ToV6Index],
            ipAddressBinary.data(),
            ipaddress::kIPV4AddressBytes);
      } else if (ipAddressBinary.size() == ipaddress::kIPAddressBytes) {
        memcpy(&addrBytes, ipAddressBinary.data(), ipaddress::kIPAddressBytes);
      } else {
        if (threadSkipErrorDetails()) {
          context.setStatus(row, Status::UserError());
        } else {
          context.setStatus(
              row,
              Status::UserError(
                  "Invalid IP address binary length: {}",
                  ipAddressBinary.size()));
        }
        return;
      }

      std::reverse(addrBytes.begin(), addrBytes.end());
      memcpy(&intAddr, &addrBytes, ipaddress::kIPAddressBytes);
      flatResult->set(row, intAddr);
    });
  }
};

class IPAddressTypeFactories : public CustomTypeFactories {
 public:
  IPAddressTypeFactories() = default;

  TypePtr getType(const std::vector<TypeParameter>& parameters) const override {
    VELOX_CHECK(parameters.empty());
    return IPADDRESS();
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return std::make_shared<IPAddressCastOperator>();
  }

  AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& /*config*/) const override {
    return nullptr;
  }
};
} // namespace

void registerIPAddressType() {
  registerCustomType(
      "ipaddress", std::make_unique<const IPAddressTypeFactories>());
}
} // namespace facebook::velox
