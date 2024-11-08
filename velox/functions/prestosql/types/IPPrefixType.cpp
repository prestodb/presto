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
#include <folly/small_vector.h>

#include "velox/expression/CastExpr.h"
#include "velox/expression/VectorWriters.h"
#include "velox/functions/prestosql/types/IPPrefixType.h"

namespace facebook::velox {

namespace {

class IPPrefixCastOperator : public exec::CastOperator {
 public:
  bool isSupportedFromType(const TypePtr& other) const override {
    switch (other->kind()) {
      case TypeKind::VARCHAR:
        return true;
      default:
        return false;
    }
  }

  bool isSupportedToType(const TypePtr& other) const override {
    switch (other->kind()) {
      case TypeKind::VARCHAR:
        return true;
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
    switch (input.typeKind()) {
      case TypeKind::VARCHAR:
        return castFromString(input, context, rows, *result);
      default:
        VELOX_NYI(
            "Cast from {} to IPPrefix not yet supported",
            input.type()->toString());
    }
  }

  void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);
    switch (resultType->kind()) {
      case TypeKind::VARCHAR:
        return castToString(input, context, rows, *result);
      default:
        VELOX_NYI(
            "Cast from IPPrefix to {} not yet supported",
            resultType->toString());
    }
  }

 private:
  static void castToString(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<StringView>>();
    auto rowVector = input.as<RowVector>();
    auto rowType = rowVector->type();
    const auto* ipaddr = rowVector->childAt(ipaddress::kIpRowIndex)
                             ->as<SimpleVector<int128_t>>();
    const auto* prefix = rowVector->childAt(ipaddress::kIpPrefixRowIndex)
                             ->as<SimpleVector<int8_t>>();
    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto ipAddrVal = ipaddr->valueAt(row);
      // The string representation of the last byte needs
      // to be unsigned
      const uint8_t prefixVal = prefix->valueAt(row);

      // Copy the first 16 bytes into a ByteArray16.
      folly::ByteArray16 addrBytes;
      memcpy(&addrBytes, &ipAddrVal, ipaddress::kIPAddressBytes);
      // Reverse the bytes to get the correct order. Similar to
      // IPAddressType. We assume we're ALWAYS on a little endian machine.
      // Note: for big endian, we should not reverse the bytes.
      std::reverse(addrBytes.begin(), addrBytes.end());
      // // Construct a V6 address from the ByteArray16.
      folly::IPAddressV6 v6Addr(addrBytes);

      // Inline func to get string for ipv4 or ipv6 string
      const auto ipString =
          (v6Addr.isIPv4Mapped()) ? v6Addr.createIPv4().str() : v6Addr.str();

      // Format of string is {ipString}/{mask}
      auto stringRet = fmt::format("{}/{}", ipString, prefixVal);

      // Write the string to the result vector
      exec::StringWriter<false> result(flatResult, row);
      result.append(stringRet);
      result.finalize();
    });
  }

  static void castFromString(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* rowVectorResult = result.as<RowVector>();
    const auto* ipPrefixStrings = input.as<SimpleVector<StringView>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      auto ipAddressStringView = ipPrefixStrings->valueAt(row);
      auto tryIpPrefix = ipaddress::tryParseIpPrefixString(ipAddressStringView);
      if (tryIpPrefix.hasError()) {
        context.setStatus(row, std::move(tryIpPrefix.error()));
      }

      const auto& ipPrefix = tryIpPrefix.value();
      auto writer = exec::VectorWriter<Row<int128_t, int8_t>>();
      writer.init(*rowVectorResult);
      writer.setOffset(row);
      auto& rowWriter = writer.current();
      rowWriter.get_writer_at<0>() = ipPrefix.first;
      rowWriter.get_writer_at<1>() = ipPrefix.second;
      writer.commit();
    });
  }
};

class IPPrefixTypeFactories : public CustomTypeFactories {
 public:
  TypePtr getType() const override {
    return IPPrefixType::get();
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return std::make_shared<IPPrefixCastOperator>();
  }
};

} // namespace

void registerIPPrefixType() {
  registerCustomType(
      "ipprefix", std::make_unique<const IPPrefixTypeFactories>());
}

} // namespace facebook::velox
