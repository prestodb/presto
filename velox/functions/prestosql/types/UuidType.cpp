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

#include "velox/functions/prestosql/types/UuidType.h"
#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace facebook::velox {

namespace {

class UuidCastOperator : public exec::CastOperator {
 public:
  bool isSupportedFromType(const TypePtr& other) const override {
    return VARCHAR()->equivalent(*other);
  }

  bool isSupportedToType(const TypePtr& other) const override {
    return VARCHAR()->equivalent(*other);
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
    } else {
      VELOX_UNSUPPORTED(
          "Cast from {} to UUID not yet supported", resultType->toString());
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
    } else {
      VELOX_UNSUPPORTED(
          "Cast from UUID to {} not yet supported", resultType->toString());
    }
  }

 private:
  static void castToString(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<StringView>>();
    const auto* uuids = input.as<SimpleVector<int128_t>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto uuid = uuids->valueAt(row);

      boost::uuids::uuid u;
      memcpy(&u, &uuid, 16);

      std::string s = boost::lexical_cast<std::string>(u);

      exec::StringWriter<false> result(flatResult, row);
      result.append(s);
      result.finalize();
    });
  }

  static void castFromString(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      BaseVector& result) {
    auto* flatResult = result.as<FlatVector<int128_t>>();
    const auto* uuidStrings = input.as<SimpleVector<StringView>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto uuidString = uuidStrings->valueAt(row);

      auto uuid = boost::lexical_cast<boost::uuids::uuid>(uuidString);

      int128_t u;
      memcpy(&u, &uuid, 16);

      flatResult->set(row, u);
    });
  }
};

class UuidTypeFactories : public CustomTypeFactories {
 public:
  UuidTypeFactories() = default;

  TypePtr getType() const override {
    return UUID();
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return std::make_shared<UuidCastOperator>();
  }
};

} // namespace

void registerUuidType() {
  registerCustomType("uuid", std::make_unique<const UuidTypeFactories>());
}

} // namespace facebook::velox
