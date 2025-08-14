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

#include "velox/functions/prestosql/types/BingTileRegistration.h"

#include "velox/common/fuzzer/ConstrainedGenerators.h"
#include "velox/expression/CastExpr.h"
#include "velox/functions/prestosql/types/BingTileType.h"

namespace facebook::velox {

namespace {

class BingTileCastOperator final : public exec::CastOperator {
  BingTileCastOperator() = default;

 public:
  static std::shared_ptr<const CastOperator> get() {
    VELOX_CONSTEXPR_SINGLETON BingTileCastOperator kInstance;
    return {std::shared_ptr<const CastOperator>{}, &kInstance};
  }

  bool isSupportedFromType(const TypePtr& other) const override {
    switch (other->kind()) {
      case TypeKind::BIGINT:
        return true;
      default:
        return false;
    }
  }

  bool isSupportedToType(const TypePtr& other) const override {
    switch (other->kind()) {
      case TypeKind::BIGINT:
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
    if (input.typeKind() == TypeKind::BIGINT) {
      auto* bingTileResult = result->asFlatVector<int64_t>();
      bingTileResult->clearNulls(rows);
      const auto inputVector = input.asChecked<SimpleVector<int64_t>>();
      // The values we just copy, and we will set errors later
      bingTileResult->copy(inputVector, rows, nullptr);

      // These are simple ops on int64s. We can vectorize for performance.
      // For example, we could:
      // 1. check validity isBingTileIntValid in parallel (branchless SIMD)
      // 2. bitwise-OR valid flags with `rows` selectivity (branchless SIMD)
      // 3. add an error to any false rows (uncommon)
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        const uint64_t tileInt = bingTileResult->valueAt(row);
        if (FOLLY_UNLIKELY(!BingTileType::isBingTileIntValid(tileInt))) {
          std::optional<std::string> reasonOpt =
              BingTileType::bingTileInvalidReason(tileInt);
          if (reasonOpt.has_value()) {
            context.setStatus(row, Status::UserError(*reasonOpt));
          } else {
            // isBingTileIntValid and bingTileInvalidReason have gotten out of
            // sync
            context.setStatus(
                row,
                Status::UnknownError(
                    "Unknown BingTile validation error for tile %ld: this is a bug in Velox",
                    tileInt));
          }
        }
      });

    } else {
      VELOX_UNSUPPORTED(
          "Cast from {} to BINGTILE not supported", input.type()->toString());
    }
  }

  void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);

    if (resultType->kind() == TypeKind::BIGINT) {
      auto* flatResult = result->asChecked<FlatVector<int64_t>>();
      flatResult->clearNulls(rows);
      const auto inputVector = input.asChecked<SimpleVector<int64_t>>();
      flatResult->copy(inputVector, rows, nullptr);
    } else {
      VELOX_UNSUPPORTED(
          "Cast from BINGTILE to {} not yet supported", resultType->toString());
    }
  }
};

class BingTileTypeFactory : public CustomTypeFactory {
 public:
  velox::TypePtr getType(
      const std::vector<velox::TypeParameter>& parameters) const override {
    VELOX_CHECK(parameters.empty());
    return BINGTILE();
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return BingTileCastOperator::get();
  }

  AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& config) const override {
    return std::make_shared<fuzzer::BingTileInputGenerator>(
        config.seed_, BINGTILE(), config.nullRatio_);
  }
};

} // namespace

void registerBingTileType() {
  registerCustomType("bingtile", std::make_unique<const BingTileTypeFactory>());
}

} // namespace facebook::velox
