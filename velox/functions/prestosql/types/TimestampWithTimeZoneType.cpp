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
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"
#include "velox/type/Timestamp.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox {
namespace {

void castFromTimestamp(
    const SimpleVector<Timestamp>& inputVector,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    VectorPtr& result) {
  const auto& config = context.execCtx()->queryCtx()->queryConfig();
  const auto& sessionTzName = config.sessionTimezone();
  int64_t sessionTzID = 0;
  if (!sessionTzName.empty()) {
    sessionTzID = util::getTimeZoneID(sessionTzName);
  }
  const auto adjustTimestampToTimezone = config.adjustTimestampToTimezone();

  auto timestampWithTzVector = result->asFlatVector<int64_t>();
  auto rawTimestampWithTzValues =
      timestampWithTzVector->values()->asMutable<int64_t>();
  timestampWithTzVector->clearNulls(rows);

  context.applyToSelectedNoThrow(rows, [&](auto row) {
    auto ts = inputVector.valueAt(row);
    if (!adjustTimestampToTimezone) {
      // Treat TIMESTAMP as wall time in session time zone. This means that in
      // order to get its UTC representation we need to shift the value by the
      // offset of the time zone.
      ts.toGMT(sessionTzID);
    }
    rawTimestampWithTzValues[row] = pack(ts.toMillis(), sessionTzID);
  });
}

void castFromString(
    const SimpleVector<StringView>& inputVector,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    VectorPtr& result) {
  auto timestampWithTzVector = result->asFlatVector<int64_t>();
  auto rawTimestampWithTzValues =
      timestampWithTzVector->values()->asMutable<int64_t>();
  timestampWithTzVector->clearNulls(rows);

  context.applyToSelectedNoThrow(rows, [&](auto row) {
    const auto castResult = util::fromTimestampWithTimezoneString(
        inputVector.valueAt(row).data(), inputVector.valueAt(row).size());
    if (castResult.hasError()) {
      context.setStatus(row, castResult.error());
    } else {
      auto [ts, tzID] = castResult.value();
      // Input string may not contain a timezone - if so, it is interpreted in
      // session timezone.
      if (tzID == -1) {
        const auto& config = context.execCtx()->queryCtx()->queryConfig();
        const auto& sessionTzName = config.sessionTimezone();
        if (!sessionTzName.empty()) {
          tzID = util::getTimeZoneID(sessionTzName);
        }
      }
      ts.toGMT(tzID);
      rawTimestampWithTzValues[row] = pack(ts.toMillis(), tzID);
    }
  });
}

template <TypeKind kind>
void castToTimestampWithTimeZone(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    VectorPtr& result) {
  if (kind == TypeKind::TIMESTAMP) {
    const auto inputVector = input.as<SimpleVector<Timestamp>>();
    castFromTimestamp(*inputVector, context, rows, result);
  } else if (kind == TypeKind::VARCHAR) {
    const auto inputVector = input.as<SimpleVector<StringView>>();
    castFromString(*inputVector, context, rows, result);
  } else {
    VELOX_UNSUPPORTED(
        "Cast from {} to TIMESTAMP WITH TIME ZONE not yet supported",
        mapTypeKindToName(kind));
  }
}

void castToTimestamp(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    BaseVector& result) {
  const auto& config = context.execCtx()->queryCtx()->queryConfig();
  const auto adjustTimestampToTimezone = config.adjustTimestampToTimezone();
  auto* flatResult = result.as<FlatVector<Timestamp>>();
  const auto* timestamps = input.as<SimpleVector<int64_t>>();

  context.applyToSelectedNoThrow(rows, [&](auto row) {
    auto timestampWithTimezone = timestamps->valueAt(row);
    auto ts = unpackTimestampUtc(timestampWithTimezone);
    if (!adjustTimestampToTimezone) {
      // Convert UTC to the given time zone.
      ts.toTimezone(unpackZoneKeyId(timestampWithTimezone));
    }
    flatResult->set(row, ts);
  });
}

void castToDate(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    BaseVector& result) {
  auto* flatResult = result.as<FlatVector<int32_t>>();
  const auto* timestampVector = input.as<SimpleVector<int64_t>>();

  context.applyToSelectedNoThrow(rows, [&](auto row) {
    auto timestampWithTimezone = timestampVector->valueAt(row);
    auto timestamp = unpackTimestampUtc(timestampWithTimezone);
    timestamp.toTimezone(unpackZoneKeyId(timestampWithTimezone));

    const auto days = util::toDate(timestamp, nullptr);
    flatResult->set(row, days);
  });
}

} // namespace

bool TimestampWithTimeZoneCastOperator::isSupportedFromType(
    const TypePtr& other) const {
  switch (other->kind()) {
    case TypeKind::TIMESTAMP:
      return true;
    case TypeKind::VARCHAR:
      return true;
    case TypeKind::INTEGER:
      // TODO: support cast from DATE
    default:
      return false;
  }
}

bool TimestampWithTimeZoneCastOperator::isSupportedToType(
    const TypePtr& other) const {
  switch (other->kind()) {
    case TypeKind::TIMESTAMP:
      return true;
    case TypeKind::VARCHAR:
      // TODO: support cast to VARCHAR
    case TypeKind::INTEGER:
      return other->isDate();
    default:
      return false;
  }
}

void TimestampWithTimeZoneCastOperator::castTo(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    const TypePtr& resultType,
    VectorPtr& result) const {
  context.ensureWritable(rows, resultType, result);
  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      castToTimestampWithTimeZone,
      input.typeKind(),
      input,
      context,
      rows,
      result);
}

void TimestampWithTimeZoneCastOperator::castFrom(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    const TypePtr& resultType,
    VectorPtr& result) const {
  context.ensureWritable(rows, resultType, result);

  if (resultType->kind() == TypeKind::TIMESTAMP) {
    castToTimestamp(input, context, rows, *result);
  } else if (resultType->kind() == TypeKind::INTEGER) {
    VELOX_CHECK(resultType->isDate());
    castToDate(input, context, rows, *result);
  } else {
    VELOX_UNSUPPORTED(
        "Cast from TIMESTAMP WITH TIME ZONE to {} not yet supported",
        resultType->toString());
  }
}

void registerTimestampWithTimeZoneType() {
  registerCustomType(
      "timestamp with time zone",
      std::make_unique<const TimestampWithTimeZoneTypeFactories>());
}

} // namespace facebook::velox
