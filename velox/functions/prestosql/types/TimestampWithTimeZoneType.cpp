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
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox {
namespace {

void castFromTimestamp(
    const SimpleVector<Timestamp>& inputVector,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    RowVector& rowResult) {
  const auto& config = context.execCtx()->queryCtx()->queryConfig();
  const auto& sessionTzName = config.sessionTimezone();
  int64_t sessionTzID = 0;
  if (!sessionTzName.empty()) {
    sessionTzID = util::getTimeZoneID(sessionTzName);
  }
  const auto adjustTimestampToTimezone = config.adjustTimestampToTimezone();

  auto timestampVector = rowResult.childAt(0)->asFlatVector<int64_t>();
  auto timezoneVector = rowResult.childAt(1)->asFlatVector<int16_t>();
  auto rawTsValues = timestampVector->values()->asMutable<int64_t>();
  auto rawTzValues = timezoneVector->values()->asMutable<int16_t>();
  timestampVector->clearNulls(rows);
  timezoneVector->clearNulls(rows);

  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputVector.isNullAt(row)) {
      rowResult.setNull(row, true);
    } else {
      auto ts = inputVector.valueAt(row);
      if (!adjustTimestampToTimezone) {
        // Treat TIMESTAMP as wall time in session time zone. This means that in
        // order to get its UTC representation we need to shift the value by the
        // offset of the time zone.
        ts.toGMT(sessionTzID);
      }
      rawTsValues[row] = ts.toMillis();
      rawTzValues[row] = sessionTzID;
    }
  });
}

template <TypeKind kind>
void castToTimestampWithTimeZone(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    RowVector& rowResult) {
  VELOX_CHECK_EQ(kind, TypeKind::TIMESTAMP)
  const auto inputVector = input.as<SimpleVector<Timestamp>>();
  castFromTimestamp(*inputVector, context, rows, rowResult);
}

void castToTimestamp(
    const RowVector& inputVector,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    FlatVector<Timestamp>& flatResult) {
  const auto& config = context.execCtx()->queryCtx()->queryConfig();
  const auto adjustTimestampToTimezone = config.adjustTimestampToTimezone();
  const auto timestampVector =
      inputVector.childAt(0)->as<SimpleVector<int64_t>>();
  const auto timezoneVector =
      inputVector.childAt(1)->as<SimpleVector<int16_t>>();

  context.applyToSelectedNoThrow(rows, [&](auto row) {
    if (inputVector.isNullAt(row)) {
      flatResult.setNull(row, true);
    } else {
      Timestamp ts = Timestamp::fromMillis(timestampVector->valueAt(row));
      if (!adjustTimestampToTimezone) {
        // Convert UTC to the given time zone.
        ts.toTimezone(timezoneVector->valueAt(row));
      }
      flatResult.set(row, ts);
    }
  });
}

template <TypeKind kind>
void castFromTimestampWithTimeZone(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    BaseVector& result) {
  VELOX_CHECK_EQ(kind, TypeKind::TIMESTAMP)

  const auto inputVector = input.as<RowVector>();
  auto flatResult = result.as<FlatVector<Timestamp>>();
  castToTimestamp(*inputVector, context, rows, *flatResult);
}
} // namespace

bool TimestampWithTimeZoneCastOperator::isSupportedFromType(
    const TypePtr& other) const {
  switch (other->kind()) {
    case TypeKind::TIMESTAMP:
      return true;
    case TypeKind::VARCHAR:
      // TODO: support cast from VARCHAR
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
      // TODO: support cast to DATE
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
  auto rowResult = result->as<RowVector>();
  for (const auto& child : rowResult->children()) {
    child->resize(rowResult->size());
  }

  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      castToTimestampWithTimeZone,
      input.typeKind(),
      input,
      context,
      rows,
      *rowResult);
}

void TimestampWithTimeZoneCastOperator::castFrom(
    const BaseVector& input,
    exec::EvalCtx& context,
    const SelectivityVector& rows,
    const TypePtr& resultType,
    VectorPtr& result) const {
  context.ensureWritable(rows, resultType, result);

  VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
      castFromTimestampWithTimeZone,
      result->typeKind(),
      input,
      context,
      rows,
      *result);
}

void registerTimestampWithTimeZoneType() {
  registerCustomType(
      "timestamp with time zone",
      std::make_unique<const TimestampWithTimeZoneTypeFactories>());
}

} // namespace facebook::velox
