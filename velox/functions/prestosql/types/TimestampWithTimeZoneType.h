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

#include "velox/expression/CastExpr.h"
#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"
#include "velox/vector/VectorTypeUtils.h"

namespace facebook::velox {

class TimestampWithTimeZoneCastOperator : public exec::CastOperator {
 public:
  static const std::shared_ptr<const CastOperator>& get() {
    static const std::shared_ptr<const CastOperator> instance{
        new TimestampWithTimeZoneCastOperator()};

    return instance;
  }

  bool isSupportedFromType(const TypePtr& other) const override;

  bool isSupportedToType(const TypePtr& other) const override;

  void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override;

  void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override;

 private:
  TimestampWithTimeZoneCastOperator() = default;
};

/// Represents timestamp with time zone as a number of milliseconds since epoch
/// and time zone ID.
class TimestampWithTimeZoneType : public BigintType {
  TimestampWithTimeZoneType() = default;

 public:
  static const std::shared_ptr<const TimestampWithTimeZoneType>& get() {
    static const std::shared_ptr<const TimestampWithTimeZoneType> instance =
        std::shared_ptr<TimestampWithTimeZoneType>(
            new TimestampWithTimeZoneType());

    return instance;
  }

  bool equivalent(const Type& other) const override {
    // Pointer comparison works since this type is a singleton.
    return this == &other;
  }

  const char* name() const override {
    return "TIMESTAMP WITH TIME ZONE";
  }

  const std::vector<TypeParameter>& parameters() const override {
    static const std::vector<TypeParameter> kEmpty = {};
    return kEmpty;
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

inline bool isTimestampWithTimeZoneType(const TypePtr& type) {
  // Pointer comparison works since this type is a singleton.
  return TimestampWithTimeZoneType::get() == type;
}

inline std::shared_ptr<const TimestampWithTimeZoneType>
TIMESTAMP_WITH_TIME_ZONE() {
  return TimestampWithTimeZoneType::get();
}

// Type used for function registration.
struct TimestampWithTimezoneT {
  using type = int64_t;
  static constexpr const char* typeName = "timestamp with time zone";
};

using TimestampWithTimezone = CustomType<TimestampWithTimezoneT>;

class TimestampWithTimeZoneTypeFactories : public CustomTypeFactories {
 public:
  TypePtr getType() const override {
    return TIMESTAMP_WITH_TIME_ZONE();
  }

  // Type casting from and to TimestampWithTimezone is not supported yet.
  exec::CastOperatorPtr getCastOperator() const override {
    return TimestampWithTimeZoneCastOperator::get();
  }
};

void registerTimestampWithTimeZoneType();

using TimeZoneKey = int16_t;

constexpr int32_t kTimezoneMask = 0xFFF;
constexpr int32_t kMillisShift = 12;

inline int64_t unpackMillisUtc(int64_t dateTimeWithTimeZone) {
  return dateTimeWithTimeZone >> kMillisShift;
}

inline TimeZoneKey unpackZoneKeyId(int64_t dateTimeWithTimeZone) {
  return dateTimeWithTimeZone & kTimezoneMask;
}

inline int64_t pack(int64_t millisUtc, int16_t timeZoneKey) {
  return (millisUtc << kMillisShift) | (timeZoneKey & kTimezoneMask);
}

inline int64_t pack(const Timestamp& timestamp, int16_t timeZoneKey) {
  return pack(timestamp.toMillis(), timeZoneKey);
}

inline Timestamp unpackTimestampUtc(int64_t dateTimeWithTimeZone) {
  return Timestamp::fromMillis(unpackMillisUtc(dateTimeWithTimeZone));
}

} // namespace facebook::velox
