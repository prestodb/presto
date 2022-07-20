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

#include <velox/core/Expressions.h>
#include <velox/core/ITypedExpr.h>
#include <velox/type/Filter.h>
#include <velox/type/Subfield.h>

namespace facebook::velox::exec {

inline std::unique_ptr<common::BigintRange> lessThan(
    int64_t max,
    bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      std::numeric_limits<int64_t>::min(), max - 1, nullAllowed);
}

inline std::unique_ptr<common::BigintRange> lessThanOrEqual(
    int64_t max,
    bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      std::numeric_limits<int64_t>::min(), max, nullAllowed);
}

inline std::unique_ptr<common::BigintRange> greaterThan(
    int64_t min,
    bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      min + 1, std::numeric_limits<int64_t>::max(), nullAllowed);
}

inline std::unique_ptr<common::BigintRange> greaterThanOrEqual(
    int64_t min,
    bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(
      min, std::numeric_limits<int64_t>::max(), nullAllowed);
}

inline std::unique_ptr<common::NegatedBigintRange> notEqual(
    int64_t val,
    bool nullAllowed = false) {
  return std::make_unique<common::NegatedBigintRange>(val, val, nullAllowed);
}

inline std::unique_ptr<common::NegatedBigintRange>
notBetween(int64_t lower, int64_t upper, bool nullAllowed = false) {
  return std::make_unique<common::NegatedBigintRange>(
      lower, upper, nullAllowed);
}

inline std::unique_ptr<common::DoubleRange> lessThanDouble(
    double max,
    bool nullAllowed = false) {
  return std::make_unique<common::DoubleRange>(
      std::numeric_limits<double>::lowest(),
      true,
      true,
      max,
      false,
      true,
      nullAllowed);
}

inline std::unique_ptr<common::DoubleRange> lessThanOrEqualDouble(
    double max,
    bool nullAllowed = false) {
  return std::make_unique<common::DoubleRange>(
      std::numeric_limits<double>::lowest(),
      true,
      true,
      max,
      false,
      false,
      nullAllowed);
}

inline std::unique_ptr<common::DoubleRange> greaterThanDouble(
    double min,
    bool nullAllowed = false) {
  return std::make_unique<common::DoubleRange>(
      min,
      false,
      true,
      std::numeric_limits<double>::max(),
      true,
      true,
      nullAllowed);
}

inline std::unique_ptr<common::DoubleRange> greaterThanOrEqualDouble(
    double min,
    bool nullAllowed = false) {
  return std::make_unique<common::DoubleRange>(
      min,
      false,
      false,
      std::numeric_limits<double>::max(),
      true,
      true,
      nullAllowed);
}

inline std::unique_ptr<common::DoubleRange>
betweenDouble(double min, double max, bool nullAllowed = false) {
  return std::make_unique<common::DoubleRange>(
      min, false, false, max, false, false, nullAllowed);
}

inline std::unique_ptr<common::FloatRange> lessThanFloat(
    float max,
    bool nullAllowed = false) {
  return std::make_unique<common::FloatRange>(
      std::numeric_limits<float>::lowest(),
      true,
      true,
      max,
      false,
      true,
      nullAllowed);
}

inline std::unique_ptr<common::FloatRange> lessThanOrEqualFloat(
    float max,
    bool nullAllowed = false) {
  return std::make_unique<common::FloatRange>(
      std::numeric_limits<float>::lowest(),
      true,
      true,
      max,
      false,
      false,
      nullAllowed);
}

inline std::unique_ptr<common::FloatRange> greaterThanFloat(
    float min,
    bool nullAllowed = false) {
  return std::make_unique<common::FloatRange>(
      min,
      false,
      true,
      std::numeric_limits<float>::max(),
      true,
      true,
      nullAllowed);
}

inline std::unique_ptr<common::FloatRange> greaterThanOrEqualFloat(
    float min,
    bool nullAllowed = false) {
  return std::make_unique<common::FloatRange>(
      min,
      false,
      false,
      std::numeric_limits<float>::max(),
      true,
      true,
      nullAllowed);
}

inline std::unique_ptr<common::FloatRange>
betweenFloat(float min, float max, bool nullAllowed = false) {
  return std::make_unique<common::FloatRange>(
      min, false, false, max, false, false, nullAllowed);
}

inline std::unique_ptr<common::BigintRange>
between(int64_t min, int64_t max, bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(min, max, nullAllowed);
}

inline std::unique_ptr<common::BigintMultiRange> bigintOr(
    std::unique_ptr<common::BigintRange> a,
    std::unique_ptr<common::BigintRange> b,
    bool nullAllowed = false) {
  std::vector<std::unique_ptr<common::BigintRange>> filters;
  filters.emplace_back(std::move(a));
  filters.emplace_back(std::move(b));
  return std::make_unique<common::BigintMultiRange>(
      std::move(filters), nullAllowed);
}

inline std::unique_ptr<common::BigintMultiRange> bigintOr(
    std::unique_ptr<common::BigintRange> a,
    std::unique_ptr<common::BigintRange> b,
    std::unique_ptr<common::BigintRange> c,
    bool nullAllowed = false) {
  std::vector<std::unique_ptr<common::BigintRange>> filters;
  filters.emplace_back(std::move(a));
  filters.emplace_back(std::move(b));
  filters.emplace_back(std::move(c));
  return std::make_unique<common::BigintMultiRange>(
      std::move(filters), nullAllowed);
}

inline std::unique_ptr<common::BytesValues> equal(
    const std::string& value,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesValues>(
      std::vector<std::string>{value}, nullAllowed);
}

inline std::unique_ptr<common::BigintRange> equal(
    int64_t value,
    bool nullAllowed = false) {
  return std::make_unique<common::BigintRange>(value, value, nullAllowed);
}

inline std::unique_ptr<common::BytesRange> between(
    const std::string& min,
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      min, false, false, max, false, false, nullAllowed);
}

inline std::unique_ptr<common::BytesRange> betweenExclusive(
    const std::string& min,
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      min, false, true, max, false, true, nullAllowed);
}

inline std::unique_ptr<common::NegatedBytesRange> notBetween(
    const std::string& min,
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::NegatedBytesRange>(
      min, false, false, max, false, false, nullAllowed);
}

inline std::unique_ptr<common::NegatedBytesRange> notBetweenExclusive(
    const std::string& min,
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::NegatedBytesRange>(
      min, false, true, max, false, true, nullAllowed);
}

inline std::unique_ptr<common::BytesRange> lessThanOrEqual(
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      "", true, false, max, false, false, nullAllowed);
}

inline std::unique_ptr<common::BytesRange> lessThan(
    const std::string& max,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      "", true, false, max, false, true, nullAllowed);
}

inline std::unique_ptr<common::BytesRange> greaterThanOrEqual(
    const std::string& min,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      min, false, false, "", true, false, nullAllowed);
}

inline std::unique_ptr<common::BytesRange> greaterThan(
    const std::string& min,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesRange>(
      min, false, true, "", true, false, nullAllowed);
}

inline std::unique_ptr<common::Filter> in(
    const std::vector<int64_t>& values,
    bool nullAllowed = false) {
  return common::createBigintValues(values, nullAllowed);
}

inline std::unique_ptr<common::Filter> notIn(
    const std::vector<int64_t>& values,
    bool nullAllowed = false) {
  return common::createNegatedBigintValues(values, nullAllowed);
}

inline std::unique_ptr<common::BytesValues> in(
    const std::vector<std::string>& values,
    bool nullAllowed = false) {
  return std::make_unique<common::BytesValues>(values, nullAllowed);
}

inline std::unique_ptr<common::NegatedBytesValues> notIn(
    const std::vector<std::string>& values,
    bool nullAllowed = false) {
  return std::make_unique<common::NegatedBytesValues>(values, nullAllowed);
}

inline std::unique_ptr<common::BoolValue> boolEqual(
    bool value,
    bool nullAllowed = false) {
  return std::make_unique<common::BoolValue>(value, nullAllowed);
}

inline std::unique_ptr<common::IsNull> isNull() {
  return std::make_unique<common::IsNull>();
}

inline std::unique_ptr<common::IsNotNull> isNotNull() {
  return std::make_unique<common::IsNotNull>();
}

template <typename T>
std::unique_ptr<common::MultiRange> orFilter(
    std::unique_ptr<T> a,
    std::unique_ptr<T> b,
    bool nullAllowed = false,
    bool nanAllowed = false) {
  std::vector<std::unique_ptr<common::Filter>> filters;
  filters.emplace_back(std::move(a));
  filters.emplace_back(std::move(b));
  return std::make_unique<common::MultiRange>(
      std::move(filters), nullAllowed, nanAllowed);
}

std::pair<common::Subfield, std::unique_ptr<common::Filter>> toSubfieldFilter(
    const core::TypedExprPtr& expr);
} // namespace facebook::velox::exec
