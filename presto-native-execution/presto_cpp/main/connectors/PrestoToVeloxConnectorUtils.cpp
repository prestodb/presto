/*
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

#include "presto_cpp/main/connectors/PrestoToVeloxConnectorUtils.h"

#include <folly/String.h>
#include "presto_cpp/main/types/TypeParser.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/type/fbhive/HiveTypeParser.h"

namespace facebook::presto {

using namespace facebook::velox;

namespace {

int64_t toInt64(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto value = exprConverter.getConstantValue(type, *block);
  return VariantConverter::convert<velox::TypeKind::BIGINT>(value)
      .value<int64_t>();
}

int128_t toInt128(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto value = exprConverter.getConstantValue(type, *block);
  return value.value<velox::TypeKind::HUGEINT>();
}

Timestamp toTimestamp(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  const auto value = exprConverter.getConstantValue(type, *block);
  return value.value<velox::TypeKind::TIMESTAMP>();
}

int64_t dateToInt64(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto value = exprConverter.getConstantValue(type, *block);
  return value.value<int32_t>();
}

template <typename T>
T toFloatingPoint(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto variant = exprConverter.getConstantValue(type, *block);
  return variant.value<T>();
}

std::string toString(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto value = exprConverter.getConstantValue(type, *block);
  if (type->isVarbinary()) {
    return value.value<TypeKind::VARBINARY>();
  }
  return value.value<std::string>();
}

bool toBoolean(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto variant = exprConverter.getConstantValue(type, *block);
  return variant.value<bool>();
}

std::unique_ptr<common::BigintRange> bigintRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowUnbounded = range.low.valueBlock == nullptr;
  auto low = lowUnbounded ? std::numeric_limits<int64_t>::min()
                          : toInt64(range.low.valueBlock, exprConverter, type);
  if (!lowUnbounded && range.low.bound == protocol::Bound::ABOVE) {
    low++;
  }

  bool highUnbounded = range.high.valueBlock == nullptr;
  auto high = highUnbounded
      ? std::numeric_limits<int64_t>::max()
      : toInt64(range.high.valueBlock, exprConverter, type);
  if (!highUnbounded && range.high.bound == protocol::Bound::BELOW) {
    high--;
  }
  return std::make_unique<common::BigintRange>(low, high, nullAllowed);
}

std::unique_ptr<common::HugeintRange> hugeintRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowUnbounded = range.low.valueBlock == nullptr;
  auto low = lowUnbounded ? std::numeric_limits<int128_t>::min()
                          : toInt128(range.low.valueBlock, exprConverter, type);
  if (!lowUnbounded && range.low.bound == protocol::Bound::ABOVE) {
    low++;
  }

  bool highUnbounded = range.high.valueBlock == nullptr;
  auto high = highUnbounded
      ? std::numeric_limits<int128_t>::max()
      : toInt128(range.high.valueBlock, exprConverter, type);
  if (!highUnbounded && range.high.bound == protocol::Bound::BELOW) {
    high--;
  }
  return std::make_unique<common::HugeintRange>(low, high, nullAllowed);
}

std::unique_ptr<common::TimestampRange> timestampRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  const bool lowUnbounded = range.low.valueBlock == nullptr;
  auto low = lowUnbounded
      ? std::numeric_limits<Timestamp>::min()
      : toTimestamp(range.low.valueBlock, exprConverter, type);
  if (!lowUnbounded && range.low.bound == protocol::Bound::ABOVE) {
    ++low;
  }

  const bool highUnbounded = range.high.valueBlock == nullptr;
  auto high = highUnbounded
      ? std::numeric_limits<Timestamp>::max()
      : toTimestamp(range.high.valueBlock, exprConverter, type);
  if (!highUnbounded && range.high.bound == protocol::Bound::BELOW) {
    --high;
  }
  return std::make_unique<common::TimestampRange>(low, high, nullAllowed);
}

std::unique_ptr<common::Filter> boolRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowExclusive = range.low.bound == protocol::Bound::ABOVE;
  bool lowUnbounded = range.low.valueBlock == nullptr && lowExclusive;
  bool highExclusive = range.high.bound == protocol::Bound::BELOW;
  bool highUnbounded = range.high.valueBlock == nullptr && highExclusive;

  if (!lowUnbounded && !highUnbounded) {
    bool lowValue = toBoolean(range.low.valueBlock, exprConverter, type);
    bool highValue = toBoolean(range.high.valueBlock, exprConverter, type);
    VELOX_CHECK_EQ(
        lowValue,
        highValue,
        "Boolean range should not be [FALSE, TRUE] after coordinator "
        "optimization.");
    return std::make_unique<common::BoolValue>(lowValue, nullAllowed);
  }
  // Presto coordinator has made optimizations to the bool range already. For
  // example, [FALSE, TRUE) will be optimized and shown here as (-infinity,
  // TRUE). Plus (-infinity, +infinity) case has been guarded in toFilter()
  // method, here it can only be one side bounded scenarios.
  VELOX_CHECK_NE(
      lowUnbounded,
      highUnbounded,
      "Passed in boolean range can only have one side bounded range scenario");
  if (!lowUnbounded) {
    VELOX_CHECK(
        highUnbounded,
        "Boolean range should not be double side bounded after coordinator "
        "optimization.");
    bool lowValue = toBoolean(range.low.valueBlock, exprConverter, type);

    // (TRUE, +infinity) case, should resolve to filter all
    if (lowExclusive && lowValue) {
      if (nullAllowed) {
        return std::make_unique<common::IsNull>();
      }
      return std::make_unique<common::AlwaysFalse>();
    }

    // Both cases (FALSE, +infinity) or [TRUE, +infinity) should evaluate to
    // true. Case [FALSE, +infinity) should not be expected
    VELOX_CHECK(
        !(!lowExclusive && !lowValue),
        "Case [FALSE, +infinity) should "
        "not be expected");
    return std::make_unique<common::BoolValue>(true, nullAllowed);
  }
  if (!highUnbounded) {
    VELOX_CHECK(
        lowUnbounded,
        "Boolean range should not be double side bounded after coordinator "
        "optimization.");
    bool highValue = toBoolean(range.high.valueBlock, exprConverter, type);

    // (-infinity, FALSE) case, should resolve to filter all
    if (highExclusive && !highValue) {
      if (nullAllowed) {
        return std::make_unique<common::IsNull>();
      }
      return std::make_unique<common::AlwaysFalse>();
    }

    // Both cases (-infinity, TRUE) or (-infinity, FALSE] should evaluate to
    // false. Case (-infinity, TRUE] should not be expected
    VELOX_CHECK(
        !(!highExclusive && highValue),
        "Case (-infinity, TRUE] should "
        "not be expected");
    return std::make_unique<common::BoolValue>(false, nullAllowed);
  }
  VELOX_UNREACHABLE();
}

template <typename T>
std::unique_ptr<common::Filter> floatingPointRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowExclusive = range.low.bound == protocol::Bound::ABOVE;
  bool lowUnbounded = range.low.valueBlock == nullptr && lowExclusive;
  auto low = lowUnbounded
      ? (-1.0 * std::numeric_limits<T>::infinity())
      : toFloatingPoint<T>(range.low.valueBlock, exprConverter, type);

  bool highExclusive = range.high.bound == protocol::Bound::BELOW;
  bool highUnbounded = range.high.valueBlock == nullptr && highExclusive;
  auto high = highUnbounded
      ? std::numeric_limits<T>::infinity()
      : toFloatingPoint<T>(range.high.valueBlock, exprConverter, type);

  // Handle NaN cases as NaN is not supported as a limit in Velox Filters
  if (!lowUnbounded && std::isnan(low)) {
    if (lowExclusive) {
      // x > NaN is always false as NaN is considered the largest value.
      return std::make_unique<common::AlwaysFalse>();
    }
    // Equivalent to x > infinity as only NaN is greater than infinity
    // Presto currently converts x >= NaN into the filter with domain
    // [NaN, max), so ignoring the high value is fine.
    low = std::numeric_limits<T>::infinity();
    lowExclusive = true;
    high = std::numeric_limits<T>::infinity();
    highUnbounded = true;
    highExclusive = false;
  } else if (!highUnbounded && std::isnan(high)) {
    high = std::numeric_limits<T>::infinity();
    if (highExclusive) {
      // equivalent to x in [low , infinity] or (low , infinity]
      highExclusive = false;
    } else {
      if (lowUnbounded) {
        // Anything <= NaN is true as NaN is the largest possible value.
        return std::make_unique<common::AlwaysTrue>();
      }
      // Equivalent to x > low or x >=low
      highUnbounded = true;
    }
  }

  return std::make_unique<common::FloatingPointRange<T>>(
      low,
      lowUnbounded,
      lowExclusive,
      high,
      highUnbounded,
      highExclusive,
      nullAllowed);
}

std::unique_ptr<common::BytesRange> varcharRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowExclusive = range.low.bound == protocol::Bound::ABOVE;
  bool lowUnbounded = range.low.valueBlock == nullptr && lowExclusive;
  auto low =
      lowUnbounded ? "" : toString(range.low.valueBlock, exprConverter, type);

  bool highExclusive = range.high.bound == protocol::Bound::BELOW;
  bool highUnbounded = range.high.valueBlock == nullptr && highExclusive;
  auto high =
      highUnbounded ? "" : toString(range.high.valueBlock, exprConverter, type);
  return std::make_unique<common::BytesRange>(
      low,
      lowUnbounded,
      lowExclusive,
      high,
      highUnbounded,
      highExclusive,
      nullAllowed);
}

std::unique_ptr<common::BigintRange> dateRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowUnbounded = range.low.valueBlock == nullptr;
  auto low = lowUnbounded
      ? std::numeric_limits<int32_t>::min()
      : dateToInt64(range.low.valueBlock, exprConverter, type);
  if (!lowUnbounded && range.low.bound == protocol::Bound::ABOVE) {
    low++;
  }

  bool highUnbounded = range.high.valueBlock == nullptr;
  auto high = highUnbounded
      ? std::numeric_limits<int32_t>::max()
      : dateToInt64(range.high.valueBlock, exprConverter, type);
  if (!highUnbounded && range.high.bound == protocol::Bound::BELOW) {
    high--;
  }

  return std::make_unique<common::BigintRange>(low, high, nullAllowed);
}

std::unique_ptr<common::Filter> toFilter(
    const TypePtr& type,
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter) {
  if (type->isDate()) {
    return dateRangeToFilter(range, nullAllowed, exprConverter, type);
  }
  switch (type->kind()) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      return bigintRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::HUGEINT:
      return hugeintRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::DOUBLE:
      return floatingPointRangeToFilter<double>(
          range, nullAllowed, exprConverter, type);
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return varcharRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::BOOLEAN:
      return boolRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::REAL:
      return floatingPointRangeToFilter<float>(
          range, nullAllowed, exprConverter, type);
    case TypeKind::TIMESTAMP:
      return timestampRangeToFilter(range, nullAllowed, exprConverter, type);
    default:
      VELOX_UNSUPPORTED("Unsupported range type: {}", type->toString());
  }
}

std::unique_ptr<common::Filter> combineIntegerRanges(
    std::vector<std::unique_ptr<common::BigintRange>>& bigintFilters,
    bool nullAllowed) {
  bool allSingleValue = std::all_of(
      bigintFilters.begin(), bigintFilters.end(), [](const auto& range) {
        return range->isSingleValue();
      });

  if (allSingleValue) {
    std::vector<int64_t> values;
    values.reserve(bigintFilters.size());
    for (const auto& filter : bigintFilters) {
      values.emplace_back(filter->lower());
    }
    return common::createBigintValues(values, nullAllowed);
  }

  if (bigintFilters.size() == 2 &&
      bigintFilters[0]->lower() == std::numeric_limits<int64_t>::min() &&
      bigintFilters[1]->upper() == std::numeric_limits<int64_t>::max()) {
    assert(bigintFilters[0]->upper() + 1 <= bigintFilters[1]->lower() - 1);
    return std::make_unique<common::NegatedBigintRange>(
        bigintFilters[0]->upper() + 1,
        bigintFilters[1]->lower() - 1,
        nullAllowed);
  }

  bool allNegatedValues = true;
  bool foundMaximum = false;
  assert(bigintFilters.size() > 1); // true by size checks on ranges
  std::vector<int64_t> rejectedValues;

  // check if int64 min is a rejected value
  if (bigintFilters[0]->lower() == std::numeric_limits<int64_t>::min() + 1) {
    rejectedValues.emplace_back(std::numeric_limits<int64_t>::min());
  }
  if (bigintFilters[0]->lower() > std::numeric_limits<int64_t>::min() + 1) {
    // too many value at the lower end, bail out
    return std::make_unique<common::BigintMultiRange>(
        std::move(bigintFilters), nullAllowed);
  }
  rejectedValues.push_back(bigintFilters[0]->upper() + 1);
  for (int i = 1; i < bigintFilters.size(); ++i) {
    if (bigintFilters[i]->lower() != bigintFilters[i - 1]->upper() + 2) {
      allNegatedValues = false;
      break;
    }
    if (bigintFilters[i]->upper() == std::numeric_limits<int64_t>::max()) {
      foundMaximum = true;
      break;
    }
    rejectedValues.push_back(bigintFilters[i]->upper() + 1);
    // make sure there is another range possible above this one
    if (bigintFilters[i]->upper() == std::numeric_limits<int64_t>::max() - 1) {
      foundMaximum = true;
      break;
    }
  }

  if (allNegatedValues && foundMaximum) {
    return common::createNegatedBigintValues(rejectedValues, nullAllowed);
  }

  return std::make_unique<common::BigintMultiRange>(
      std::move(bigintFilters), nullAllowed);
}

std::unique_ptr<common::Filter> combineBytesRanges(
    std::vector<std::unique_ptr<common::BytesRange>>& bytesFilters,
    bool nullAllowed) {
  bool allSingleValue = std::all_of(
      bytesFilters.begin(), bytesFilters.end(), [](const auto& range) {
        return range->isSingleValue();
      });

  if (allSingleValue) {
    std::vector<std::string> values;
    values.reserve(bytesFilters.size());
    for (const auto& filter : bytesFilters) {
      values.emplace_back(filter->lower());
    }
    return std::make_unique<common::BytesValues>(values, nullAllowed);
  }

  int lowerUnbounded = 0, upperUnbounded = 0;
  bool allExclusive = std::all_of(
      bytesFilters.begin(), bytesFilters.end(), [](const auto& range) {
        return range->lowerExclusive() && range->upperExclusive();
      });
  if (allExclusive) {
    folly::F14FastSet<std::string> unmatched;
    std::vector<std::string> rejectedValues;
    rejectedValues.reserve(bytesFilters.size());
    for (int i = 0; i < bytesFilters.size(); ++i) {
      if (bytesFilters[i]->isLowerUnbounded()) {
        ++lowerUnbounded;
      } else {
        if (unmatched.contains(bytesFilters[i]->lower())) {
          unmatched.erase(bytesFilters[i]->lower());
          rejectedValues.emplace_back(bytesFilters[i]->lower());
        } else {
          unmatched.insert(bytesFilters[i]->lower());
        }
      }
      if (bytesFilters[i]->isUpperUnbounded()) {
        ++upperUnbounded;
      } else {
        if (unmatched.contains(bytesFilters[i]->upper())) {
          unmatched.erase(bytesFilters[i]->upper());
          rejectedValues.emplace_back(bytesFilters[i]->upper());
        } else {
          unmatched.insert(bytesFilters[i]->upper());
        }
      }
    }

    if (lowerUnbounded == 1 && upperUnbounded == 1 && unmatched.size() == 0) {
      return std::make_unique<common::NegatedBytesValues>(
          rejectedValues, nullAllowed);
    }
  }

  if (bytesFilters.size() == 2 && bytesFilters[0]->isLowerUnbounded() &&
      bytesFilters[1]->isUpperUnbounded()) {
    // create a negated bytes range instead
    return std::make_unique<common::NegatedBytesRange>(
        bytesFilters[0]->upper(),
        false,
        !bytesFilters[0]->upperExclusive(),
        bytesFilters[1]->lower(),
        false,
        !bytesFilters[1]->lowerExclusive(),
        nullAllowed);
  }

  std::vector<std::unique_ptr<common::Filter>> bytesGeneric;
  for (int i = 0; i < bytesFilters.size(); ++i) {
    bytesGeneric.emplace_back(
        std::unique_ptr<common::Filter>(
            dynamic_cast<common::Filter*>(bytesFilters[i].release())));
  }

  return std::make_unique<common::MultiRange>(
      std::move(bytesGeneric), nullAllowed, false);
}

} // namespace

TypePtr stringToType(
    const std::string& typeString,
    const TypeParser& typeParser) {
  return typeParser.parse(typeString);
}

template <>
TypePtr fieldNamesToLowerCase<TypeKind::ARRAY>(const TypePtr& type);

template <>
TypePtr fieldNamesToLowerCase<TypeKind::MAP>(const TypePtr& type);

template <>
TypePtr fieldNamesToLowerCase<TypeKind::ROW>(const TypePtr& type);

template <TypeKind KIND>
TypePtr fieldNamesToLowerCase(const TypePtr& type) {
  return type;
}

template <>
TypePtr fieldNamesToLowerCase<TypeKind::ROW>(const TypePtr& type) {
  auto& rowType = type->asRow();
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(type->size());
  types.reserve(type->size());
  for (int i = 0; i < rowType.size(); i++) {
    std::string name = rowType.nameOf(i);
    folly::toLowerAscii(name);
    names.push_back(std::move(name));
    auto& childType = rowType.childAt(i);
    types.push_back(VELOX_DYNAMIC_TYPE_DISPATCH(
        fieldNamesToLowerCase, childType->kind(), childType));
  }
  return std::make_shared<RowType>(std::move(names), std::move(types));
}

template <>
TypePtr fieldNamesToLowerCase<TypeKind::MAP>(const TypePtr& type) {
  auto& keyType = type->childAt(0);
  auto& valueType = type->childAt(1);
  return std::make_shared<MapType>(
      VELOX_DYNAMIC_TYPE_DISPATCH(
          fieldNamesToLowerCase, keyType->kind(), keyType),
      VELOX_DYNAMIC_TYPE_DISPATCH(
          fieldNamesToLowerCase, valueType->kind(), valueType));
}

template <>
TypePtr fieldNamesToLowerCase<TypeKind::ARRAY>(const TypePtr& type) {
  auto& elementType = type->childAt(0);
  return std::make_shared<ArrayType>(VELOX_DYNAMIC_TYPE_DISPATCH(
      fieldNamesToLowerCase, elementType->kind(), elementType));
}

template TypePtr fieldNamesToLowerCase<TypeKind::BOOLEAN>(const TypePtr&);
template TypePtr fieldNamesToLowerCase<TypeKind::TINYINT>(const TypePtr&);
template TypePtr fieldNamesToLowerCase<TypeKind::SMALLINT>(const TypePtr&);
template TypePtr fieldNamesToLowerCase<TypeKind::INTEGER>(const TypePtr&);
template TypePtr fieldNamesToLowerCase<TypeKind::BIGINT>(const TypePtr&);
template TypePtr fieldNamesToLowerCase<TypeKind::REAL>(const TypePtr&);
template TypePtr fieldNamesToLowerCase<TypeKind::DOUBLE>(const TypePtr&);
template TypePtr fieldNamesToLowerCase<TypeKind::VARCHAR>(const TypePtr&);
template TypePtr fieldNamesToLowerCase<TypeKind::VARBINARY>(const TypePtr&);
template TypePtr fieldNamesToLowerCase<TypeKind::TIMESTAMP>(const TypePtr&);
template TypePtr fieldNamesToLowerCase<TypeKind::HUGEINT>(const TypePtr&);

std::unique_ptr<common::Filter> toFilter(
    const protocol::Domain& domain,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) {
  auto nullAllowed = domain.nullAllowed;
  if (auto sortedRangeSet =
          std::dynamic_pointer_cast<protocol::SortedRangeSet>(domain.values)) {
    auto type = stringToType(sortedRangeSet->type, typeParser);
    auto ranges = sortedRangeSet->ranges;

    if (ranges.empty()) {
      VELOX_CHECK(nullAllowed, "Unexpected always-false filter");
      return std::make_unique<common::IsNull>();
    }

    if (ranges.size() == 1) {
      // 'is not null' arrives as unbounded range with 'nulls not allowed'.
      // We catch this case and create 'is not null' filter instead of the range
      // filter.
      const auto& range = ranges[0];
      bool lowExclusive = range.low.bound == protocol::Bound::ABOVE;
      bool lowUnbounded = range.low.valueBlock == nullptr && lowExclusive;
      bool highExclusive = range.high.bound == protocol::Bound::BELOW;
      bool highUnbounded = range.high.valueBlock == nullptr && highExclusive;
      if (lowUnbounded && highUnbounded && !nullAllowed) {
        return std::make_unique<common::IsNotNull>();
      }

      return toFilter(type, ranges[0], nullAllowed, exprConverter);
    }

    if (type->isDate()) {
      std::vector<std::unique_ptr<common::BigintRange>> dateFilters;
      dateFilters.reserve(ranges.size());
      for (const auto& range : ranges) {
        dateFilters.emplace_back(
            dateRangeToFilter(range, nullAllowed, exprConverter, type));
      }
      return std::make_unique<common::BigintMultiRange>(
          std::move(dateFilters), nullAllowed);
    }

    if (type->kind() == TypeKind::BIGINT || type->kind() == TypeKind::INTEGER ||
        type->kind() == TypeKind::SMALLINT ||
        type->kind() == TypeKind::TINYINT) {
      std::vector<std::unique_ptr<common::BigintRange>> bigintFilters;
      bigintFilters.reserve(ranges.size());
      for (const auto& range : ranges) {
        bigintFilters.emplace_back(
            bigintRangeToFilter(range, nullAllowed, exprConverter, type));
      }
      return combineIntegerRanges(bigintFilters, nullAllowed);
    }

    if (type->kind() == TypeKind::VARCHAR) {
      std::vector<std::unique_ptr<common::BytesRange>> bytesFilters;
      bytesFilters.reserve(ranges.size());
      for (const auto& range : ranges) {
        bytesFilters.emplace_back(
            varcharRangeToFilter(range, nullAllowed, exprConverter, type));
      }
      return combineBytesRanges(bytesFilters, nullAllowed);
    }

    if (type->kind() == TypeKind::BOOLEAN) {
      VELOX_CHECK_EQ(ranges.size(), 2, "Multi bool ranges size can only be 2.");
      std::unique_ptr<common::Filter> boolFilter;
      for (const auto& range : ranges) {
        auto filter =
            boolRangeToFilter(range, nullAllowed, exprConverter, type);
        if (filter->kind() == common::FilterKind::kAlwaysFalse or
            filter->kind() == common::FilterKind::kIsNull) {
          continue;
        }
        VELOX_CHECK_NULL(boolFilter);
        boolFilter = std::move(filter);
      }

      VELOX_CHECK_NOT_NULL(boolFilter);
      return boolFilter;
    }

    std::vector<std::unique_ptr<common::Filter>> filters;
    filters.reserve(ranges.size());
    for (const auto& range : ranges) {
      filters.emplace_back(toFilter(type, range, nullAllowed, exprConverter));
    }

    return std::make_unique<common::MultiRange>(
        std::move(filters), nullAllowed, false);
  } else if (
      auto equatableValueSet =
          std::dynamic_pointer_cast<protocol::EquatableValueSet>(
              domain.values)) {
    if (equatableValueSet->entries.empty()) {
      if (nullAllowed) {
        return std::make_unique<common::IsNull>();
      } else {
        return std::make_unique<common::IsNotNull>();
      }
    }
    VELOX_UNSUPPORTED(
        "EquatableValueSet (with non-empty entries) to Velox filter conversion is not supported yet.");
  } else if (
      auto allOrNoneValueSet =
          std::dynamic_pointer_cast<protocol::AllOrNoneValueSet>(
              domain.values)) {
    VELOX_UNSUPPORTED(
        "AllOrNoneValueSet to Velox filter conversion is not supported yet.");
  }
  VELOX_UNSUPPORTED("Unsupported filter found.");
}

std::vector<common::Subfield> toRequiredSubfields(
    const protocol::List<protocol::Subfield>& subfields) {
  std::vector<common::Subfield> result;
  result.reserve(subfields.size());
  for (auto& subfield : subfields) {
    result.emplace_back(subfield);
  }
  return result;
}

velox::common::CompressionKind toFileCompressionKind(
    const protocol::hive::HiveCompressionCodec& hiveCompressionCodec) {
  switch (hiveCompressionCodec) {
    case protocol::hive::HiveCompressionCodec::SNAPPY:
      return velox::common::CompressionKind::CompressionKind_SNAPPY;
    case protocol::hive::HiveCompressionCodec::GZIP:
      return velox::common::CompressionKind::CompressionKind_GZIP;
    case protocol::hive::HiveCompressionCodec::LZ4:
      return velox::common::CompressionKind::CompressionKind_LZ4;
    case protocol::hive::HiveCompressionCodec::ZSTD:
      return velox::common::CompressionKind::CompressionKind_ZSTD;
    case protocol::hive::HiveCompressionCodec::NONE:
      return velox::common::CompressionKind::CompressionKind_NONE;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported file compression format: {}.",
          toJsonString(hiveCompressionCodec));
  }
}

connector::hive::HiveColumnHandle::ColumnType toHiveColumnType(
    protocol::hive::ColumnType type) {
  switch (type) {
    case protocol::hive::ColumnType::PARTITION_KEY:
      return connector::hive::HiveColumnHandle::ColumnType::kPartitionKey;
    case protocol::hive::ColumnType::REGULAR:
      return connector::hive::HiveColumnHandle::ColumnType::kRegular;
    case protocol::hive::ColumnType::SYNTHESIZED:
      return connector::hive::HiveColumnHandle::ColumnType::kSynthesized;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported Hive column type: {}.", toJsonString(type));
  }
}

std::unique_ptr<velox::connector::ConnectorTableHandle> toHiveTableHandle(
    const protocol::TupleDomain<protocol::Subfield>& domainPredicate,
    const std::shared_ptr<protocol::RowExpression>& remainingPredicate,
    bool isPushdownFilterEnabled,
    const std::string& tableName,
    const protocol::List<protocol::Column>& dataColumns,
    const protocol::TableHandle& tableHandle,
    const protocol::Map<protocol::String, protocol::String>& tableParameters,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) {
  common::SubfieldFilters subfieldFilters;
  auto domains = domainPredicate.domains;
  for (const auto& domain : *domains) {
    auto filter = domain.second;
    subfieldFilters[common::Subfield(domain.first)] =
        toFilter(domain.second, exprConverter, typeParser);
  }

  auto remainingFilter = exprConverter.toVeloxExpr(remainingPredicate);
  if (auto constant = std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
          remainingFilter)) {
    bool value = constant->value().value<bool>();
    VELOX_CHECK(value, "Unexpected always-false remaining predicate");

    remainingFilter = nullptr;
  }

  RowTypePtr finalDataColumns;
  if (!dataColumns.empty()) {
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    velox::type::fbhive::HiveTypeParser hiveTypeParser;
    names.reserve(dataColumns.size());
    types.reserve(dataColumns.size());
    for (auto& column : dataColumns) {
      std::string name = column.name;
      folly::toLowerAscii(name);
      names.emplace_back(std::move(name));
      auto parsedType = hiveTypeParser.parse(column.type);
      types.push_back(VELOX_DYNAMIC_TYPE_DISPATCH(
          fieldNamesToLowerCase, parsedType->kind(), parsedType));
    }
    finalDataColumns = ROW(std::move(names), std::move(types));
  }

  if (tableParameters.empty()) {
    return std::make_unique<connector::hive::HiveTableHandle>(
        tableHandle.connectorId,
        tableName,
        isPushdownFilterEnabled,
        std::move(subfieldFilters),
        remainingFilter,
        finalDataColumns);
  }

  std::unordered_map<std::string, std::string> finalTableParameters = {};
  finalTableParameters.reserve(tableParameters.size());
  for (const auto& [key, value] : tableParameters) {
    finalTableParameters[key] = value;
  }

  return std::make_unique<connector::hive::HiveTableHandle>(
      tableHandle.connectorId,
      tableName,
      isPushdownFilterEnabled,
      std::move(subfieldFilters),
      remainingFilter,
      finalDataColumns,
      finalTableParameters);
}

} // namespace facebook::presto
