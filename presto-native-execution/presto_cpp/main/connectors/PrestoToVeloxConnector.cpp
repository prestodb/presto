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

#include "presto_cpp/main/connectors/PrestoToVeloxConnector.h"
#include "presto_cpp/main/connectors/PrestoToVeloxConnectorUtils.h"
#include "presto_cpp/main/types/PrestoToVeloxExpr.h"
#include "presto_cpp/main/types/TypeParser.h"
#include "presto_cpp/presto_protocol/connector/hive/HiveConnectorProtocol.h"
#include "presto_cpp/presto_protocol/connector/iceberg/presto_protocol_iceberg.h"
#include "presto_cpp/presto_protocol/connector/tpcds/TpcdsConnectorProtocol.h"
#include "presto_cpp/presto_protocol/connector/tpch/TpchConnectorProtocol.h"

#include <velox/type/fbhive/HiveTypeParser.h>
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "velox/connectors/tpcds/TpcdsConnector.h"
#include "velox/connectors/tpcds/TpcdsConnectorSplit.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/type/Filter.h"

namespace facebook::presto {
using namespace velox;

namespace {
std::unordered_map<std::string, std::unique_ptr<const PrestoToVeloxConnector>>&
connectors() {
  static std::
      unordered_map<std::string, std::unique_ptr<const PrestoToVeloxConnector>>
          connectors;
  return connectors;
}
} // namespace

void registerPrestoToVeloxConnector(
    std::unique_ptr<const PrestoToVeloxConnector> connector) {
  auto connectorName = connector->connectorName();
  auto connectorProtocol = connector->createConnectorProtocol();
  VELOX_CHECK(
      connectors().insert({connectorName, std::move(connector)}).second,
      "Connector {} is already registered",
      connectorName);
  protocol::registerConnectorProtocol(
      connectorName, std::move(connectorProtocol));
}

void unregisterPrestoToVeloxConnector(const std::string& connectorName) {
  connectors().erase(connectorName);
  protocol::unregisterConnectorProtocol(connectorName);
}

const PrestoToVeloxConnector& getPrestoToVeloxConnector(
    const std::string& connectorName) {
  auto it = connectors().find(connectorName);
  VELOX_CHECK(
      it != connectors().end(), "Connector {} not registered", connectorName);
  return *(it->second);
}

std::unique_ptr<velox::connector::ConnectorSplit>
TpcdsPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* connectorSplit,
    const protocol::SplitContext* splitContext) const {
  auto tpcdsSplit =
      dynamic_cast<const protocol::tpcds::TpcdsSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      tpcdsSplit, "Unexpected split type {}", connectorSplit->_type);
  return std::make_unique<connector::tpcds::TpcdsConnectorSplit>(
      catalogId,
      splitContext->cacheable,
      tpcdsSplit->totalParts,
      tpcdsSplit->partNumber);
}

std::unique_ptr<velox::connector::ColumnHandle>
TpcdsPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto tpcdsColumn =
      dynamic_cast<const protocol::tpcds::TpcdsColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      tpcdsColumn, "Unexpected column handle type {}", column->_type);
  return std::make_unique<connector::tpcds::TpcdsColumnHandle>(
      tpcdsColumn->columnName);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
TpcdsPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) const {
  auto tpcdsLayout =
      std::dynamic_pointer_cast<const protocol::tpcds::TpcdsTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      tpcdsLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);
  return std::make_unique<connector::tpcds::TpcdsTableHandle>(
      tableHandle.connectorId,
      tpcds::fromTableName(tpcdsLayout->table.tableName),
      tpcdsLayout->table.scaleFactor);
}

std::unique_ptr<protocol::ConnectorProtocol>
TpcdsPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::tpcds::TpcdsConnectorProtocol>();
}
namespace {
using namespace velox;

template <typename T>
std::string toJsonString(const T& value) {
  return ((json)value).dump();
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

std::vector<common::Subfield> toRequiredSubfields(
    const protocol::List<protocol::Subfield>& subfields) {
  std::vector<common::Subfield> result;
  result.reserve(subfields.size());
  for (auto& subfield : subfields) {
    result.emplace_back(subfield);
  }
  return result;
}

template <TypeKind KIND>
TypePtr fieldNamesToLowerCase(const TypePtr& type) {
  return type;
}

template <>
TypePtr fieldNamesToLowerCase<TypeKind::ARRAY>(const TypePtr& type);

template <>
TypePtr fieldNamesToLowerCase<TypeKind::MAP>(const TypePtr& type);

template <>
TypePtr fieldNamesToLowerCase<TypeKind::ROW>(const TypePtr& type);

template <>
TypePtr fieldNamesToLowerCase<TypeKind::ARRAY>(const TypePtr& type) {
  auto& elementType = type->childAt(0);
  return std::make_shared<ArrayType>(VELOX_DYNAMIC_TYPE_DISPATCH(
      fieldNamesToLowerCase, elementType->kind(), elementType));
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
    bytesGeneric.emplace_back(std::unique_ptr<common::Filter>(
        dynamic_cast<common::Filter*>(bytesFilters[i].release())));
  }

  return std::make_unique<common::MultiRange>(
      std::move(bytesGeneric), nullAllowed, false);
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

std::unique_ptr<connector::ConnectorTableHandle> toHiveTableHandle(
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

    // Use null for always-true filter.
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
      // The type from the metastore may have upper case letters
      // in field names, convert them all to lower case to be
      // compatible with Presto.
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

connector::hive::LocationHandle::TableType toTableType(
    protocol::hive::TableType tableType) {
  switch (tableType) {
    case protocol::hive::TableType::NEW:
    // Temporary tables are written and read by the SPI in a single pipeline.
    // So they can be treated as New. They do not require Append or Overwrite
    // semantics as applicable for regular tables.
    case protocol::hive::TableType::TEMPORARY:
      return connector::hive::LocationHandle::TableType::kNew;
    case protocol::hive::TableType::EXISTING:
      return connector::hive::LocationHandle::TableType::kExisting;
    default:
      VELOX_UNSUPPORTED("Unsupported table type: {}.", toJsonString(tableType));
  }
}

std::shared_ptr<connector::hive::LocationHandle> toLocationHandle(
    const protocol::hive::LocationHandle& locationHandle) {
  return std::make_shared<connector::hive::LocationHandle>(
      locationHandle.targetPath,
      locationHandle.writePath,
      toTableType(locationHandle.tableType));
}

dwio::common::FileFormat toFileFormat(
    const protocol::hive::HiveStorageFormat storageFormat,
    const char* usage) {
  switch (storageFormat) {
    case protocol::hive::HiveStorageFormat::DWRF:
      return dwio::common::FileFormat::DWRF;
    case protocol::hive::HiveStorageFormat::PARQUET:
      return dwio::common::FileFormat::PARQUET;
    case protocol::hive::HiveStorageFormat::ALPHA:
      // This has been renamed in Velox from ALPHA to NIMBLE.
      return dwio::common::FileFormat::NIMBLE;
    case protocol::hive::HiveStorageFormat::TEXTFILE:
      return dwio::common::FileFormat::TEXT;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported file format in {}: {}.",
          usage,
          toJsonString(storageFormat));
  }
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

velox::connector::hive::HiveBucketProperty::Kind toHiveBucketPropertyKind(
    protocol::hive::BucketFunctionType bucketFuncType) {
  switch (bucketFuncType) {
    case protocol::hive::BucketFunctionType::PRESTO_NATIVE:
      return velox::connector::hive::HiveBucketProperty::Kind::kPrestoNative;
    case protocol::hive::BucketFunctionType::HIVE_COMPATIBLE:
      return velox::connector::hive::HiveBucketProperty::Kind::kHiveCompatible;
    default:
      VELOX_USER_FAIL(
          "Unknown hive bucket function: {}", toJsonString(bucketFuncType));
  }
}

std::vector<TypePtr> stringToTypes(
    const std::shared_ptr<protocol::List<protocol::Type>>& typeStrings,
    const TypeParser& typeParser) {
  std::vector<TypePtr> types;
  types.reserve(typeStrings->size());
  for (const auto& typeString : *typeStrings) {
    types.push_back(stringToType(typeString, typeParser));
  }
  return types;
}

core::SortOrder toSortOrder(protocol::hive::Order order) {
  switch (order) {
    case protocol::hive::Order::ASCENDING:
      return core::SortOrder(true, true);
    case protocol::hive::Order::DESCENDING:
      return core::SortOrder(false, false);
    default:
      VELOX_USER_FAIL("Unknown sort order: {}", toJsonString(order));
  }
}

std::shared_ptr<velox::connector::hive::HiveSortingColumn> toHiveSortingColumn(
    const protocol::hive::SortingColumn& sortingColumn) {
  return std::make_shared<velox::connector::hive::HiveSortingColumn>(
      sortingColumn.columnName, toSortOrder(sortingColumn.order));
}

std::vector<std::shared_ptr<const velox::connector::hive::HiveSortingColumn>>
toHiveSortingColumns(
    const protocol::List<protocol::hive::SortingColumn>& sortedBy) {
  std::vector<std::shared_ptr<const velox::connector::hive::HiveSortingColumn>>
      sortingColumns;
  sortingColumns.reserve(sortedBy.size());
  for (const auto& sortingColumn : sortedBy) {
    sortingColumns.push_back(toHiveSortingColumn(sortingColumn));
  }
  return sortingColumns;
}

std::shared_ptr<velox::connector::hive::HiveBucketProperty>
toHiveBucketProperty(
    const std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>>&
        inputColumns,
    const std::shared_ptr<protocol::hive::HiveBucketProperty>& bucketProperty,
    const TypeParser& typeParser) {
  if (bucketProperty == nullptr) {
    return nullptr;
  }

  VELOX_USER_CHECK_GT(
      bucketProperty->bucketCount, 0, "Bucket count must be a positive value");

  VELOX_USER_CHECK(
      !bucketProperty->bucketedBy.empty(),
      "Bucketed columns must be set: {}",
      toJsonString(*bucketProperty));

  const velox::connector::hive::HiveBucketProperty::Kind kind =
      toHiveBucketPropertyKind(bucketProperty->bucketFunctionType);
  std::vector<TypePtr> bucketedTypes;
  if (kind ==
      velox::connector::hive::HiveBucketProperty::Kind::kHiveCompatible) {
    VELOX_USER_CHECK_NULL(
        bucketProperty->types,
        "Unexpected bucketed types set for hive compatible bucket function: {}",
        toJsonString(*bucketProperty));
    bucketedTypes.reserve(bucketProperty->bucketedBy.size());
    for (const auto& bucketedColumn : bucketProperty->bucketedBy) {
      TypePtr bucketedType{nullptr};
      for (const auto& inputColumn : inputColumns) {
        if (inputColumn->name() != bucketedColumn) {
          continue;
        }
        VELOX_USER_CHECK_NOT_NULL(inputColumn->hiveType());
        bucketedType = inputColumn->hiveType();
        break;
      }
      VELOX_USER_CHECK_NOT_NULL(
          bucketedType, "Bucketed column {} not found", bucketedColumn);
      bucketedTypes.push_back(std::move(bucketedType));
    }
  } else {
    VELOX_USER_CHECK_EQ(
        bucketProperty->types->size(),
        bucketProperty->bucketedBy.size(),
        "Bucketed types is not set properly for presto native bucket function: {}",
        toJsonString(*bucketProperty));
    bucketedTypes = stringToTypes(bucketProperty->types, typeParser);
  }

  const auto sortedBy = toHiveSortingColumns(bucketProperty->sortedBy);

  return std::make_shared<velox::connector::hive::HiveBucketProperty>(
      toHiveBucketPropertyKind(bucketProperty->bucketFunctionType),
      bucketProperty->bucketCount,
      bucketProperty->bucketedBy,
      bucketedTypes,
      sortedBy);
}

} // namespace

// Export helper functions for Hive and Iceberg connectors
dwio::common::FileFormat toVeloxFileFormat(
    const presto::protocol::hive::StorageFormat& format) {
  if (format.inputFormat == "com.facebook.hive.orc.OrcInputFormat") {
    return dwio::common::FileFormat::DWRF;
  } else if (
      format.inputFormat == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat") {
    return dwio::common::FileFormat::ORC;
  } else if (
      format.inputFormat ==
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat") {
    return dwio::common::FileFormat::PARQUET;
  } else if (format.inputFormat == "org.apache.hadoop.mapred.TextInputFormat") {
    if (format.serDe == "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe") {
      return dwio::common::FileFormat::TEXT;
    } else if (format.serDe == "org.apache.hive.hcatalog.data.JsonSerDe") {
      return dwio::common::FileFormat::JSON;
    }
  } else if (
      format.inputFormat ==
      "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat") {
    if (format.serDe ==
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe") {
      return dwio::common::FileFormat::PARQUET;
    }
  } else if (format.inputFormat == "com.facebook.alpha.AlphaInputFormat") {
    // ALPHA has been renamed in Velox to NIMBLE.
    return dwio::common::FileFormat::NIMBLE;
  }
  VELOX_UNSUPPORTED(
      "Unsupported file format: {} {}", format.inputFormat, format.serDe);
}

dwio::common::FileFormat toVeloxFileFormat(
    const presto::protocol::iceberg::FileFormat format) {
  if (format == protocol::iceberg::FileFormat::ORC) {
    return dwio::common::FileFormat::ORC;
  } else if (format == protocol::iceberg::FileFormat::PARQUET) {
    return dwio::common::FileFormat::PARQUET;
  }
  VELOX_UNSUPPORTED("Unsupported file format: {}", fmt::underlying(format));
}

std::unique_ptr<velox::connector::hive::HiveColumnHandle>
toVeloxHiveColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) {
  auto* hiveColumn =
      dynamic_cast<const protocol::hive::HiveColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      hiveColumn, "Unexpected column handle type {}", column->_type);
  velox::type::fbhive::HiveTypeParser hiveTypeParser;

  // Use inline conversion to avoid copying issues with unique_ptrs
  velox::connector::hive::HiveColumnHandle::ColumnType columnType;
  switch (hiveColumn->columnType) {
    case protocol::hive::ColumnType::PARTITION_KEY:
      columnType = velox::connector::hive::HiveColumnHandle::ColumnType::kPartitionKey;
      break;
    case protocol::hive::ColumnType::REGULAR:
      columnType = velox::connector::hive::HiveColumnHandle::ColumnType::kRegular;
      break;
    case protocol::hive::ColumnType::SYNTHESIZED:
      columnType = velox::connector::hive::HiveColumnHandle::ColumnType::kSynthesized;
      break;
    default:
      VELOX_UNSUPPORTED("Unsupported Hive column type");
  }

  std::vector<velox::common::Subfield> requiredSubfields;
  requiredSubfields.reserve(hiveColumn->requiredSubfields.size());
  for (const auto& subfield : hiveColumn->requiredSubfields) {
    requiredSubfields.emplace_back(subfield);
  }

  return std::make_unique<velox::connector::hive::HiveColumnHandle>(
      hiveColumn->name,
      columnType,
      stringToType(hiveColumn->typeSignature, typeParser),
      hiveTypeParser.parse(hiveColumn->hiveType),
      std::move(requiredSubfields));
}

velox::connector::hive::HiveBucketConversion toVeloxBucketConversion(
    const protocol::hive::BucketConversion& bucketConversion) {
  velox::connector::hive::HiveBucketConversion veloxBucketConversion;
  veloxBucketConversion.tableBucketCount = bucketConversion.tableBucketCount;
  veloxBucketConversion.partitionBucketCount =
      bucketConversion.partitionBucketCount;
  TypeParser typeParser;
  for (const auto& column : bucketConversion.bucketColumnHandles) {
    veloxBucketConversion.bucketColumnHandles.push_back(
        toVeloxHiveColumnHandle(&column, typeParser));
  }
  return veloxBucketConversion;
}

velox::connector::hive::iceberg::FileContent toVeloxFileContent(
    const presto::protocol::iceberg::FileContent content) {
  if (content == protocol::iceberg::FileContent::DATA) {
    return velox::connector::hive::iceberg::FileContent::kData;
  } else if (content == protocol::iceberg::FileContent::POSITION_DELETES) {
    return velox::connector::hive::iceberg::FileContent::kPositionalDeletes;
  }
  VELOX_UNSUPPORTED("Unsupported file content: {}", fmt::underlying(content));
}

} // namespace facebook::presto
