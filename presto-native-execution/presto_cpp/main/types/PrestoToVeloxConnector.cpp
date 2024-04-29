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

#include "presto_cpp/main/types/PrestoToVeloxConnector.h"
#include <velox/type/fbhive/HiveTypeParser.h>
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"

namespace facebook::presto {

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

namespace {
using namespace velox;

dwio::common::FileFormat toVeloxFileFormat(
    const presto::protocol::StorageFormat& format) {
  if (format.inputFormat == "com.facebook.hive.orc.OrcInputFormat") {
    return dwio::common::FileFormat::DWRF;
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
  } else if (format.inputFormat == "com.facebook.alpha.AlphaInputFormat") {
    // ALPHA has been renamed in Velox to NIMBLE.
    return dwio::common::FileFormat::NIMBLE;
  }
  VELOX_UNSUPPORTED(
      "Unsupported file format: {} {}", format.inputFormat, format.serDe);
}

dwio::common::FileFormat toVeloxFileFormat(
    const presto::protocol::FileFormat format) {
  if (format == protocol::FileFormat::ORC) {
    return dwio::common::FileFormat::DWRF;
  } else if (format == protocol::FileFormat::PARQUET) {
    return dwio::common::FileFormat::PARQUET;
  }
  VELOX_UNSUPPORTED("Unsupported file format: {}", fmt::underlying(format));
}

template <typename T>
std::string toJsonString(const T& value) {
  return ((json)value).dump();
}

TypePtr stringToType(
    const std::string& typeString,
    const TypeParser& typeParser) {
  return typeParser.parse(typeString);
}

connector::hive::HiveColumnHandle::ColumnType toHiveColumnType(
    protocol::ColumnType type) {
  switch (type) {
    case protocol::ColumnType::PARTITION_KEY:
      return connector::hive::HiveColumnHandle::ColumnType::kPartitionKey;
    case protocol::ColumnType::REGULAR:
      return connector::hive::HiveColumnHandle::ColumnType::kRegular;
    case protocol::ColumnType::SYNTHESIZED:
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

double toDouble(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto variant = exprConverter.getConstantValue(type, *block);
  return variant.value<double>();
}

float toFloat(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto variant = exprConverter.getConstantValue(type, *block);
  return variant.value<float>();
}

std::string toString(
    const std::shared_ptr<protocol::Block>& block,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  auto value = exprConverter.getConstantValue(type, *block);
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

std::unique_ptr<common::DoubleRange> doubleRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowExclusive = range.low.bound == protocol::Bound::ABOVE;
  bool lowUnbounded = range.low.valueBlock == nullptr && lowExclusive;
  auto low = lowUnbounded ? std::numeric_limits<double>::lowest()
                          : toDouble(range.low.valueBlock, exprConverter, type);

  bool highExclusive = range.high.bound == protocol::Bound::BELOW;
  bool highUnbounded = range.high.valueBlock == nullptr && highExclusive;
  auto high = highUnbounded
      ? std::numeric_limits<double>::max()
      : toDouble(range.high.valueBlock, exprConverter, type);
  return std::make_unique<common::DoubleRange>(
      low,
      lowUnbounded,
      lowExclusive,
      high,
      highUnbounded,
      highExclusive,
      nullAllowed);
}

std::unique_ptr<common::FloatRange> floatRangeToFilter(
    const protocol::Range& range,
    bool nullAllowed,
    const VeloxExprConverter& exprConverter,
    const TypePtr& type) {
  bool lowExclusive = range.low.bound == protocol::Bound::ABOVE;
  bool lowUnbounded = range.low.valueBlock == nullptr && lowExclusive;
  auto low = lowUnbounded ? std::numeric_limits<float>::lowest()
                          : toFloat(range.low.valueBlock, exprConverter, type);

  bool highExclusive = range.high.bound == protocol::Bound::BELOW;
  bool highUnbounded = range.high.valueBlock == nullptr && highExclusive;
  auto high = highUnbounded
      ? std::numeric_limits<float>::max()
      : toFloat(range.high.valueBlock, exprConverter, type);
  return std::make_unique<common::FloatRange>(
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
      return doubleRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::VARCHAR:
      return varcharRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::BOOLEAN:
      return boolRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::REAL:
      return floatRangeToFilter(range, nullAllowed, exprConverter, type);
    case TypeKind::TIMESTAMP:
      return timestampRangeToFilter(range, nullAllowed, exprConverter, type);
    default:
      VELOX_UNSUPPORTED("Unsupported range type: {}", type->toString());
  }
}

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
  connector::hive::SubfieldFilters subfieldFilters;
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
    protocol::TableType tableType) {
  switch (tableType) {
    case protocol::TableType::NEW:
      return connector::hive::LocationHandle::TableType::kNew;
    case protocol::TableType::EXISTING:
      return connector::hive::LocationHandle::TableType::kExisting;
    default:
      VELOX_UNSUPPORTED("Unsupported table type: {}.", toJsonString(tableType));
  }
}

std::shared_ptr<connector::hive::LocationHandle> toLocationHandle(
    const protocol::LocationHandle& locationHandle) {
  return std::make_shared<connector::hive::LocationHandle>(
      locationHandle.targetPath,
      locationHandle.writePath,
      toTableType(locationHandle.tableType));
}

dwio::common::FileFormat toFileFormat(
    const protocol::HiveStorageFormat storageFormat,
    const char* usage) {
  switch (storageFormat) {
    case protocol::HiveStorageFormat::DWRF:
      return dwio::common::FileFormat::DWRF;
    case protocol::HiveStorageFormat::PARQUET:
      return dwio::common::FileFormat::PARQUET;
    case protocol::HiveStorageFormat::ALPHA:
      // This has been renamed in Velox from ALPHA to NIMBLE.
      return dwio::common::FileFormat::NIMBLE;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported file format in {}: {}.",
          usage,
          toJsonString(storageFormat));
  }
}

velox::common::CompressionKind toFileCompressionKind(
    const protocol::HiveCompressionCodec& hiveCompressionCodec) {
  switch (hiveCompressionCodec) {
    case protocol::HiveCompressionCodec::SNAPPY:
      return velox::common::CompressionKind::CompressionKind_SNAPPY;
    case protocol::HiveCompressionCodec::GZIP:
      return velox::common::CompressionKind::CompressionKind_GZIP;
    case protocol::HiveCompressionCodec::LZ4:
      return velox::common::CompressionKind::CompressionKind_LZ4;
    case protocol::HiveCompressionCodec::ZSTD:
      return velox::common::CompressionKind::CompressionKind_ZSTD;
    case protocol::HiveCompressionCodec::NONE:
      return velox::common::CompressionKind::CompressionKind_NONE;
    default:
      VELOX_UNSUPPORTED(
          "Unsupported file compression format: {}.",
          toJsonString(hiveCompressionCodec));
  }
}

velox::connector::hive::HiveBucketProperty::Kind toHiveBucketPropertyKind(
    protocol::BucketFunctionType bucketFuncType) {
  switch (bucketFuncType) {
    case protocol::BucketFunctionType::PRESTO_NATIVE:
      return velox::connector::hive::HiveBucketProperty::Kind::kPrestoNative;
    case protocol::BucketFunctionType::HIVE_COMPATIBLE:
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

core::SortOrder toSortOrder(protocol::Order order) {
  switch (order) {
    case protocol::Order::ASCENDING:
      return core::SortOrder(true, true);
    case protocol::Order::DESCENDING:
      return core::SortOrder(false, false);
    default:
      VELOX_USER_FAIL("Unknown sort order: {}", toJsonString(order));
  }
}

std::shared_ptr<velox::connector::hive::HiveSortingColumn> toHiveSortingColumn(
    const protocol::SortingColumn& sortingColumn) {
  return std::make_shared<velox::connector::hive::HiveSortingColumn>(
      sortingColumn.columnName, toSortOrder(sortingColumn.order));
}

std::vector<std::shared_ptr<const velox::connector::hive::HiveSortingColumn>>
toHiveSortingColumns(const protocol::List<protocol::SortingColumn>& sortedBy) {
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
    const std::shared_ptr<protocol::HiveBucketProperty>& bucketProperty,
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

velox::connector::hive::iceberg::FileContent toVeloxFileContent(
    const presto::protocol::FileContent content) {
  if (content == protocol::FileContent::DATA) {
    return velox::connector::hive::iceberg::FileContent::kData;
  } else if (content == protocol::FileContent::POSITION_DELETES) {
    return velox::connector::hive::iceberg::FileContent::kPositionalDeletes;
  }
  VELOX_UNSUPPORTED("Unsupported file content: {}", fmt::underlying(content));
}

} // namespace

std::unique_ptr<velox::connector::ConnectorSplit>
HivePrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* const connectorSplit) const {
  auto hiveSplit = dynamic_cast<const protocol::HiveSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      hiveSplit, "Unexpected split type {}", connectorSplit->_type);
  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  for (const auto& entry : hiveSplit->partitionKeys) {
    partitionKeys.emplace(
        entry.name,
        entry.value == nullptr ? std::nullopt
                               : std::optional<std::string>{*entry.value});
  }
  std::unordered_map<std::string, std::string> customSplitInfo;
  for (const auto& [key, value] : hiveSplit->fileSplit.customSplitInfo) {
    customSplitInfo[key] = value;
  }
  std::shared_ptr<std::string> extraFileInfo;
  if (hiveSplit->fileSplit.extraFileInfo) {
    extraFileInfo = std::make_shared<std::string>(
        velox::encoding::Base64::decode(*hiveSplit->fileSplit.extraFileInfo));
  }
  std::unordered_map<std::string, std::string> serdeParameters;
  serdeParameters.reserve(hiveSplit->storage.serdeParameters.size());
  for (const auto& [key, value] : hiveSplit->storage.serdeParameters) {
    serdeParameters[key] = value;
  }
  std::unordered_map<std::string, std::string> infoColumns;
  infoColumns.reserve(2);
  infoColumns.insert(
      {"$file_size", std::to_string(hiveSplit->fileSplit.fileSize)});
  infoColumns.insert(
      {"$file_modified_time",
       std::to_string(hiveSplit->fileSplit.fileModifiedTime)});
  return std::make_unique<velox::connector::hive::HiveConnectorSplit>(
      catalogId,
      hiveSplit->fileSplit.path,
      toVeloxFileFormat(hiveSplit->storage.storageFormat),
      hiveSplit->fileSplit.start,
      hiveSplit->fileSplit.length,
      partitionKeys,
      hiveSplit->tableBucketNumber
          ? std::optional<int>(*hiveSplit->tableBucketNumber)
          : std::nullopt,
      customSplitInfo,
      extraFileInfo,
      serdeParameters,
      hiveSplit->splitWeight,
      infoColumns);
}

std::unique_ptr<velox::connector::ColumnHandle>
HivePrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto hiveColumn = dynamic_cast<const protocol::HiveColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      hiveColumn, "Unexpected column handle type {}", column->_type);
  velox::type::fbhive::HiveTypeParser hiveTypeParser;
  // TODO(spershin): Should we pass something different than 'typeSignature'
  // to 'hiveType' argument of the 'HiveColumnHandle' constructor?
  return std::make_unique<velox::connector::hive::HiveColumnHandle>(
      hiveColumn->name,
      toHiveColumnType(hiveColumn->columnType),
      stringToType(hiveColumn->typeSignature, typeParser),
      hiveTypeParser.parse(hiveColumn->hiveType),
      toRequiredSubfields(hiveColumn->requiredSubfields));
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
HivePrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser,
    std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::ColumnHandle>>& assignments) const {
  auto addSynthesizedColumn = [&](const std::string& name,
                                  protocol::ColumnType columnType,
                                  const protocol::ColumnHandle& column) {
    if (toHiveColumnType(columnType) ==
        velox::connector::hive::HiveColumnHandle::ColumnType::kSynthesized) {
      if (assignments.count(name) == 0) {
        assignments.emplace(name, toVeloxColumnHandle(&column, typeParser));
      }
    }
  };
  auto hiveLayout =
      std::dynamic_pointer_cast<const protocol::HiveTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      hiveLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);
  for (const auto& entry : hiveLayout->partitionColumns) {
    assignments.emplace(entry.name, toVeloxColumnHandle(&entry, typeParser));
  }

  // Add synthesized columns to the TableScanNode columnHandles as well.
  for (const auto& entry : hiveLayout->predicateColumns) {
    addSynthesizedColumn(entry.first, entry.second.columnType, entry.second);
  }

  auto hiveTableHandle =
      std::dynamic_pointer_cast<const protocol::HiveTableHandle>(
          tableHandle.connectorHandle);
  VELOX_CHECK_NOT_NULL(
      hiveTableHandle,
      "Unexpected table handle type {}",
      tableHandle.connectorHandle->_type);

  // Use fully qualified name if available.
  std::string tableName = hiveTableHandle->schemaName.empty()
      ? hiveTableHandle->tableName
      : fmt::format(
            "{}.{}", hiveTableHandle->schemaName, hiveTableHandle->tableName);

  return toHiveTableHandle(
      hiveLayout->domainPredicate,
      hiveLayout->remainingPredicate,
      hiveLayout->pushdownFilterEnabled,
      tableName,
      hiveLayout->dataColumns,
      tableHandle,
      hiveLayout->tableParameters,
      exprConverter,
      typeParser);
}

std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
HivePrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::CreateHandle* createHandle,
    const TypeParser& typeParser) const {
  auto hiveOutputTableHandle =
      std::dynamic_pointer_cast<protocol::HiveOutputTableHandle>(
          createHandle->handle.connectorHandle);
  VELOX_CHECK_NOT_NULL(
      hiveOutputTableHandle,
      "Unexpected output table handle type {}",
      createHandle->handle.connectorHandle->_type);
  bool isPartitioned{false};
  const auto inputColumns = toHiveColumns(
      hiveOutputTableHandle->inputColumns, typeParser, isPartitioned);
  VELOX_USER_CHECK(
      hiveOutputTableHandle->bucketProperty == nullptr || isPartitioned,
      "Bucketed table must be partitioned: {}",
      toJsonString(*hiveOutputTableHandle));
  return std::make_unique<velox::connector::hive::HiveInsertTableHandle>(
      inputColumns,
      toLocationHandle(hiveOutputTableHandle->locationHandle),
      toFileFormat(hiveOutputTableHandle->tableStorageFormat, "TableWrite"),
      toHiveBucketProperty(
          inputColumns, hiveOutputTableHandle->bucketProperty, typeParser),
      std::optional(
          toFileCompressionKind(hiveOutputTableHandle->compressionCodec)));
}

std::unique_ptr<velox::connector::ConnectorInsertTableHandle>
HivePrestoToVeloxConnector::toVeloxInsertTableHandle(
    const protocol::InsertHandle* insertHandle,
    const TypeParser& typeParser) const {
  auto hiveInsertTableHandle =
      std::dynamic_pointer_cast<protocol::HiveInsertTableHandle>(
          insertHandle->handle.connectorHandle);
  VELOX_CHECK_NOT_NULL(
      hiveInsertTableHandle,
      "Unexpected insert table handle type {}",
      insertHandle->handle.connectorHandle->_type);
  bool isPartitioned{false};
  const auto inputColumns = toHiveColumns(
      hiveInsertTableHandle->inputColumns, typeParser, isPartitioned);
  VELOX_USER_CHECK(
      hiveInsertTableHandle->bucketProperty == nullptr || isPartitioned,
      "Bucketed table must be partitioned: {}",
      toJsonString(*hiveInsertTableHandle));

  const auto table = hiveInsertTableHandle->pageSinkMetadata.table;
  VELOX_USER_CHECK_NOT_NULL(table, "Table must not be null for insert query");
  return std::make_unique<connector::hive::HiveInsertTableHandle>(
      inputColumns,
      toLocationHandle(hiveInsertTableHandle->locationHandle),
      toFileFormat(hiveInsertTableHandle->tableStorageFormat, "TableWrite"),
      toHiveBucketProperty(
          inputColumns, hiveInsertTableHandle->bucketProperty, typeParser),
      std::optional(
          toFileCompressionKind(hiveInsertTableHandle->compressionCodec)),
      std::unordered_map<std::string, std::string>(
          table->storage.serdeParameters.begin(),
          table->storage.serdeParameters.end()));
}

std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>>
HivePrestoToVeloxConnector::toHiveColumns(
    const protocol::List<protocol::HiveColumnHandle>& inputColumns,
    const TypeParser& typeParser,
    bool& hasPartitionColumn) const {
  hasPartitionColumn = false;
  std::vector<std::shared_ptr<const connector::hive::HiveColumnHandle>>
      hiveColumns;
  hiveColumns.reserve(inputColumns.size());
  for (const auto& columnHandle : inputColumns) {
    hasPartitionColumn |=
        columnHandle.columnType == protocol::ColumnType::PARTITION_KEY;
    hiveColumns.emplace_back(
        std::dynamic_pointer_cast<connector::hive::HiveColumnHandle>(
            std::shared_ptr(toVeloxColumnHandle(&columnHandle, typeParser))));
  }
  return hiveColumns;
}

std::unique_ptr<velox::core::PartitionFunctionSpec>
HivePrestoToVeloxConnector::createVeloxPartitionFunctionSpec(
    const protocol::ConnectorPartitioningHandle* partitioningHandle,
    const std::vector<int>& bucketToPartition,
    const std::vector<velox::column_index_t>& channels,
    const std::vector<velox::VectorPtr>& constValues,
    bool& effectivelyGather) const {
  auto hivePartitioningHandle =
      dynamic_cast<const protocol::HivePartitioningHandle*>(partitioningHandle);
  VELOX_CHECK_NOT_NULL(
      hivePartitioningHandle,
      "Unexpected partitioning handle type {}",
      partitioningHandle->_type);
  VELOX_USER_CHECK(
      hivePartitioningHandle->bucketFunctionType ==
          protocol::BucketFunctionType::HIVE_COMPATIBLE,
      "Unsupported Hive bucket function type: {}",
      toJsonString(hivePartitioningHandle->bucketFunctionType));
  effectivelyGather = hivePartitioningHandle->bucketCount == 1;
  return std::make_unique<connector::hive::HivePartitionFunctionSpec>(
      hivePartitioningHandle->bucketCount,
      bucketToPartition,
      channels,
      constValues);
}

std::unique_ptr<protocol::ConnectorProtocol>
HivePrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::HiveConnectorProtocol>();
}

std::unique_ptr<velox::connector::ConnectorSplit>
IcebergPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* const connectorSplit) const {
  auto icebergSplit =
      dynamic_cast<const protocol::IcebergSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      icebergSplit, "Unexpected split type {}", connectorSplit->_type);

  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  for (const auto& entry : icebergSplit->partitionKeys) {
    partitionKeys.emplace(
        entry.second.name,
        entry.second.value == nullptr
            ? std::nullopt
            : std::optional<std::string>{*entry.second.value});
  }

  std::unordered_map<std::string, std::string> customSplitInfo;
  customSplitInfo["table_format"] = "hive-iceberg";

  std::vector<velox::connector::hive::iceberg::IcebergDeleteFile> deletes;
  deletes.reserve(icebergSplit->deletes.size());
  for (const auto& deleteFile : icebergSplit->deletes) {
    std::unordered_map<int32_t, std::string> lowerBounds(
        deleteFile.lowerBounds.begin(), deleteFile.lowerBounds.end());

    std::unordered_map<int32_t, std::string> upperBounds(
        deleteFile.upperBounds.begin(), deleteFile.upperBounds.end());

    velox::connector::hive::iceberg::IcebergDeleteFile icebergDeleteFile(
        toVeloxFileContent(deleteFile.content),
        deleteFile.path,
        toVeloxFileFormat(deleteFile.format),
        deleteFile.recordCount,
        deleteFile.fileSizeInBytes,
        std::vector(deleteFile.equalityFieldIds),
        lowerBounds,
        upperBounds);

    deletes.emplace_back(icebergDeleteFile);
  }

  return std::make_unique<connector::hive::iceberg::HiveIcebergSplit>(
      catalogId,
      icebergSplit->path,
      toVeloxFileFormat(icebergSplit->fileFormat),
      icebergSplit->start,
      icebergSplit->length,
      partitionKeys,
      std::nullopt,
      customSplitInfo,
      nullptr,
      deletes);
}

std::unique_ptr<velox::connector::ColumnHandle>
IcebergPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto icebergColumn =
      dynamic_cast<const protocol::IcebergColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      icebergColumn, "Unexpected column handle type {}", column->_type);
  // TODO(imjalpreet): Modify 'hiveType' argument of the 'HiveColumnHandle'
  //  constructor similar to how Hive Connector is handling for bucketing
  velox::type::fbhive::HiveTypeParser hiveTypeParser;
  return std::make_unique<connector::hive::HiveColumnHandle>(
      icebergColumn->columnIdentity.name,
      toHiveColumnType(icebergColumn->columnType),
      stringToType(icebergColumn->type, typeParser),
      stringToType(icebergColumn->type, typeParser),
      toRequiredSubfields(icebergColumn->requiredSubfields));
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
IcebergPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser,
    std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::ColumnHandle>>& assignments) const {
  auto addSynthesizedColumn = [&](const std::string& name,
                                  protocol::ColumnType columnType,
                                  const protocol::ColumnHandle& column) {
    if (toHiveColumnType(columnType) ==
        velox::connector::hive::HiveColumnHandle::ColumnType::kSynthesized) {
      if (assignments.count(name) == 0) {
        assignments.emplace(name, toVeloxColumnHandle(&column, typeParser));
      }
    }
  };

  auto icebergLayout =
      std::dynamic_pointer_cast<const protocol::IcebergTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      icebergLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);

  for (const auto& entry : icebergLayout->partitionColumns) {
    assignments.emplace(
        entry.columnIdentity.name, toVeloxColumnHandle(&entry, typeParser));
  }

  // Add synthesized columns to the TableScanNode columnHandles as well.
  for (const auto& entry : icebergLayout->predicateColumns) {
    addSynthesizedColumn(entry.first, entry.second.columnType, entry.second);
  }

  auto icebergTableHandle =
      std::dynamic_pointer_cast<const protocol::IcebergTableHandle>(
          tableHandle.connectorHandle);
  VELOX_CHECK_NOT_NULL(
      icebergTableHandle,
      "Unexpected table handle type {}",
      tableHandle.connectorHandle->_type);

  // Use fully qualified name if available.
  std::string tableName = icebergTableHandle->schemaName.empty()
      ? icebergTableHandle->icebergTableName.tableName
      : fmt::format(
            "{}.{}",
            icebergTableHandle->schemaName,
            icebergTableHandle->icebergTableName.tableName);

  return toHiveTableHandle(
      icebergLayout->domainPredicate,
      icebergLayout->remainingPredicate,
      icebergLayout->pushdownFilterEnabled,
      tableName,
      icebergLayout->dataColumns,
      tableHandle,
      {},
      exprConverter,
      typeParser);
}

std::unique_ptr<protocol::ConnectorProtocol>
IcebergPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::IcebergConnectorProtocol>();
}

std::unique_ptr<velox::connector::ConnectorSplit>
TpchPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* const connectorSplit) const {
  auto tpchSplit = dynamic_cast<const protocol::TpchSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      tpchSplit, "Unexpected split type {}", connectorSplit->_type);
  return std::make_unique<connector::tpch::TpchConnectorSplit>(
      catalogId, tpchSplit->totalParts, tpchSplit->partNumber);
}

std::unique_ptr<velox::connector::ColumnHandle>
TpchPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  auto tpchColumn = dynamic_cast<const protocol::TpchColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      tpchColumn, "Unexpected column handle type {}", column->_type);
  return std::make_unique<connector::tpch::TpchColumnHandle>(
      tpchColumn->columnName);
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
TpchPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser,
    std::unordered_map<
        std::string,
        std::shared_ptr<velox::connector::ColumnHandle>>& assignments) const {
  auto tpchLayout =
      std::dynamic_pointer_cast<const protocol::TpchTableLayoutHandle>(
          tableHandle.connectorTableLayout);
  VELOX_CHECK_NOT_NULL(
      tpchLayout,
      "Unexpected layout type {}",
      tableHandle.connectorTableLayout->_type);
  return std::make_unique<connector::tpch::TpchTableHandle>(
      tableHandle.connectorId,
      tpch::fromTableName(tpchLayout->table.tableName),
      tpchLayout->table.scaleFactor);
}

std::unique_ptr<protocol::ConnectorProtocol>
TpchPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::TpchConnectorProtocol>();
}
} // namespace facebook::presto