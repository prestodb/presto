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
#include "presto_cpp/main/thrift/JdbcThriftToJson.h"
#include <folly/base64.h>

#include <array>
#include <stdexcept>
#include "presto_cpp/main/thrift/ThriftIO.h"
#include "presto_cpp/main/thrift/gen-cpp2/presto_jdbc_types.h"

namespace facebook::presto::protocol::arrow_federation {
namespace {
using nlohmann::json;

json toJson(const thrift::jdbc::PrestoType& value) {
  return *value.signature();
}

json toJson(const thrift::jdbc::Block& value) {
  return folly::base64Encode(*value.data());
}

json toJson(const std::string& value) {
  return value;
}

json toJson(const thrift::jdbc::JdbcExpression& value);

template <typename T>
json optionalJson(const T& field) {
  return field.has_value() ? toJson(*field) : json(nullptr);
}

template <typename T>
json optionalJson(const apache::thrift::field_ref<T>& field) {
  return apache::thrift::is_non_optional_field_set_manually_or_by_serializer(
             field)
      ? toJson(*field)
      : json(nullptr);
}

json toJson(const thrift::jdbc::JdbcTypeHandle& value) {
  return {
      {"jdbcType", *value.jdbcType()},
      {"jdbcTypeName", *value.jdbcTypeName()},
      {"columnSize", *value.columnSize()},
      {"decimalDigits", *value.decimalDigits()}};
}

json toJson(const thrift::jdbc::JdbcColumnHandle& value) {
  return {
      {"connectorId", *value.connectorId()},
      {"columnName", *value.columnName()},
      {"jdbcTypeHandle", toJson(*value.jdbcTypeHandle())},
      {"columnType", toJson(*value.columnType())},
      {"nullable", *value.nullable()},
      {"comment", optionalJson(value.comment())}};
}

json toJson(const thrift::jdbc::Marker& value) {
  static const std::array<std::string, 3> bounds = {
      "BELOW", "EXACTLY", "ABOVE"};
  return {
      {"type", toJson(*value.type())},
      {"valueBlock", optionalJson(value.valueBlock())},
      {"bound", bounds.at(static_cast<size_t>(*value.bound()))}};
}

json toJson(const thrift::jdbc::Range& value) {
  return {{"low", toJson(*value.low())}, {"high", toJson(*value.high())}};
}

json toJson(const thrift::jdbc::ValueEntry& value) {
  return {{"type", toJson(*value.type())}, {"block", toJson(*value.block())}};
}

template <typename T>
json toJsonArray(const T& values) {
  auto result = json::array();
  for (const auto& value : values) {
    result.push_back(toJson(value));
  }
  return result;
}

json toJson(const thrift::jdbc::ThriftValueSet& value) {
  switch (value.getType()) {
    case thrift::jdbc::ThriftValueSet::Type::equatableValueSet: {
      const auto& set = value.get_equatableValueSet();
      return {
          {"@type", "equatable"},
          {"type", toJson(*set.type())},
          {"whiteList", *set.whiteList()},
          {"entries", toJsonArray(*set.entries())}};
    }
    case thrift::jdbc::ThriftValueSet::Type::sortedRangeSet: {
      const auto& set = value.get_sortedRangeSet();
      return {
          {"@type", "sortable"},
          {"type", toJson(*set.type())},
          {"ranges", toJsonArray(*set.ranges())}};
    }
    case thrift::jdbc::ThriftValueSet::Type::allOrNoneValueSet: {
      const auto& set = value.get_allOrNoneValueSet();
      return {
          {"@type", "allOrNone"},
          {"type", toJson(*set.type())},
          {"all", *set.all()}};
    }
    default:
      throw std::invalid_argument("Empty JDBC value set");
  }
}

json toJson(const thrift::jdbc::Domain& value) {
  return {
      {"values", toJson(*value.values())},
      {"nullAllowed", *value.nullAllowed()}};
}

json toJson(const thrift::jdbc::JdbcThriftTupleDomain& value) {
  json domains = nullptr;
  if (value.columnDomains().has_value()) {
    domains = json::array();
    for (const auto& entry : *value.columnDomains()) {
      domains.push_back(
          {{"column", toJson(*entry.column())},
           {"domain", toJson(*entry.domain())}});
    }
  }
  return {{"columnDomains", domains}};
}

json toJson(const thrift::jdbc::JdbcExpression& value) {
  auto constants = json::array();
  for (const auto& constant : *value.boundConstantValues()) {
    constants.push_back(
        {{"@type", "constant"},
         {"valueBlock", toJson(*constant.valueBlock())},
         {"type", toJson(*constant.type())}});
  }
  return {
      {"translatedString", *value.expression()},
      {"boundConstantValues", constants}};
}

json toJson(const thrift::jdbc::JdbcTableHandle& value) {
  return {
      {"connectorId", *value.connectorId()},
      {"schemaTableName",
       {{"schema", *value.schemaTableName()->schema()},
        {"table", *value.schemaTableName()->table()}}},
      {"catalogName", optionalJson(value.catalogName())},
      {"schemaName", optionalJson(value.schemaName())},
      {"tableName", *value.tableName()}};
}

json toJson(const thrift::jdbc::JdbcTableLayoutHandle& value) {
  return {
      {"table", toJson(*value.table())},
      {"tupleDomain", toJson(*value.tupleDomain())},
      {"additionalPredicate", optionalJson(value.additionalPredicate())},
      {"layoutString", *value.layoutString()}};
}

json toJson(const thrift::jdbc::JdbcSplit& value) {
  return {
      {"connectorId", *value.connectorId()},
      {"catalogName", optionalJson(value.catalogName())},
      {"schemaName", optionalJson(value.schemaName())},
      {"tableName", *value.tableName()},
      {"tupleDomain", toJson(*value.tupleDomain())},
      {"additionalProperty", optionalJson(value.additionalPredicate())}};
}

json toJson(const thrift::jdbc::JdbcTransactionHandle& value) {
  const auto& bytes = *value.uuid();
  if (bytes.size() != 16) {
    throw std::invalid_argument("JDBC transaction UUID must contain 16 bytes");
  }
  const char* hex = "0123456789abcdef";
  std::string uuid;
  for (size_t i = 0; i < bytes.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) {
      uuid += '-';
    }
    const auto byte = static_cast<unsigned char>(bytes[i]);
    uuid += hex[byte >> 4];
    uuid += hex[byte & 15];
  }
  return {{"@type", "arrow-federation"}, {"uuid", uuid}};
}

void removeNullFields(json& value) {
  if (value.is_object()) {
    for (auto it = value.begin(); it != value.end();) {
      if (it->is_null()) {
        it = value.erase(it);
      } else {
        removeNullFields(*it);
        ++it;
      }
    }
  } else if (value.is_array()) {
    for (auto& element : value) {
      removeNullFields(element);
    }
  }
}

template <typename T>
json decode(const std::string& data) {
  auto value = std::make_shared<T>();
  thriftRead(data, value);
  auto result = toJson(*value);
  result["@type"] = "arrow-federation";
  removeNullFields(result);
  return result;
}

}

json jdbcTableHandleFromThrift(const std::string& data) {
  return decode<thrift::jdbc::JdbcTableHandle>(data);
}

json jdbcTableLayoutHandleFromThrift(const std::string& data) {
  return decode<thrift::jdbc::JdbcTableLayoutHandle>(data);
}

json jdbcColumnHandleFromThrift(const std::string& data) {
  return decode<thrift::jdbc::JdbcColumnHandle>(data);
}

json jdbcSplitFromThrift(const std::string& data) {
  return decode<thrift::jdbc::JdbcSplit>(data);
}

json jdbcTransactionHandleFromThrift(const std::string& data) {
  return decode<thrift::jdbc::JdbcTransactionHandle>(data);
}

}
