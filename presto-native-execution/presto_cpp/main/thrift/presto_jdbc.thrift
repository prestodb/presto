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

include "thrift/annotation/thrift.thrift"

@thrift.AllowLegacyMissingUris
package;

namespace cpp2 facebook.presto.thrift.jdbc

enum Bound {
  BELOW = 0,
  EXACTLY = 1,
  ABOVE = 2,
}

struct PrestoType {
  1: required string signature;
}

struct Block {
  1: required binary data;
}

struct JdbcTransactionHandle {
  1: binary uuid;
}

struct JdbcOutputTableHandle {
  1: string connectorId;
  2: string catalogName;
  3: string schemaName;
  4: string tableName;
  5: list<string> columnNames;
  6: list<PrestoType> columnTypes;
  7: string temporaryTableName;
}

struct JdbcTypeHandle {
  1: i32 jdbcType;
  2: string jdbcTypeName;
  3: i32 columnSize;
  4: i32 decimalDigits;
}

struct SchemaTableName {
  1: string schema;
  2: string table;
}

struct JdbcThriftTupleDomain {
  1: optional list<JdbcThriftColumnDomain> columnDomains;
}

struct EquatableValueSet {
  1: PrestoType type;
  2: bool whiteList;
  3: set<ValueEntry> entries;
}

struct ValueEntry {
  1: PrestoType type;
  2: Block block;
}

struct SortedRangeSet {
  1: PrestoType type;
  2: list<Range> ranges;
}

struct AllOrNoneValueSet {
  1: PrestoType type;
  2: bool all;
}

struct JdbcExpression {
  1: string expression;
  2: list<ConstantExpression> boundConstantValues;
}

struct ConstantExpression {
  1: Block valueBlock;
  2: PrestoType type;
}

struct JdbcColumnHandle {
  1: string connectorId;
  2: string columnName;
  3: JdbcTypeHandle jdbcTypeHandle;
  4: PrestoType columnType;
  5: bool nullable;
  6: optional string comment;
}

struct JdbcTableHandle {
  1: string connectorId;
  2: SchemaTableName schemaTableName;
  3: string catalogName;
  4: string schemaName;
  5: string tableName;
}

struct JdbcTableLayoutHandle {
  1: JdbcTableHandle table;
  2: JdbcThriftTupleDomain tupleDomain;
  3: optional JdbcExpression additionalPredicate;
  4: string layoutString;
}

struct JdbcSplit {
  1: string connectorId;
  2: string catalogName;
  3: string schemaName;
  4: string tableName;
  5: JdbcThriftTupleDomain tupleDomain;
  6: optional JdbcExpression additionalPredicate;
}

union ThriftValueSet {
  1: EquatableValueSet equatableValueSet;
  2: SortedRangeSet sortedRangeSet;
  3: AllOrNoneValueSet allOrNoneValueSet;
}

struct Marker {
  1: PrestoType type;
  2: optional Block valueBlock;
  3: Bound bound;
}

struct Domain {
  1: ThriftValueSet values;
  2: bool nullAllowed;
}

struct Range {
  1: Marker low;
  2: Marker high;
}

struct JdbcThriftColumnDomain {
  1: JdbcColumnHandle column;
  2: Domain domain;
}
