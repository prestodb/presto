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
package com.facebook.presto.cassandra.util;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.facebook.presto.cassandra.CassandraColumnHandle;
import com.facebook.presto.cassandra.CassandraTableHandle;
import com.facebook.presto.cassandra.CassandraType;
import com.facebook.presto.spi.ColumnHandle;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import io.airlift.slice.Slice;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class CassandraCqlUtils
{
    private CassandraCqlUtils()
    {
    }

    private static final String[] KEYWORDS = {"ADD", "ALL", "ALLOW", "ALTER", "AND", "APPLY",
            "ASC", "ASCII", "AUTHORIZE", "BATCH", "BEGIN", "BIGINT", "BLOB", "BOOLEAN", "BY",
            "CLUSTERING", "COLUMNFAMILY", "COMPACT", "COUNT", "COUNTER", "CREATE", "DECIMAL",
            "DATE", "DELETE", "DESC", "DOUBLE", "DROP", "FILTERING", "FLOAT", "FROM", "GRANT", "IN",
            "INDEX", "INET", "INSERT", "INT", "INTO", "KEY", "KEYSPACE", "KEYSPACES", "LIMIT",
            "LIST", "MAP", "MODIFY", "NORECURSIVE", "NOSUPERUSER", "OF", "ON", "ORDER", "PASSWORD",
            "PERMISSION", "PERMISSIONS", "PRIMARY", "RENAME", "REVOKE", "SCHEMA", "SELECT", "SET",
            "SMALLINT", "STORAGE", "SUPERUSER", "TABLE", "TEXT", "TIMESTAMP", "TIMEUUID", "TINYINT", "TO", "TOKEN",
            "TRUNCATE", "TTL", "TYPE", "UNLOGGED", "UPDATE", "USE", "USER", "USERS", "USING",
            "UUID", "VALUES", "VARCHAR", "VARINT", "WHERE", "WITH", "WRITETIME"};

    private static final Set<String> keywords = new HashSet<>(Arrays.asList(KEYWORDS));

    public static final String EMPTY_COLUMN_NAME = "__empty__";

    public static String validSchemaName(String identifier)
    {
        return quoteIdentifier(identifier);
    }

    public static String validTableName(String identifier)
    {
        return quoteIdentifier(identifier);
    }

    public static String validColumnName(String identifier)
    {
        if (identifier.isEmpty() || identifier.equals(EMPTY_COLUMN_NAME)) {
            return "\"\"";
        }

        return quoteIdentifier(identifier);
    }

    private static String quoteIdentifier(String identifier)
    {
        return '"' + identifier + '"';
    }

    public static String quoteStringLiteral(String string)
    {
        return "'" + string.replace("'", "''") + "'";
    }

    public static String quoteStringLiteralForJson(String string)
    {
        return '"' + new String(JsonStringEncoder.getInstance().quoteAsUTF8(string)) + '"';
    }

    public static void appendSelectColumns(StringBuilder stringBuilder, List<? extends ColumnHandle> columns)
    {
        appendSelectColumns(stringBuilder, columns, true);
    }

    private static void appendSelectColumns(StringBuilder stringBuilder, List<? extends ColumnHandle> columns, boolean first)
    {
        for (ColumnHandle column : columns) {
            if (first) {
                first = false;
            }
            else {
                stringBuilder.append(",");
            }
            stringBuilder.append(validColumnName(((CassandraColumnHandle) column).getName()));
        }
    }

    public static String cqlNameToSqlName(String name)
    {
        if (name.isEmpty()) {
            return EMPTY_COLUMN_NAME;
        }
        return name;
    }

    public static String sqlNameToCqlName(String name)
    {
        if (name.equals(EMPTY_COLUMN_NAME)) {
            return "";
        }
        return name;
    }

    public static Select selectFrom(CassandraTableHandle tableHandle, List<CassandraColumnHandle> columns)
    {
        String schema = validSchemaName(tableHandle.getSchemaName());
        String table = validTableName(tableHandle.getTableName());

        // Driver 4.x: selectFrom() returns SelectFrom, need to add at least one column to get Select
        if (columns.isEmpty()) {
            return QueryBuilder.selectFrom(schema, table).all();
        }

        Select select = QueryBuilder.selectFrom(schema, table)
                .column(validColumnName(columns.get(0).getName()));

        // Add remaining columns
        for (int i = 1; i < columns.size(); i++) {
            select = select.column(validColumnName(columns.get(i).getName()));
        }

        return select;
    }

    public static Select selectDistinctFrom(CassandraTableHandle tableHandle, List<CassandraColumnHandle> columns)
    {
        String schema = validSchemaName(tableHandle.getSchemaName());
        String table = validTableName(tableHandle.getTableName());

        // Driver 4.x: selectFrom() returns SelectFrom, need to add at least one column to get Select
        if (columns.isEmpty()) {
            return QueryBuilder.selectFrom(schema, table).distinct().all();
        }

        Select select = QueryBuilder.selectFrom(schema, table).distinct()
                .column(validColumnName(columns.get(0).getName()));

        // Add remaining columns
        for (int i = 1; i < columns.size(); i++) {
            select = select.column(validColumnName(columns.get(i).getName()));
        }

        return select;
    }

    public static Select selectCountAllFrom(CassandraTableHandle tableHandle)
    {
        String schema = validSchemaName(tableHandle.getSchemaName());
        String table = validTableName(tableHandle.getTableName());

        // Driver 4.x: Use countAll() selector
        return QueryBuilder.selectFrom(schema, table)
                .countAll();
    }

    public static String cqlValue(String value, CassandraType cassandraType)
    {
        switch (cassandraType) {
            case ASCII:
            case TEXT:
            case VARCHAR:
                return quoteStringLiteral(value);
            case INET:
                // remove '/' in the string. e.g. /127.0.0.1
                return quoteStringLiteral(value.substring(1));
            default:
                return value;
        }
    }

    public static String toCQLCompatibleString(Object value)
    {
        if (value instanceof Slice) {
            return ((Slice) value).toStringUtf8();
        }
        return value.toString();
    }
}
