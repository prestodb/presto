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
package com.facebook.presto.raptorx.systemtable;

import com.facebook.presto.raptorx.RaptorTableHandle;
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.SchemaInfo;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.util.ColumnRange;
import com.facebook.presto.raptorx.util.PageListBuilder;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.VerifyException;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ColumnRangesSystemTable
        implements SystemTable
{
    private static final String MIN_COLUMN_SUFFIX = "_min";
    private static final String MAX_COLUMN_SUFFIX = "_max";
    private static final String COLUMN_RANGES_TABLE_SUFFIX = "$column_ranges";

    private final RaptorTableHandle sourceTable;
    private final List<ColumnInfo> indexedRaptorColumns;
    private final ConnectorTableMetadata tableMetadata;
    private final Transaction transaction;

    public ColumnRangesSystemTable(RaptorTableHandle sourceTable, Transaction transaction)
    {
        this.sourceTable = requireNonNull(sourceTable, "sourceTable is null");
        this.transaction = transaction;

        TableInfo tableInfo = transaction.getTableInfo(sourceTable.getTableId());
        SchemaInfo schemaInfo = transaction.getSchemaInfo(tableInfo.getSchemaId());
        this.indexedRaptorColumns = tableInfo.getColumns().stream()
                .filter(column -> isIndexedType(column.getType()))
                .collect(toImmutableList());
        List<ColumnMetadata> systemTableColumns = indexedRaptorColumns.stream()
                .flatMap(column -> Stream.of(
                        new ColumnMetadata(column.getColumnName() + MIN_COLUMN_SUFFIX, column.getType(), null, false),
                        new ColumnMetadata(column.getColumnName() + MAX_COLUMN_SUFFIX, column.getType(), null, false)))
                .collect(toImmutableList());
        SchemaTableName tableName = new SchemaTableName(schemaInfo.getSchemaName(), tableInfo.getTableName() + COLUMN_RANGES_TABLE_SUFFIX);
        this.tableMetadata = new ConnectorTableMetadata(tableName, systemTableColumns);
    }

    public static Optional<SchemaTableName> getSourceTable(SchemaTableName tableName)
    {
        if (tableName.getTableName().endsWith(COLUMN_RANGES_TABLE_SUFFIX) &&
                !tableName.getTableName().equals(COLUMN_RANGES_TABLE_SUFFIX)) {
            int tableNameLength = tableName.getTableName().length() - COLUMN_RANGES_TABLE_SUFFIX.length();
            return Optional.of(new SchemaTableName(
                    tableName.getSchemaName(),
                    tableName.getTableName().substring(0, tableNameLength)));
        }
        return Optional.empty();
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        PageListBuilder builder = PageListBuilder.forTable(tableMetadata);
        List<ColumnRange> columnRanges = transaction.getColumnRanges(sourceTable.getTableId(), indexedRaptorColumns);
        builder.beginRow();

        for (ColumnRange columnRange : columnRanges) {
            Type columnType = columnRange.getColumnType();
            Object value = columnRange.getVal();

            if (value != null) {
                if (columnType.equals(BIGINT) || columnType.equals(TIMESTAMP)) {
                    builder.appendBigint((Long) value);
                }
                else if (columnType.equals(DATE)) {
                    builder.appendDate((Long) value);
                }
                else if (columnType.equals(BOOLEAN)) {
                    builder.appendBoolean((Boolean) value);
                }
                else {
                    throw new VerifyException("Unknown or unsupported column type: " + columnType);
                }
            }
            else {
                builder.appendNull();
            }
        }

        builder.endRow();
        return new FixedPageSource(builder.build());
    }

    private static boolean isIndexedType(Type type)
    {
        // We only consider the following types in the column_ranges system table
        // Exclude INTEGER because we don't collect column stats for INTEGER type.
        // Exclude DOUBLE because Java double is not completely compatible with MySQL double
        // Exclude VARCHAR because they can be truncated
        return type.equals(BOOLEAN) || type.equals(BIGINT) || type.equals(DATE) || type.equals(TIMESTAMP);
    }
}
