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

import com.facebook.presto.raptorx.TransactionManager;
import com.facebook.presto.raptorx.metadata.TableStats;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.util.PageListBuilder;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.raptorx.util.PredicateUtil.getColumnIndex;
import static com.facebook.presto.raptorx.util.PredicateUtil.getStringValue;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.predicate.TupleDomain.extractFixedValues;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TableStatsSystemTable
        implements SystemTable
{
    private final TransactionManager transactionManager;
    private final ConnectorTableMetadata tableMetadata;

    @Inject
    public TableStatsSystemTable(TransactionManager transactionManager, TypeManager typeManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");

        this.tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("system", "table_stats"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("table_schema", VARCHAR))
                        .add(new ColumnMetadata("table_name", VARCHAR))
                        .add(new ColumnMetadata("create_time", TIMESTAMP))
                        .add(new ColumnMetadata("update_time", TIMESTAMP))
                        .add(new ColumnMetadata("table_version", BIGINT))
                        .add(new ColumnMetadata("chunk_count", BIGINT))
                        .add(new ColumnMetadata("row_count", BIGINT))
                        .add(new ColumnMetadata("compressed_size", BIGINT))
                        .add(new ColumnMetadata("uncompressed_size", BIGINT))
                        .build());
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
        Transaction transaction = transactionManager.get(transactionHandle);

        Map<Integer, NullableValue> values = extractFixedValues(constraint).orElse(ImmutableMap.of());
        Optional<String> schemaName = getStringValue(values.get(getColumnIndex(tableMetadata, "table_schema")));
        Optional<String> tableName = getStringValue(values.get(getColumnIndex(tableMetadata, "table_name")));

        SchemaTablePrefix prefix = schemaName.map(schema ->
                tableName.map(table -> new SchemaTablePrefix(schema, table))
                        .orElseGet(() -> new SchemaTablePrefix(schema)))
                .orElseGet(SchemaTablePrefix::new);

        Collection<TableStats> tables = transaction.listTableStats(prefix);

        PageListBuilder builder = PageListBuilder.forTable(tableMetadata);

        for (TableStats table : tables) {
            builder.beginRow();
            writeTable(builder, table);
            builder.endRow();
        }

        return new FixedPageSource(builder.build());
    }

    private static void writeTable(PageListBuilder builder, TableStats stats)
    {
        builder.appendVarchar(stats.getTableSchema());
        builder.appendVarchar(stats.getTableName());
        builder.appendTimestamp(stats.getCreateTime());
        builder.appendTimestamp(stats.getUpdateTime());
        builder.appendBigint(stats.getTableVersion());
        builder.appendBigint(stats.getChunkCount());
        builder.appendBigint(stats.getRowCount());
        builder.appendBigint(stats.getCompressedSize());
        builder.appendBigint(stats.getUncompressedSize());
    }
}
