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
import com.facebook.presto.raptorx.metadata.ColumnInfo;
import com.facebook.presto.raptorx.metadata.DistributionInfo;
import com.facebook.presto.raptorx.metadata.TableInfo;
import com.facebook.presto.raptorx.transaction.Transaction;
import com.facebook.presto.raptorx.util.PageListBuilder;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.raptorx.util.PredicateUtil.getColumnIndex;
import static com.facebook.presto.raptorx.util.PredicateUtil.getStringValue;
import static com.facebook.presto.raptorx.util.PredicateUtil.listTables;
import static com.facebook.presto.spi.SystemTable.Distribution.SINGLE_COORDINATOR;
import static com.facebook.presto.spi.predicate.TupleDomain.extractFixedValues;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class TableSystemTable
        implements SystemTable
{
    private final TransactionManager transactionManager;
    private final ConnectorTableMetadata tableMetadata;

    @Inject
    public TableSystemTable(TransactionManager transactionManager, TypeManager typeManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");

        Type varcharArray = typeManager.getType(parseTypeSignature("array<varchar>"));
        this.tableMetadata = new ConnectorTableMetadata(
                new SchemaTableName("system", "tables"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata("table_schema", VARCHAR))
                        .add(new ColumnMetadata("table_name", VARCHAR))
                        .add(new ColumnMetadata("temporal_column", VARCHAR))
                        .add(new ColumnMetadata("ordering_columns", varcharArray))
                        .add(new ColumnMetadata("distribution_name", VARCHAR))
                        .add(new ColumnMetadata("bucket_count", BIGINT))
                        .add(new ColumnMetadata("bucketing_columns", varcharArray))
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

        // TODO: fetch table info in bulk
        List<TableInfo> tables = listTables(transaction, schemaName, tableName).stream()
                .map(transaction::getTableId)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(transaction::getTableInfo)
                .collect(toImmutableList());

        PageListBuilder builder = PageListBuilder.forTable(tableMetadata);

        for (TableInfo table : tables) {
            builder.beginRow();
            writeTable(builder, transaction, table);
            builder.endRow();
        }

        return new FixedPageSource(builder.build());
    }

    private static void writeTable(PageListBuilder builder, Transaction transaction, TableInfo table)
    {
        // extract special columns
        Optional<String> temporalColumn = table.getTemporalColumn()
                .map(ColumnInfo::getColumnName);

        List<String> ordering = table.getSortColumns().stream()
                .map(ColumnInfo::getColumnName)
                .collect(toImmutableList());

        // schema_name
        builder.appendVarchar(transaction.getSchemaInfo(table.getSchemaId()).getSchemaName());

        // table_name
        builder.appendVarchar(table.getTableName());

        // temporal_column
        if (temporalColumn.isPresent()) {
            builder.appendVarchar(temporalColumn.get());
        }
        else {
            builder.appendNull();
        }

        // ordering_columns
        if (ordering.isEmpty()) {
            builder.appendNull();
        }
        else {
            builder.appendVarcharArray(ordering);
        }

        // distribution_name
        DistributionInfo distribution = transaction.getDistributionInfo(table.getDistributionId());
        if (distribution.getDistributionName().isPresent()) {
            builder.appendVarchar(distribution.getDistributionName().get());
        }
        else {
            builder.appendNull();
        }

        // bucket_count, bucketing_columns
        if (distribution.getColumnTypes().isEmpty()) {
            builder.appendNull();
            builder.appendNull();
        }
        else {
            List<String> bucketing = table.getBucketColumns().stream()
                    .map(ColumnInfo::getColumnName)
                    .collect(toImmutableList());
            builder.appendBigint(distribution.getBucketCount());
            builder.appendVarcharArray(bucketing);
        }
    }
}
