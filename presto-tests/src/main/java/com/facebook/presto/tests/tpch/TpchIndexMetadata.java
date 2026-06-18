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
package com.facebook.presto.tests.tpch;

import com.facebook.presto.common.block.MethodHandleUtil;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.tests.tpch.TpchIndexProvider.handleToNames;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static java.util.Objects.requireNonNull;

public class TpchIndexMetadata
        extends TpchMetadata
{
    private final TpchIndexedData indexedData;
    // For tables in this set, add an extra map column to their metadata.
    private final Set<String> tableWithExtraColumn = ImmutableSet.of("orders_extra");

    public TpchIndexMetadata(String connectorId, TpchIndexedData indexedData)
    {
        super(connectorId);
        this.indexedData = requireNonNull(indexedData, "indexedData is null");
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Set<ColumnHandle> indexableColumns,
            Set<ColumnHandle> outputColumns,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;
        String tableName = tpchTableHandle.getTableName();
        if (tableWithExtraColumn.contains(tableName)) {
            tpchTableHandle = new TpchTableHandle(getOriginalTpchTableName(tableName), tpchTableHandle.getScaleFactor());
        }

        // Keep the fixed values that don't overlap with the indexableColumns
        // Note: technically we could more efficiently utilize the overlapped columns, but this way is simpler for now

        Map<ColumnHandle, NullableValue> fixedValues = TupleDomain.extractFixedValues(tupleDomain).orElse(ImmutableMap.of())
                .entrySet().stream()
                .filter(entry -> !indexableColumns.contains(entry.getKey()))
                .filter(entry -> !entry.getValue().isNull()) // strip nulls since meaningless in index join lookups
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // determine all columns available for index lookup
        Set<String> lookupColumnNames = ImmutableSet.<String>builder()
                .addAll(handleToNames(ImmutableList.copyOf(indexableColumns)))
                .addAll(handleToNames(ImmutableList.copyOf(fixedValues.keySet())))
                .build();

        // do we have an index?
        if (!indexedData.getIndexedTable(tpchTableHandle.getTableName(), tpchTableHandle.getScaleFactor(), lookupColumnNames).isPresent()) {
            return Optional.empty();
        }

        TupleDomain<ColumnHandle> filteredTupleDomain = tupleDomain;
        if (!tupleDomain.isNone()) {
            filteredTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains().get(), not(in(fixedValues.keySet()))));
        }
        TpchIndexHandle indexHandle = new TpchIndexHandle(
                tableName,
                tpchTableHandle.getScaleFactor(),
                lookupColumnNames,
                TupleDomain.fromFixedValues(fixedValues));
        return Optional.of(new ConnectorResolvedIndex(indexHandle, filteredTupleDomain));
    }

    @Override
    public TpchTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        String tableName = schemaTableName.getTableName();
        if (tableWithExtraColumn.contains(tableName)) {
            TpchTableHandle originalTableHandle = super.getTableHandle(session, new SchemaTableName(schemaTableName.getSchemaName(), getOriginalTpchTableName(tableName)));
            return new TpchTableHandle(schemaTableName.getTableName(), originalTableHandle.getScaleFactor());
        }
        return super.getTableHandle(session, schemaTableName);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorTableLayoutHandle> tableLayoutHandle, List<ColumnHandle> columnHandles, Constraint<ColumnHandle> constraint)
    {
        String tableName = ((TpchTableHandle) tableHandle).getTableName();
        if (tableWithExtraColumn.contains(tableName)) {
            TpchTableHandle originalTableHandle = new TpchTableHandle(getOriginalTpchTableName(tableName), ((TpchTableHandle) tableHandle).getScaleFactor());
            return super.getTableStatistics(session, originalTableHandle, tableLayoutHandle, columnHandles, constraint);
        }
        return super.getTableStatistics(session, tableHandle, tableLayoutHandle, columnHandles, constraint);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;
        String tableName = tpchTableHandle.getTableName();
        if (tableWithExtraColumn.contains(tableName)) {
            ConnectorTableMetadata tableMetadata = super.getTableMetadata(session, new TpchTableHandle(getOriginalTpchTableName(tableName), tpchTableHandle.getScaleFactor()));
            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            columns.addAll(tableMetadata.getColumns());
            columns.add(getExtraMapColumnMetadata());
            return new ConnectorTableMetadata(new SchemaTableName(tableMetadata.getTable().getSchemaName(), tableName),
                    columns.build());
        }
        return super.getTableMetadata(session, tableHandle);
    }

    private ColumnMetadata getExtraMapColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName("data")
                .setType(new MapType(BIGINT,
                        VARCHAR,
                        MethodHandleUtil.methodHandle(TpchIndexMetadata.class, "throwUnsupportedOperation"),
                        MethodHandleUtil.methodHandle(TpchIndexMetadata.class, "throwUnsupportedOperation")))
                .build();
    }

    private String getOriginalTpchTableName(String tableName)
    {
        String suffix = "_extra";
        if (tableName != null && tableName.endsWith(suffix)) {
            return tableName.substring(0, tableName.length() - suffix.length());
        }
        return tableName;
    }

    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }
}
