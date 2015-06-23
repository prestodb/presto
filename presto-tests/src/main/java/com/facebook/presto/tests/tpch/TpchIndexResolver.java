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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndexHandle;
import com.facebook.presto.spi.ConnectorIndexResolver;
import com.facebook.presto.spi.ConnectorResolvedIndex;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.MappedRecordSet;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.transform;

public class TpchIndexResolver
        implements ConnectorIndexResolver
{
    private final String connectorId;
    private final TpchIndexedData indexedData;

    public TpchIndexResolver(String connectorId, TpchIndexedData indexedData)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.indexedData = checkNotNull(indexedData, "indexedData is null");
    }

    @Override
    public ConnectorResolvedIndex resolveIndex(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            Set<ColumnHandle> indexableColumns,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        TpchTableHandle tpchTableHandle = checkType(tableHandle, TpchTableHandle.class, "tableHandle");

        // Keep the fixed values that don't overlap with the indexableColumns
        // Note: technically we could more efficiently utilize the overlapped columns, but this way is simpler for now
        Map<ColumnHandle, Comparable<?>> fixedValues = Maps.filterKeys(tupleDomain.extractFixedValues(), not(in(indexableColumns)));

        // determine all columns available for index lookup
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        builder.addAll(transform(indexableColumns, columnNameGetter()));
        builder.addAll(transform(fixedValues.keySet(), columnNameGetter()));
        Set<String> lookupColumnNames = builder.build();

        // do we have an index?
        if (!indexedData.getIndexedTable(tpchTableHandle.getTableName(), tpchTableHandle.getScaleFactor(), lookupColumnNames).isPresent()) {
            return null;
        }

        TupleDomain<ColumnHandle> filteredTupleDomain = tupleDomain;
        if (!tupleDomain.isNone()) {
            filteredTupleDomain = TupleDomain.withColumnDomains(Maps.filterKeys(tupleDomain.getDomains(), not(in(fixedValues.keySet()))));
        }
        return new ConnectorResolvedIndex(new TpchIndexHandle(connectorId, tpchTableHandle.getTableName(), tpchTableHandle.getScaleFactor(), lookupColumnNames, TupleDomain.withFixedValues(fixedValues)), filteredTupleDomain);
    }

    @Override
    public ConnectorIndex getIndex(ConnectorSession session, ConnectorIndexHandle indexHandle, List<ColumnHandle> lookupSchema, List<ColumnHandle> outputSchema)
    {
        TpchIndexHandle tpchIndexHandle = checkType(indexHandle, TpchIndexHandle.class, "indexHandle");

        Map<ColumnHandle, Comparable<?>> fixedValues = tpchIndexHandle.getFixedValues().extractFixedValues();
        checkArgument(!any(lookupSchema, in(fixedValues.keySet())), "Lookup columnHandles are not expected to overlap with the fixed value predicates");

        // Establish an order for the fixedValues
        List<ColumnHandle> fixedValueColumns = ImmutableList.copyOf(fixedValues.keySet());

        // Extract the fixedValues as their raw values and types
        ImmutableList.Builder<Object> valueBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
        for (ColumnHandle fixedValueColumn : fixedValueColumns) {
            valueBuilder.add(fixedValues.get(fixedValueColumn));
            typeBuilder.add(((TpchColumnHandle) fixedValueColumn).getType());
        }
        final List<Object> rawFixedValues = valueBuilder.build();
        final List<Type> rawFixedTypes = typeBuilder.build();

        // Establish the schema after we append the fixed values to the lookup keys.
        List<ColumnHandle> finalLookupSchema = ImmutableList.<ColumnHandle>builder()
                .addAll(lookupSchema)
                .addAll(fixedValueColumns)
                .build();

        Optional<TpchIndexedData.IndexedTable> indexedTable = indexedData.getIndexedTable(tpchIndexHandle.getTableName(), tpchIndexHandle.getScaleFactor(), tpchIndexHandle.getIndexColumnNames());
        checkState(indexedTable.isPresent());
        TpchIndexedData.IndexedTable table = indexedTable.get();

        // Compute how to map from the final lookup schema to the table index key order
        final List<Integer> keyRemap = computeRemap(handleToNames(finalLookupSchema), table.getKeyColumns());
        Function<RecordSet, RecordSet> keyFormatter = new Function<RecordSet, RecordSet>()
        {
            @Override
            public RecordSet apply(RecordSet key)
            {
                return new MappedRecordSet(new AppendingRecordSet(key, rawFixedValues, rawFixedTypes), keyRemap);
            }
        };

        // Compute how to map from the output of the indexed data to the expected output schema
        final List<Integer> outputRemap = computeRemap(table.getOutputColumns(), handleToNames(outputSchema));
        Function<RecordSet, RecordSet> outputFormatter = new Function<RecordSet, RecordSet>()
        {
            @Override
            public RecordSet apply(RecordSet output)
            {
                return new MappedRecordSet(output, outputRemap);
            }
        };

        return new TpchConnectorIndex(keyFormatter, outputFormatter, table);
    }

    private static List<Integer> computeRemap(List<String> startSchema, List<String> endSchema)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (String columnName : endSchema) {
            int index = startSchema.indexOf(columnName);
            checkArgument(index != -1, "Column name in end that is not in the start: %s", columnName);
            builder.add(index);
        }
        return builder.build();
    }

    private static List<String> handleToNames(List<ColumnHandle> columnHandles)
    {
        return Lists.transform(columnHandles, columnNameGetter());
    }

    private static Function<ColumnHandle, String> columnNameGetter()
    {
        return new Function<ColumnHandle, String>()
        {
            @Override
            public String apply(ColumnHandle columnHandle)
            {
                return checkType(columnHandle, TpchColumnHandle.class, "columnHandle").getColumnName();
            }
        };
    }
}
