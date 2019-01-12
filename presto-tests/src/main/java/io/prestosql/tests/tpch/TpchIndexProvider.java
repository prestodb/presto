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
package io.prestosql.tests.tpch;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorIndex;
import io.prestosql.spi.connector.ConnectorIndexHandle;
import io.prestosql.spi.connector.ConnectorIndexProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.split.MappedRecordSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class TpchIndexProvider
        implements ConnectorIndexProvider
{
    private final TpchIndexedData indexedData;

    public TpchIndexProvider(TpchIndexedData indexedData)
    {
        this.indexedData = requireNonNull(indexedData, "indexedData is null");
    }

    @Override
    public ConnectorIndex getIndex(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorIndexHandle indexHandle,
            List<ColumnHandle> lookupSchema,
            List<ColumnHandle> outputSchema)
    {
        TpchIndexHandle tpchIndexHandle = (TpchIndexHandle) indexHandle;

        Map<ColumnHandle, NullableValue> fixedValues = TupleDomain.extractFixedValues(tpchIndexHandle.getFixedValues()).get();
        checkArgument(lookupSchema.stream().noneMatch(handle -> fixedValues.keySet().contains(handle)),
                "Lookup columnHandles are not expected to overlap with the fixed value predicates");

        // Establish an order for the fixedValues
        List<ColumnHandle> fixedValueColumns = ImmutableList.copyOf(fixedValues.keySet());

        // Extract the fixedValues as their raw values and types
        List<Object> rawFixedValues = new ArrayList<>(fixedValueColumns.size());
        List<Type> rawFixedTypes = new ArrayList<>(fixedValueColumns.size());
        for (ColumnHandle fixedValueColumn : fixedValueColumns) {
            rawFixedValues.add(fixedValues.get(fixedValueColumn).getValue());
            rawFixedTypes.add(((TpchColumnHandle) fixedValueColumn).getType());
        }

        // Establish the schema after we append the fixed values to the lookup keys.
        List<ColumnHandle> finalLookupSchema = ImmutableList.<ColumnHandle>builder()
                .addAll(lookupSchema)
                .addAll(fixedValueColumns)
                .build();

        Optional<TpchIndexedData.IndexedTable> indexedTable = indexedData.getIndexedTable(tpchIndexHandle.getTableName(), tpchIndexHandle.getScaleFactor(), tpchIndexHandle.getIndexColumnNames());
        checkState(indexedTable.isPresent());
        TpchIndexedData.IndexedTable table = indexedTable.get();

        // Compute how to map from the final lookup schema to the table index key order
        List<Integer> keyRemap = computeRemap(handleToNames(finalLookupSchema), table.getKeyColumns());
        Function<RecordSet, RecordSet> keyFormatter = key -> new MappedRecordSet(new AppendingRecordSet(key, rawFixedValues, rawFixedTypes), keyRemap);

        // Compute how to map from the output of the indexed data to the expected output schema
        List<Integer> outputRemap = computeRemap(table.getOutputColumns(), handleToNames(outputSchema));
        Function<RecordSet, RecordSet> outputFormatter = output -> new MappedRecordSet(output, outputRemap);

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

    static List<String> handleToNames(List<ColumnHandle> columnHandles)
    {
        return columnHandles.stream()
                .map(TpchColumnHandle.class::cast)
                .map(TpchColumnHandle::getColumnName)
                .collect(toList());
    }
}
