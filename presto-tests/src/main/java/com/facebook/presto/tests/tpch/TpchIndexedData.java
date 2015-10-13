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
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchRecordSetProvider;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Function;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.tpch.TpchTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

class TpchIndexedData
{
    private final Map<Set<TpchScaledColumn>, IndexedTable> indexedTables;

    public TpchIndexedData(String connectorId, TpchIndexSpec tpchIndexSpec)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(tpchIndexSpec, "tpchIndexSpec is null");

        TpchMetadata tpchMetadata = new TpchMetadata(connectorId);
        TpchRecordSetProvider tpchRecordSetProvider = new TpchRecordSetProvider();

        ImmutableMap.Builder<Set<TpchScaledColumn>, IndexedTable> indexedTablesBuilder = ImmutableMap.builder();

        Set<TpchScaledTable> tables = tpchIndexSpec.listIndexedTables();
        for (TpchScaledTable table : tables) {
            SchemaTableName tableName = new SchemaTableName("sf" + table.getScaleFactor(), table.getTableName());
            TpchTableHandle tableHandle = tpchMetadata.getTableHandle(null, tableName);
            Map<String, ColumnHandle> columnHandles = new LinkedHashMap<>(tpchMetadata.getColumnHandles(null, tableHandle));
            for (Set<String> columnNames : tpchIndexSpec.getColumnIndexes(table)) {
                List<String> keyColumnNames = ImmutableList.copyOf(columnNames); // Finalize the key order
                Set<TpchScaledColumn> keyColumns = FluentIterable.from(keyColumnNames)
                        .transform(TpchScaledColumn.columnFunction(table))
                        .toSet();

                TpchTable<?> tpchTable = TpchTable.getTable(table.getTableName());
                RecordSet recordSet = tpchRecordSetProvider.getRecordSet(tpchTable, ImmutableList.copyOf(columnHandles.values()), table.getScaleFactor(), 0, 1);
                IndexedTable indexedTable = indexTable(recordSet, ImmutableList.copyOf(columnHandles.keySet()), keyColumnNames);
                indexedTablesBuilder.put(keyColumns, indexedTable);
            }
        }

        indexedTables = indexedTablesBuilder.build();
    }

    public Optional<IndexedTable> getIndexedTable(String tableName, double scaleFactor, Set<String> indexColumnNames)
    {
        TpchScaledTable table = new TpchScaledTable(tableName, scaleFactor);
        Set<TpchScaledColumn> indexColumns = FluentIterable.from(indexColumnNames)
                .transform(TpchScaledColumn.columnFunction(table))
                .toSet();
        return Optional.ofNullable(indexedTables.get(indexColumns));
    }

    private static <T> List<T> extractPositionValues(final List<T> values, List<Integer> positions)
    {
        return Lists.transform(positions, position -> {
            checkPositionIndex(position, values.size());
            return values.get(position);
        });
    }

    private static IndexedTable indexTable(RecordSet recordSet, final List<String> outputColumns, List<String> keyColumns)
    {
        List<Integer> keyPositions = FluentIterable.from(keyColumns)
                .transform(columnName -> {
                    int position = outputColumns.indexOf(columnName);
                    checkState(position != -1);
                    return position;
                })
                .toList();

        ImmutableListMultimap.Builder<MaterializedTuple, MaterializedTuple> indexedValuesBuilder = ImmutableListMultimap.builder();

        List<Type> outputTypes = recordSet.getColumnTypes();
        List<Type> keyTypes = extractPositionValues(outputTypes, keyPositions);

        RecordCursor cursor = recordSet.cursor();
        while (cursor.advanceNextPosition()) {
            List<Object> values = extractValues(cursor, outputTypes);
            List<Object> keyValues = extractPositionValues(values, keyPositions);

            indexedValuesBuilder.put(new MaterializedTuple(keyValues), new MaterializedTuple(values));
        }

        return new IndexedTable(keyColumns, keyTypes, outputColumns, outputTypes, indexedValuesBuilder.build());
    }

    private static List<Object> extractValues(RecordCursor cursor, List<Type> types)
    {
        List<Object> list = new ArrayList<>(types.size());
        for (int i = 0; i < types.size(); i++) {
            list.add(extractObject(cursor, i, types.get(i)));
        }
        return list;
    }

    private static Object extractObject(RecordCursor cursor, int field, Type type)
    {
        if (cursor.isNull(field)) {
            return null;
        }

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            return cursor.getBoolean(field);
        }
        else if (javaType == long.class) {
            return cursor.getLong(field);
        }
        else if (javaType == double.class) {
            return cursor.getDouble(field);
        }
        else if (javaType == Slice.class) {
            return cursor.getSlice(field).toStringUtf8();
        }
        throw new AssertionError("Unsupported type: " + type);
    }

    public static class IndexedTable
    {
        private final List<String> keyColumnNames;
        private final List<Type> keyTypes;
        private final List<String> outputColumnNames;
        private final List<Type> outputTypes;
        private final ListMultimap<MaterializedTuple, MaterializedTuple> keyToValues;

        private IndexedTable(List<String> keyColumnNames, List<Type> keyTypes, List<String> outputColumnNames, List<Type> outputTypes, ListMultimap<MaterializedTuple, MaterializedTuple> keyToValues)
        {
            this.keyColumnNames = ImmutableList.copyOf(requireNonNull(keyColumnNames, "keyColumnNames is null"));
            this.keyTypes = ImmutableList.copyOf(requireNonNull(keyTypes, "keyTypes is null"));
            this.outputColumnNames = ImmutableList.copyOf(requireNonNull(outputColumnNames, "outputColumnNames is null"));
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
            this.keyToValues = ImmutableListMultimap.copyOf(requireNonNull(keyToValues, "keyToValues is null"));
        }

        public List<String> getKeyColumns()
        {
            return keyColumnNames;
        }

        public List<String> getOutputColumns()
        {
            return outputColumnNames;
        }

        public RecordSet lookupKeys(RecordSet recordSet)
        {
            checkArgument(recordSet.getColumnTypes().equals(keyTypes), "Input RecordSet keys do not match expected key type");

            Iterable<RecordSet> outputRecordSets = Iterables.transform(tupleIterable(recordSet), key -> {
                for (Object value : key.getValues()) {
                    if (value == null) {
                        throw new IllegalArgumentException("TPCH index does not support null values");
                    }
                }
                return lookupKey(key);
            });

            return new ConcatRecordSet(outputRecordSets, outputTypes);
        }

        public RecordSet lookupKey(MaterializedTuple tupleKey)
        {
            return new MaterializedTupleRecordSet(keyToValues.get(tupleKey), outputTypes);
        }

        private static Iterable<MaterializedTuple> tupleIterable(final RecordSet recordSet)
        {
            return () -> new AbstractIterator<MaterializedTuple>()
            {
                private final RecordCursor cursor = recordSet.cursor();

                @Override
                protected MaterializedTuple computeNext()
                {
                    if (!cursor.advanceNextPosition()) {
                        return endOfData();
                    }
                    return new MaterializedTuple(extractValues(cursor, recordSet.getColumnTypes()));
                }
            };
        }
    }

    private static class TpchScaledColumn
    {
        private final TpchScaledTable table;
        private final String columnName;

        private TpchScaledColumn(TpchScaledTable table, String columnName)
        {
            this.table = requireNonNull(table, "table is null");
            this.columnName = requireNonNull(columnName, "columnName is null");
        }

        public static Function<String, TpchScaledColumn> columnFunction(final TpchScaledTable table)
        {
            return name -> new TpchScaledColumn(table, name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, columnName);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final TpchScaledColumn other = (TpchScaledColumn) obj;
            return Objects.equals(this.table, other.table) && Objects.equals(this.columnName, other.columnName);
        }
    }
}
