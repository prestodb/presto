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
package com.facebook.presto.tpch;

import com.facebook.presto.operator.MaterializedTuple;
import com.facebook.presto.operator.MaterializedTupleRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.tpch.TpchIndexSpec.Table;
import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;

public class TpchIndexedData
{
    private final Map<Set<QualifiedColumn>, IndexedTable> indexedTables;

    public TpchIndexedData(TpchIndexSpec tpchIndexSpec)
    {
        checkNotNull(tpchIndexSpec, "tpchIndexSpec is null");

        ImmutableMap.Builder<Set<QualifiedColumn>, IndexedTable> indexedTablesBuilder = ImmutableMap.builder();

        Set<Table> tables = tpchIndexSpec.listIndexedTables();
        for (Table table : tables) {
            for (Set<String> columnNames : tpchIndexSpec.getColumnIndexes(table)) {
                List<String> keyColumnNames = ImmutableList.copyOf(columnNames); // Finalize the key order
                Set<QualifiedColumn> keyColumns = FluentIterable.from(keyColumnNames)
                        .transform(QualifiedColumn.columnFunction(table))
                        .toSet();

                TpchTable<?> tpchTable = TpchTable.getTable(table.getTableName());
                List<String> outputColumnNames = Lists.transform(tpchTable.getColumns(), new Function<TpchColumn<? extends TpchEntity>, String>()
                {
                    @Override
                    public String apply(TpchColumn<? extends TpchEntity> tpchColumn)
                    {
                        return tpchColumn.getColumnName();
                    }
                });
                IndexedTable indexedTable = indexTable(tpchTable, table.getScaleFactor(), outputColumnNames, keyColumnNames);
                indexedTablesBuilder.put(keyColumns, indexedTable);
            }
        }

        indexedTables = indexedTablesBuilder.build();
    }

    public Optional<IndexedTable> getIndexedTable(String tableName, double scaleFactor, Set<String> indexColumnNames)
    {
        Table table = new Table(tableName, scaleFactor);
        Set<QualifiedColumn> indexColumns = FluentIterable.from(indexColumnNames)
                .transform(QualifiedColumn.columnFunction(table))
                .toSet();
        return Optional.fromNullable(indexedTables.get(indexColumns));
    }

    private static <T> List<T> extractPositionValues(final List<T> values, List<Integer> positions)
    {
        return Lists.transform(positions, new Function<Integer, T>()
        {
            @Override
            public T apply(Integer position)
            {
                checkPositionIndex(position, values.size());
                return values.get(position);
            }
        });
    }

    private static <E extends TpchEntity> RecordSet getRecordSet(TpchTable<E> table, double scaleFactor, List<String> columnNames)
    {
        ImmutableList.Builder<TpchColumn<E>> builder = ImmutableList.builder();
        for (String columnName : columnNames) {
            builder.add(table.getColumn(columnName));
        }
        return createTpchRecordSet(table, builder.build(), scaleFactor, 1, 1);
    }

    private static IndexedTable indexTable(TpchTable table, double scaleFactor, final List<String> outputColumns, List<String> keyColumns)
    {
        List<Integer> keyPositions = FluentIterable.from(keyColumns)
                .transform(new Function<String, Integer>()
                {
                    @Override
                    public Integer apply(String columnName)
                    {
                        int position = outputColumns.indexOf(columnName);
                        checkState(position != -1);
                        return position;
                    }
                })
                .toList();

        RecordSet recordSet = getRecordSet(table, scaleFactor, outputColumns);

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
            return new String(cursor.getString(field), StandardCharsets.UTF_8);
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
            this.keyColumnNames = ImmutableList.copyOf(checkNotNull(keyColumnNames, "keyColumnNames is null"));
            this.keyTypes = ImmutableList.copyOf(checkNotNull(keyTypes, "keyTypes is null"));
            this.outputColumnNames = ImmutableList.copyOf(checkNotNull(outputColumnNames, "outputColumnNames is null"));
            this.outputTypes = ImmutableList.copyOf(checkNotNull(outputTypes, "outputTypes is null"));
            this.keyToValues = ImmutableListMultimap.copyOf(checkNotNull(keyToValues, "keyToValues is null"));
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

            Iterable<RecordSet> outputRecordSets = Iterables.transform(tupleIterable(recordSet), new Function<MaterializedTuple, RecordSet>()
            {
                @Override
                public RecordSet apply(MaterializedTuple key)
                {
                    return lookupKey(key);
                }
            });

            return new ConcatRecordSet(outputRecordSets, outputTypes);
        }

        public RecordSet lookupKey(MaterializedTuple tupleKey)
        {
            return new MaterializedTupleRecordSet(keyToValues.get(tupleKey), outputTypes);
        }

        private static Iterable<MaterializedTuple> tupleIterable(final RecordSet recordSet)
        {
            return new Iterable<MaterializedTuple>()
            {
                @Override
                public Iterator<MaterializedTuple> iterator()
                {
                    return new AbstractIterator<MaterializedTuple>()
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
            };
        }
    }

    private static class QualifiedColumn
    {
        private final Table table;
        private final String columnName;

        private QualifiedColumn(Table table, String columnName)
        {
            this.table = checkNotNull(table, "table is null");
            this.columnName = checkNotNull(columnName, "columnName is null");
        }

        public static Function<String, QualifiedColumn> columnFunction(final Table table)
        {
            return new Function<String, QualifiedColumn>()
            {
                @Override
                public QualifiedColumn apply(String columnName)
                {
                    return new QualifiedColumn(table, columnName);
                }
            };
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
            final QualifiedColumn other = (QualifiedColumn) obj;
            return Objects.equals(this.table, other.table) && Objects.equals(this.columnName, other.columnName);
        }
    }
}
