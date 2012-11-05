package com.facebook.presto.tpch;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;

import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.Type;

// TODO: consider migrating to Metastore format when it gets added
public class TpchSchema
{
    public static class Table
    {
        private final String name;
        private final List<Column> columns;
        private final TupleInfo tupleInfo;

        public Table(String name, Iterable<Column> columnsIterable)
        {
            this.name = Preconditions.checkNotNull(name, "name is null");
            this.columns = ImmutableList.copyOf(Preconditions.checkNotNull(columnsIterable, "columnsIterable is null"));
            tupleInfo = new TupleInfo(Iterables.transform(columns, new Function<Column, Type>()
            {
                @Override
                public Type apply(Column input)
                {
                    return input.getType();
                }
            }));
        }

        public String getName()
        {
            return name;
        }

        public List<Column> getColumns()
        {
            return columns;
        }

        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }
    }

    public interface Column
    {
        Table getTable();
        int getIndex();
        Type getType();
    }

    // Main fact table
    public static enum Orders implements Column
    {
        ORDERKEY(0, Type.FIXED_INT_64), // Mostly increasing IDs
        CUSTKEY(1, Type.FIXED_INT_64), // 15:1
        ORDERSTATUS(2, Type.VARIABLE_BINARY), // 3 unique
        TOTALPRICE(3, Type.DOUBLE), // High cardinality
        ORDERDATE(4, Type.VARIABLE_BINARY), // 2400 unique
        ORDERPRIORITY(5, Type.VARIABLE_BINARY), // 5 unique
        CLERK(6, Type.VARIABLE_BINARY), // High cardinality
        SHIPPRIORITY(7, Type.VARIABLE_BINARY), // 1 unique
        COMMENT(8, Type.VARIABLE_BINARY); // Arbitrary strings

        public static final Table TABLE = TpchSchema.createTable("orders", Orders.class);

        private final int index;
        private final Type type;

        private Orders(int index, Type type)
        {
            this.index = index;
            this.type = type;
        }

        @Override
        public Table getTable()
        {
            return TABLE;
        }

        @Override
        public int getIndex()
        {
            return index;
        }

        @Override
        public Type getType()
        {
            return type;
        }
    }

    public static enum LineItem implements Column
    {
        ORDERKEY(0, Type.FIXED_INT_64),
        PARTKEY(1, Type.FIXED_INT_64),
        SUPPKEY(2, Type.FIXED_INT_64),
        LINENUMBER(3, Type.FIXED_INT_64),
        QUANTITY(4, Type.DOUBLE),
        EXTENDEDPRICE(5, Type.DOUBLE),
        DISCOUNT(6, Type.DOUBLE),
        TAX(7, Type.DOUBLE),
        RETURNFLAG(8, Type.VARIABLE_BINARY), // Single letter, low cardinality
        LINESTATUS(9, Type.VARIABLE_BINARY), // Single letter, low cardinality
        SHIPDATE(10, Type.VARIABLE_BINARY),
        COMMITDATE(11, Type.VARIABLE_BINARY),
        RECEIPTDATE(12, Type.VARIABLE_BINARY),
        SHIPINSTRUCT(13, Type.VARIABLE_BINARY),
        SHIPMODE(14, Type.VARIABLE_BINARY),
        COMMENT(15, Type.VARIABLE_BINARY);
        
        public static final Table TABLE = TpchSchema.createTable("lineitem", LineItem.class);

        private final int index;
        private final Type type;

        private LineItem(int index, Type type)
        {
            this.index = index;
            this.type = type;
        }

        @Override
        public Table getTable()
        {
            return TABLE;
        }

        @Override
        public int getIndex()
        {
            return index;
        }

        @Override
        public Type getType()
        {
            return type;
        }
    }
    
    private static Table createTable(String name, Class<? extends Column> clazz)
    {
        ImmutableSortedMap.Builder<Integer, Column> columnBuilder = ImmutableSortedMap.naturalOrder();
        for (Column column : clazz.getEnumConstants()) {
            columnBuilder.put(column.getIndex(), column);
        }
        ImmutableSortedMap<Integer, Column> sortedMap = columnBuilder.build();

        // Verify that there are no gaps in the definition
        int lastIndex = -1;
        for (Integer index : sortedMap.keySet()) {
            Preconditions.checkState(index == lastIndex + 1, "Table indicies has a gap");
            lastIndex = index;
        }

        return new Table(name, sortedMap.values());
    }
}
