package com.facebook.presto.tpch;

import static com.facebook.presto.TupleInfo.Type;

public class TpchSchema
{
    public interface Column
    {
        String getTableName();
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

        private final String tableName = "orders";
        private final int index;
        private final Type type;


        private Orders(int index, Type type)
        {
            this.index = index;
            this.type = type;
        }

        @Override
        public String getTableName()
        {
            return tableName;
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

        private final String tableName = "lineitem";
        private final int index;
        private final Type type;

        private LineItem(int index, Type type)
        {
            this.index = index;
            this.type = type;
        }

        @Override
        public String getTableName()
        {
            return tableName;
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
}
