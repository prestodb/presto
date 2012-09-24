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

    public static enum Orders implements Column
    {
        ORDERKEY(0, Type.FIXED_INT_64),
        CUSTKEY(1, Type.FIXED_INT_64),
        ORDERSTATUS(2, Type.VARIABLE_BINARY),
        TOTALPRICE(3, Type.DOUBLE),
        ORDERDATE(4, Type.VARIABLE_BINARY),
        ORDERPRIORITY(5, Type.VARIABLE_BINARY),
        SHIPPRIORITY(6, Type.VARIABLE_BINARY);

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
        DISCOUNT(6, Type.DOUBLE),
        TAX(7, Type.DOUBLE);

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
