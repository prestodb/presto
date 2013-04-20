package com.facebook.presto.tpch;

import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.metadata.TestingMetadata;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;

public class TpchSchema
{
    public static final String CATALOG_NAME = "tpch";
    public static final String SCHEMA_NAME = "default";

    private static final Metadata METADATA_INSTANCE = createMetadata();

    public static TpchTableHandle tableHandle(String tableName)
    {
        TableMetadata table = METADATA_INSTANCE.getTable(new QualifiedTableName(CATALOG_NAME, SCHEMA_NAME, tableName));
        return (TpchTableHandle) table.getTableHandle().get();
    }

    public static TpchColumnHandle columnHandle(TpchTableHandle tableHandle, String columnName)
    {
        TableMetadata table = METADATA_INSTANCE.getTable(new QualifiedTableName(CATALOG_NAME, SCHEMA_NAME, tableHandle.getTableName()));
        for (ColumnMetadata columnMetadata : table.getColumns()) {
            if (columnMetadata.getName().equals(columnName)) {
                return (TpchColumnHandle) columnMetadata.getColumnHandle().get();
            }
        }
        throw new IllegalArgumentException("Unknown column name: " + columnName);
    }

    public static TpchColumnHandle columnHandle(TpchTableHandle tableHandle, int fieldIndex)
    {
        TableMetadata table = METADATA_INSTANCE.getTable(new QualifiedTableName(CATALOG_NAME, SCHEMA_NAME, tableHandle.getTableName()));
        return (TpchColumnHandle) table.getColumns().get(fieldIndex).getColumnHandle().get();
    }

    public static TestingMetadata createMetadata()
    {
        TestingMetadata testingMetadata = new TestingMetadata();
        testingMetadata.createTable(createOrders());
        testingMetadata.createTable(createLineItem());
        return testingMetadata;
    }

    public static TableMetadata createOrders()
    {
        TpchTableHandle tpchTableHandle = new TpchTableHandle("orders");
        return new TableMetadata(
                new QualifiedTableName(CATALOG_NAME, SCHEMA_NAME, tpchTableHandle.getTableName()),
                ImmutableList.<ColumnMetadata>of(
                        createColumn("orderkey", FIXED_INT_64, 0), // Mostly increasing IDs
                        createColumn("custkey", FIXED_INT_64, 1), // 15:1
                        createColumn("orderstatus", VARIABLE_BINARY, 2), // 3 unique
                        createColumn("totalprice", DOUBLE, 3), // High cardinality
                        createColumn("orderdate", VARIABLE_BINARY, 4), // 2400 unique
                        createColumn("orderpriority", VARIABLE_BINARY, 5), // 5 unique
                        createColumn("clerk", VARIABLE_BINARY, 6), // High cardinality
                        createColumn("shippriority", VARIABLE_BINARY, 7), // 1 unique
                        createColumn("comment", VARIABLE_BINARY, 8) // Arbitrary strings
                ),
                tpchTableHandle
        );
    }

    public static TableMetadata createLineItem()
    {
        TpchTableHandle tpchTableHandle = new TpchTableHandle("lineitem");
        return new TableMetadata(
                new QualifiedTableName(CATALOG_NAME, SCHEMA_NAME, tpchTableHandle.getTableName()),
                ImmutableList.<ColumnMetadata>of(
                        createColumn("orderkey", FIXED_INT_64, 0),
                        createColumn("partkey", FIXED_INT_64, 1),
                        createColumn("suppkey", FIXED_INT_64, 2),
                        createColumn("linenumber", FIXED_INT_64, 3),
                        createColumn("quantity", DOUBLE, 4),
                        createColumn("extendedprice", DOUBLE, 5),
                        createColumn("discount", DOUBLE, 6),
                        createColumn("tax", DOUBLE, 7),
                        createColumn("returnflag", VARIABLE_BINARY, 8), // Single letter, low cardinality
                        createColumn("linestatus", VARIABLE_BINARY, 9), // Single letter, low cardinality
                        createColumn("shipdate", VARIABLE_BINARY, 10),
                        createColumn("commitdate", VARIABLE_BINARY, 11),
                        createColumn("receiptdate", VARIABLE_BINARY, 12),
                        createColumn("shipinstruct", VARIABLE_BINARY, 13),
                        createColumn("shipmode", VARIABLE_BINARY, 14),
                        createColumn("comment", VARIABLE_BINARY, 15)
                ),
                tpchTableHandle
        );
    }

    private static ColumnMetadata createColumn(String columnName, TupleInfo.Type type, int fieldIndex)
    {
        return new ColumnMetadata(columnName, type, new TpchColumnHandle(fieldIndex, type));
    }
}
