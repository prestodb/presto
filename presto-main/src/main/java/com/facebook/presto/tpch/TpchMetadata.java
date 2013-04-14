package com.facebook.presto.tpch;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.ConnectorMetadata;
import com.facebook.presto.metadata.InternalColumnHandle;
import com.facebook.presto.metadata.InternalSchemaMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SchemaTableMetadata;
import com.facebook.presto.metadata.SchemaTableName;
import com.facebook.presto.metadata.SchemaTablePrefix;
import com.facebook.presto.metadata.TableHandle;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.MetadataUtil.ColumnMetadataListBuilder.columnsBuilder;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TpchMetadata
        implements ConnectorMetadata
{
    public static final String TPCH_CATALOG_NAME = "tpch";
    public static final String TPCH_SCHEMA_NAME = "default";

    public static final String TPCH_ORDERS_NAME = "orders";
    public static final SchemaTableMetadata TPCH_ORDERS_METADATA = new SchemaTableMetadata(new SchemaTableName(TPCH_SCHEMA_NAME, TPCH_ORDERS_NAME), columnsBuilder()
            .column("orderkey", FIXED_INT_64) // Mostly increasing IDs
            .column("custkey", FIXED_INT_64) // 15:1
            .column("orderstatus", VARIABLE_BINARY) // 3 unique
            .column("totalprice", DOUBLE) // High cardinality
            .column("orderdate", VARIABLE_BINARY) // 2400 unique
            .column("orderpriority", VARIABLE_BINARY) // 5 unique
            .column("clerk", VARIABLE_BINARY) // High cardinality
            .column("shippriority", VARIABLE_BINARY) // 1 unique
            .column("comment", VARIABLE_BINARY)
            .build()); // Arbitrary strings

    public static final String TPCH_LINEITEM_NAME = "lineitem";
    public static final SchemaTableMetadata TPCH_LINEITEM_METADATA = new SchemaTableMetadata(new SchemaTableName(TPCH_SCHEMA_NAME, TPCH_LINEITEM_NAME),  columnsBuilder()
            .column("orderkey", FIXED_INT_64)
            .column("partkey", FIXED_INT_64)
            .column("suppkey", FIXED_INT_64)
            .column("linenumber", FIXED_INT_64)
            .column("quantity", DOUBLE)
            .column("extendedprice", DOUBLE)
            .column("discount", DOUBLE)
            .column("tax", DOUBLE)
            .column("returnflag", VARIABLE_BINARY)// Single letter, low cardinality
            .column("linestatus", VARIABLE_BINARY)// Single letter, low cardinality
            .column("shipdate", VARIABLE_BINARY)
            .column("commitdate", VARIABLE_BINARY)
            .column("receiptdate", VARIABLE_BINARY)
            .column("shipinstruct", VARIABLE_BINARY)
            .column("shipmode", VARIABLE_BINARY)
            .column("comment", VARIABLE_BINARY)
            .build());

    public static Metadata createTpchMetadata()
    {
        return new MetadataManager(ImmutableSet.<InternalSchemaMetadata>of(),
                ImmutableMap.<String, ConnectorMetadata>of(TPCH_CATALOG_NAME, new TpchMetadata()));
    }

    private final Map<String, SchemaTableMetadata> tables;

    @Inject
    public TpchMetadata()
    {
        tables = ImmutableMap.of(
                TPCH_ORDERS_NAME, TPCH_ORDERS_METADATA,
                TPCH_LINEITEM_NAME, TPCH_LINEITEM_METADATA);
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof TpchTableHandle;
    }

    @Override
    public List<String> listSchemaNames()
    {
        return ImmutableList.of(TPCH_SCHEMA_NAME);
    }

    @Override
    public TableHandle getTableHandle(SchemaTableName table)
    {
        checkNotNull(table, "table is null");
        if (!TPCH_SCHEMA_NAME.equals(table.getSchemaName()) || !tables.containsKey(table.getTableName())) {
            return null;
        }
        return new TpchTableHandle(table.getTableName());
    }

    @Override
    public SchemaTableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        String tableName = getTableName(tableHandle);
        checkArgument(tables.containsKey(tableName), "Table %s does not exist", tableHandle);
        return tables.get(tableName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getTableMetadata(tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new TpchColumnHandle(columnMetadata.getOrdinalPosition(), columnMetadata.getType()));
        }
        return builder.build();
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        for (ColumnMetadata columnMetadata : getTableMetadata(tableHandle).getColumns()) {
            if (columnMetadata.getName().equals(columnName)) {
                return new TpchColumnHandle(columnMetadata.getOrdinalPosition(), columnMetadata.getType());
            }
        }
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        if (!TPCH_SCHEMA_NAME.equals(prefix.getSchemaName().or(TPCH_SCHEMA_NAME))) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(prefix.getSchemaName())) {
            int position = 1;
            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            for (ColumnMetadata column : tables.get(tableName.getTableName()).getColumns()) {
                columns.add(new ColumnMetadata(column.getName(), column.getType(), position));
                position++;
            }
            tableColumns.put(tableName, columns.build());
        }
        return tableColumns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        String tableName = getTableName(tableHandle);
        checkArgument(tables.containsKey(tableName), "Table %s does not exist", tableHandle);

        checkArgument(columnHandle instanceof InternalColumnHandle, "columnHandle is not an instance of InternalColumnHandle");
        InternalColumnHandle internalColumnHandle = (InternalColumnHandle) columnHandle;
        int columnIndex = internalColumnHandle.getColumnIndex();
        return tables.get(tableName).getColumns().get(columnIndex);
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        return ImmutableList.of();
    }

    @Override
    public List<SchemaTableName> listTables(Optional<String> schemaName)
    {
        checkNotNull(schemaName, "schemaName is null");

        if (!TPCH_SCHEMA_NAME.equals(schemaName.or(TPCH_SCHEMA_NAME))) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String tableName : tables.keySet()) {
            builder.add(new SchemaTableName(TPCH_SCHEMA_NAME, tableName));
        }
        return builder.build();
    }

    @Override
    public TableHandle createTable(SchemaTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    private String getTableName(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof TpchTableHandle, "tableHandle is not an instance of TpchTableHandle");
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;
        return tpchTableHandle.getTableName();
    }
}
