package com.facebook.presto.tpch;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.metadata.InternalSchemaMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.MetadataUtil.ColumnMetadataListBuilder.columnsBuilder;
import static com.facebook.presto.spi.ColumnType.DOUBLE;
import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TpchMetadata
        implements ConnectorMetadata
{
    public static final String TPCH_CATALOG_NAME = "tpch";
    public static final String TPCH_SCHEMA_NAME = "default";

    public static final String TPCH_ORDERS_NAME = "orders";

    public static final SchemaTableMetadata TPCH_ORDERS_METADATA = new SchemaTableMetadata(new SchemaTableName(TPCH_SCHEMA_NAME, TPCH_ORDERS_NAME), columnsBuilder()
            .column("orderkey", LONG) // Mostly increasing IDs
            .column("custkey", LONG) // 15:1
            .column("orderstatus", STRING) // 3 unique
            .column("totalprice", DOUBLE) // High cardinality
            .column("orderdate", STRING) // 2400 unique
            .column("orderpriority", STRING) // 5 unique
            .column("clerk", STRING) // High cardinality
            .column("shippriority", STRING) // 1 unique
            .column("comment", STRING)
            .build()); // Arbitrary strings

    public static final String TPCH_LINEITEM_NAME = "lineitem";
    public static final SchemaTableMetadata TPCH_LINEITEM_METADATA = new SchemaTableMetadata(new SchemaTableName(TPCH_SCHEMA_NAME, TPCH_LINEITEM_NAME),  columnsBuilder()
            .column("orderkey", LONG)
            .column("partkey", LONG)
            .column("suppkey", LONG)
            .column("linenumber", LONG)
            .column("quantity", DOUBLE)
            .column("extendedprice", DOUBLE)
            .column("discount", DOUBLE)
            .column("tax", DOUBLE)
            .column("returnflag", STRING)// Single letter, low cardinality
            .column("linestatus", STRING)// Single letter, low cardinality
            .column("shipdate", STRING)
            .column("commitdate", STRING)
            .column("receiptdate", STRING)
            .column("shipinstruct", STRING)
            .column("shipmode", STRING)
            .column("comment", STRING)
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
        if (TPCH_SCHEMA_NAME.equals(table.getSchemaName()) && tables.containsKey(table.getTableName())) {
            return new TpchTableHandle(table.getTableName());
        }
        return null;
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
            builder.put(columnMetadata.getName(), new TpchColumnHandle(columnMetadata.getName(), columnMetadata.getOrdinalPosition(), columnMetadata.getType()));
        }
        return builder.build();
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        for (ColumnMetadata columnMetadata : getTableMetadata(tableHandle).getColumns()) {
            if (columnMetadata.getName().equals(columnName)) {
                return new TpchColumnHandle(columnMetadata.getName(), columnMetadata.getOrdinalPosition(), columnMetadata.getType());
            }
        }
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        if (prefix.getSchemaName() != null && !TPCH_SCHEMA_NAME.equals(prefix.getSchemaName())) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(prefix.getSchemaName())) {
            int position = 1;
            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            for (ColumnMetadata column : tables.get(tableName.getTableName()).getColumns()) {
                columns.add(new ColumnMetadata(column.getName(), column.getType(), position, false));
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

        checkArgument(columnHandle instanceof TpchColumnHandle, "columnHandle is not an instance of TpchColumnHandle");
        String columnName = ((TpchColumnHandle) columnHandle).getColumnName();
        for (ColumnMetadata column : tables.get(tableName).getColumns()) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        throw new IllegalArgumentException(String.format("Table %s does not have column %s", tableName, columnName));
    }

    @Override
    public List<SchemaTableName> listTables(@Nullable String schemaNameOrNull)
    {
        if (schemaNameOrNull == null || TPCH_SCHEMA_NAME.equals(schemaNameOrNull)) {
            ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
            for (String tableName : tables.keySet()) {
                builder.add(new SchemaTableName(TPCH_SCHEMA_NAME, tableName));
            }
            return builder.build();
        }
        return ImmutableList.of();
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
