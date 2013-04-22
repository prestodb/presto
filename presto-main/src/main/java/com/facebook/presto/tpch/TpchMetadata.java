package com.facebook.presto.tpch;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.ConnectorMetadata;
import com.facebook.presto.metadata.InternalColumnHandle;
import com.facebook.presto.metadata.InternalSchemaMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.ColumnMetadataListBuilder.columnsBuilder;
import static com.facebook.presto.metadata.MetadataUtil.checkTable;
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
    public static final QualifiedTableName TPCH_ORDERS = new QualifiedTableName(TPCH_CATALOG_NAME, TPCH_SCHEMA_NAME, TPCH_ORDERS_NAME);
    public static final TableMetadata TPCH_ORDERS_METADATA = new TableMetadata(TPCH_ORDERS, columnsBuilder()
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
    public static final QualifiedTableName TPCH_LINEITEM = new QualifiedTableName(TPCH_CATALOG_NAME, TPCH_SCHEMA_NAME, TPCH_LINEITEM_NAME);
    public static final TableMetadata TPCH_LINEITEM_METADATA = new TableMetadata(TPCH_LINEITEM,  columnsBuilder()
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
        return new MetadataManager(ImmutableSet.<InternalSchemaMetadata>of(), ImmutableSet.<ConnectorMetadata>of(new TpchMetadata()));
    }

    private final Map<String, TableMetadata> tables;

    @Inject
    public TpchMetadata()
    {
        tables = ImmutableMap.of(
                TPCH_ORDERS_NAME, TPCH_ORDERS_METADATA,
                TPCH_LINEITEM_NAME, TPCH_LINEITEM_METADATA);
    }

    @Override
    public int priority()
    {
        return Integer.MIN_VALUE;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof TpchTableHandle;
    }

    @Override
    public boolean canHandle(QualifiedTablePrefix prefix)
    {
        return prefix.getCatalogName().equals("tpch");
    }

    @Override
    public List<String> listSchemaNames(String catalogName)
    {
        checkNotNull(catalogName, "catalogName is null");

        List<QualifiedTableName> tables = listTables(QualifiedTablePrefix.builder(catalogName).build());
        Set<String> schemaNames = new HashSet<>();

        for (QualifiedTableName qualifiedTableName : tables) {
            schemaNames.add(qualifiedTableName.getSchemaName());
        }

        return ImmutableList.copyOf(schemaNames);
    }

    @Override
    public TableHandle getTableHandle(QualifiedTableName table)
    {
        checkTable(table);
        if (!TPCH_CATALOG_NAME.equals(table.getCatalogName()) || !TPCH_SCHEMA_NAME.equals(table.getSchemaName()) || !tables.containsKey(table.getTableName())) {
            return null;
        }
        return new TpchTableHandle(table);
    }

    @Override
    public TableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        QualifiedTableName tableName = getTableName(tableHandle);

        if (!TPCH_CATALOG_NAME.equals(tableName.getCatalogName()) || !TPCH_SCHEMA_NAME.equals(tableName.getSchemaName()) || !tables.containsKey(tableName.getTableName())) {
            throw new IllegalArgumentException(String.format("Table %s does not exist", tableHandle));
        }
        return tables.get(tableName.getTableName());
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
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableMap.Builder<QualifiedTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (QualifiedTableName tableName : listTables(prefix)) {
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
        QualifiedTableName tableName = getTableName(tableHandle);
        checkArgument(columnHandle instanceof InternalColumnHandle, "columnHandle is not an instance of InternalColumnHandle");
        InternalColumnHandle internalColumnHandle = (InternalColumnHandle) columnHandle;
        int columnIndex = internalColumnHandle.getColumnIndex();
        return tables.get(tableName.getTableName()).getColumns().get(columnIndex);
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        return ImmutableList.of();
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        if (!prefix.getCatalogName().equals(TPCH_CATALOG_NAME) || !prefix.getSchemaName().or(TPCH_SCHEMA_NAME).equals(TPCH_SCHEMA_NAME)) {
            return ImmutableList.of();
        }

        ImmutableList.Builder<QualifiedTableName> builder = ImmutableList.builder();
        for (String tableNAme : tables.keySet()) {
            builder.add(new QualifiedTableName(TPCH_CATALOG_NAME, TPCH_SCHEMA_NAME, tableNAme));
        }
        return builder.build();
    }

    @Override
    public TableHandle createTable(TableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    private QualifiedTableName getTableName(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof TpchTableHandle, "tableHandle is not an instance of TpchTableHandle");
        TpchTableHandle tpchTableHandle = (TpchTableHandle) tableHandle;
        return tpchTableHandle.getTableName();
    }
}
