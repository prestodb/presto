package com.facebook.presto.metadata;

import com.facebook.presto.connector.informationSchema.InformationSchemaMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkColumnName;
import static com.facebook.presto.metadata.QualifiedTableName.convertFromSchemaTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;

@Singleton
public class MetadataManager
        implements Metadata
{
    // Note this must be a list to assure dual is always checked first
    private final CopyOnWriteArrayList<ConnectorMetadata> internalSchemas = new CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, ConnectorMetadata> connectors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConnectorMetadata> informationSchemas = new ConcurrentHashMap<>();
    private final FunctionRegistry functions = new FunctionRegistry();

    public void addConnectorMetadata(String catalogName, ConnectorMetadata connectorMetadata)
    {
        checkState(connectors.putIfAbsent(catalogName, connectorMetadata) == null,
                "Catalog '%s' is already registered", catalogName);

        informationSchemas.put(catalogName, new InformationSchemaMetadata(catalogName));
    }

    public void addInternalSchemaMetadata(ConnectorMetadata connectorMetadata)
    {
        internalSchemas.add(checkNotNull(connectorMetadata, "connectorMetadata is null"));
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        return functions.get(name, parameterTypes);
    }

    @Override
    public FunctionInfo getFunction(FunctionHandle handle)
    {
        return functions.get(handle);
    }

    @Override
    public boolean isAggregationFunction(QualifiedName name)
    {
        return functions.isAggregationFunction(name);
    }

    @Override
    public List<FunctionInfo> listFunctions()
    {
        return functions.list();
    }

    @Override
    public List<String> listSchemaNames(String catalogName)
    {
        checkCatalogName(catalogName);
        ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
        for (ConnectorMetadata metadata : allConnectorsFor(catalogName)) {
            schemaNames.addAll(metadata.listSchemaNames());
        }
        return ImmutableList.copyOf(schemaNames.build());
    }

    @Override
    public Optional<TableHandle> getTableHandle(QualifiedTableName table)
    {
        checkNotNull(table, "table is null");

        SchemaTableName tableName = table.asSchemaTableName();
        for (ConnectorMetadata connectorMetadata : allConnectorsFor(table.getCatalogName())) {
            TableHandle tableHandle = connectorMetadata.getTableHandle(tableName);
            if (tableHandle != null) {
                return Optional.of(tableHandle);
            }

        }
        return Optional.absent();
    }

    @Override
    public TableMetadata getTableMetadata(TableHandle tableHandle)
    {
        return lookupConnectorFor(tableHandle).getTableMetadata(tableHandle);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        return lookupConnectorFor(tableHandle).getColumnHandles(tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");

        return lookupConnectorFor(tableHandle).getColumnMetadata(tableHandle, columnHandle);
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableSet.Builder<QualifiedTableName> tables = ImmutableSet.builder();
        for (ConnectorMetadata internalSchemaMetadata : allConnectorsFor(prefix.getCatalogName())) {
            tables.addAll(transform(internalSchemaMetadata.listTables(prefix.getSchemaName().orNull()), convertFromSchemaTableName(prefix.getCatalogName())));
        }
        return ImmutableList.copyOf(tables.build());
    }

    @Override
    public Optional<ColumnHandle> getColumnHandle(TableHandle tableHandle, String columnName)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkColumnName(columnName);

        return Optional.fromNullable(lookupConnectorFor(tableHandle).getColumnHandle(tableHandle, columnName));
    }

    @Override
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableMap.Builder<QualifiedTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (ConnectorMetadata connectorMetadata : allConnectorsFor(prefix.getCatalogName())) {
            for (Entry<SchemaTableName, List<ColumnMetadata>> entry : connectorMetadata.listTableColumns(prefix.asSchemaTablePrefix()).entrySet()) {
                QualifiedTableName tableName = new QualifiedTableName(prefix.getCatalogName(), entry.getKey().getSchemaName(), entry.getKey().getTableName());
                builder.put(tableName, entry.getValue());
            }
        }
        return builder.build();
    }

    @Override
    public TableHandle createTable(String catalogName, TableMetadata tableMetadata)
    {
        ConnectorMetadata connectorMetadata = connectors.get(catalogName);
        checkArgument(connectorMetadata != null, "Catalog %s does not exist", catalogName);
        return connectorMetadata.createTable(tableMetadata);
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        lookupConnectorFor(tableHandle).dropTable(tableHandle);
    }

    @Override
    public Optional<String> getConnectorId(TableHandle tableHandle)
    {
        for (Entry<String, ConnectorMetadata> entry : connectors.entrySet()) {
            if (entry.getValue().canHandle(tableHandle)) {
                return Optional.of(entry.getKey());
            }
        }
        return Optional.absent();
    }

    @Override
    public Optional<TableHandle> getTableHandle(String connectorId, SchemaTableName tableName)
    {
        // use catalog name in place of connector id
        ConnectorMetadata metadata = connectors.get(connectorId);
        if (metadata == null) {
            return Optional.absent();
        }
        return Optional.fromNullable(metadata.getTableHandle(tableName));
    }

    private List<ConnectorMetadata> allConnectorsFor(String catalogName)
    {
        ImmutableList.Builder<ConnectorMetadata> builder = ImmutableList.builder();
        builder.addAll(internalSchemas);
        ConnectorMetadata connector = connectors.get(catalogName);
        if (connector != null) {
            builder.add(connector);
        }
        ConnectorMetadata informationSchema = informationSchemas.get(catalogName);
        if (informationSchema != null) {
            builder.add(informationSchema);
        }
        return builder.build();
    }

    private ConnectorMetadata lookupConnectorFor(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");

        for (ConnectorMetadata metadata : informationSchemas.values()) {
            if (metadata.canHandle(tableHandle)) {
                return metadata;
            }
        }

        for (ConnectorMetadata internalSchemaMetadata : internalSchemas) {
            if (internalSchemaMetadata.canHandle(tableHandle)) {
                return internalSchemaMetadata;
            }
        }

        for (ConnectorMetadata metadata : connectors.values()) {
            if (metadata.canHandle(tableHandle)) {
                return metadata;
            }
        }

        throw new IllegalArgumentException("Table %s does not exist: " + tableHandle);
    }
}
