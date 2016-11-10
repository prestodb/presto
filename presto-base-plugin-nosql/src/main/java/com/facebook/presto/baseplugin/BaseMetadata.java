package com.facebook.presto.baseplugin;

import com.facebook.presto.baseplugin.metastore.BaseMetastore;
import com.facebook.presto.baseplugin.metastore.BaseMetastoreDefinition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Created by amehta on 6/13/16.
 */
public class BaseMetadata implements ConnectorMetadata {
    private final BaseProvider baseProvider;
    private final BaseMetastoreDefinition baseMetastoreDefinition;

    @Inject
    public BaseMetadata(BaseProvider baseProvider, BaseConnectorInfo baseConnectorInfo){
        this.baseProvider = requireNonNull(baseProvider, "baseProvider is null");
        this.baseMetastoreDefinition = new BaseMetastore(baseConnectorInfo, baseProvider.getConfig());
    }

    ////////////////////////////////////////USE BASE PROVIDER////////////////////////////////

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return baseProvider.listSchemaNames(session);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        return baseProvider.listTableNames(session, Optional.ofNullable(schemaNameOrNull));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return baseProvider.getMappedColumnHandlesForSchemaTable(session, ((BaseTableHandle) tableHandle).getSchemaTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        SchemaTableName schemaTableName = ((BaseTableHandle) table).getSchemaTableName();
        return new ConnectorTableMetadata(schemaTableName, baseProvider.listColumnMetadata(session, schemaTableName));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        SchemaTableName schemaTableName = new SchemaTableName(Optional.ofNullable(prefix.getSchemaName()).orElse("default_schema"), Optional.ofNullable(prefix.getTableName()).orElse("columns"));
        return ImmutableMap.of(schemaTableName, baseProvider.listColumnMetadata(session, schemaTableName));
    }

    ////////////////////////////////GENERICALLY HANDLED////////////////////////////////

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return new BaseTableHandle(tableName);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        return ImmutableList.of(new ConnectorTableLayoutResult(new ConnectorTableLayout(new BaseTableLayoutHandle((BaseTableHandle) table, constraint.getSummary())), constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        BaseColumnHandle baseColumnHandle = (BaseColumnHandle)columnHandle;
        return new ColumnMetadata(baseColumnHandle.getColumnName(), baseColumnHandle.getColumnType());
    }

    //////////////////////////////////////////Views////////////////////////////////////

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, String viewData, boolean replace) {
        //baseMetastoreDefinition.createView(session, viewName, viewData, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName) {
        //baseMetastoreDefinition.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull) {
        //return baseMetastoreDefinition.listViews(session, Optional.ofNullable(schemaNameOrNull));
        return ImmutableList.of();
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix) {
        //return baseMetastoreDefinition.getViews(session, prefix);
        return ImmutableMap.of();
    }
}
