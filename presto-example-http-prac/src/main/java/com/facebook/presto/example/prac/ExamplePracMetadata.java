package com.facebook.presto.example.prac;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * @Author LTR
 * @Date 2025/4/16 13:57
 * @注释
 */
public class ExamplePracMetadata implements ConnectorMetadata {
    private final String connectorId;
    private final ExamplePracClient examplePracClient;

    public ExamplePracMetadata(String connectorId, ExamplePracClient examplePracClient) {
        this.connectorId = connectorId;
        this.examplePracClient = examplePracClient;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.copyOf(examplePracClient.getSchemaNames());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaName){
        //requireNonNull(schemaName,"The schema is null!");
        //this schemas input may be null
        Set<String> schemaNames;
        if (schemaName != null){
            schemaNames = ImmutableSet.of(schemaName);
        }
        else{
            schemaNames = examplePracClient.getSchemaNames();
        }
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for(String schema :schemaNames){
            for(String table:examplePracClient.getTableNames(schema)){
                builder.add(new SchemaTableName(schema,table));
            }
        }
        return builder.build();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return null;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        return null;
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        return null;
    }
}
