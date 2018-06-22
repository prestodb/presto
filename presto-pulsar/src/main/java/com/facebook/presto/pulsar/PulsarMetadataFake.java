package com.facebook.presto.pulsar;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;

import static com.facebook.presto.pulsar.PulsarHandleResolver.convertColumnHandle;
import static com.facebook.presto.pulsar.PulsarHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

public class PulsarMetadataFake implements ConnectorMetadata {

    private static Map<String, List<SchemaTableName>> schemaToTableMap = new HashMap<>();
    static {
        schemaToTableMap.put("schema1", Arrays.asList(new SchemaTableName("schema1", "schema1_table1"), new SchemaTableName("schema1", "schema1_table2")));
        schemaToTableMap.put("schema2", Arrays.asList(new SchemaTableName("schema2", "schema1_table1")));
    }

    private static Map<SchemaTableName, PulsarTopicDescription> schemaTableNameToTopicMap = new HashMap<>();
    static {
        schemaTableNameToTopicMap.put(new SchemaTableName("schema1", "schema1_table1"), new PulsarTopicDescription("schema1_table1", "schema1", "schema1_table1_topic"));
        schemaTableNameToTopicMap.put(new SchemaTableName("schema1", "schema1_table2"), new PulsarTopicDescription("schema1_table2", "schema1", "schema1_table2_topic"));
        schemaTableNameToTopicMap.put(new SchemaTableName("schema2", "schema2_table1"), new PulsarTopicDescription("schema2_table1", "schema1", "schema2_table1_topic"));
    }

    private static Map<SchemaTableName, List<ColumnMetadata>> schemaTableNameToColumnMetadataMap = new HashMap<>();
    static {
        schemaTableNameToColumnMetadataMap.put(new SchemaTableName("schema1", "schema1_table1"), Arrays.asList(new ColumnMetadata("id", IntegerType.INTEGER), new ColumnMetadata("field1", VarcharType.VARCHAR)));
        schemaTableNameToColumnMetadataMap.put(new SchemaTableName("schema1", "schema1_table2"), Arrays.asList(new ColumnMetadata("id", IntegerType.INTEGER), new ColumnMetadata("field1", VarcharType.VARCHAR)));
        schemaTableNameToColumnMetadataMap.put(new SchemaTableName("schema2", "schema2_table1"), Arrays.asList(new ColumnMetadata("id", IntegerType.INTEGER), new ColumnMetadata("field1", VarcharType.VARCHAR), new ColumnMetadata("field2", VarcharType.VARCHAR)));
    }


    private final String connectorId;
    private final PulsarAdmin pulsarAdmin;

    private static final Logger log = Logger.get(PulsarMetadata.class);

    @Inject
    public PulsarMetadataFake (PulsarConnectorId connectorId, PulsarConnectorConfig pulsarConnectorConfig) {
        log.info("connectorId: %s", connectorId);
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        try {
            this.pulsarAdmin = pulsarConnectorConfig.getPulsarAdmin();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        try {
            log.info("meta tenants: %s", pulsarAdmin.tenants().getTenants());
        } catch (PulsarAdminException e) {
            log.error(e, "ex: %s", e);
        }

        log.info("pulsarConnectorConfig: %s", pulsarConnectorConfig);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        log.info("listSchemaNames...");
        return new LinkedList<>(schemaToTableMap.keySet());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        log.info("getTableHandle: %s", tableName);

        PulsarTopicDescription table = schemaTableNameToTopicMap.get(tableName);
        if (table == null) {
            return null;
        }
        return new PulsarTableHandle(
                this.connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                table.getTopicName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        log.info("getTableLayouts - %s - %s - %s", table, constraint, desiredColumns);

        PulsarTableHandle handle = convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new PulsarTableLayoutHandle(handle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        log.info("getTableLayout: %s", handle);
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        log.info("getTableMetadata: %s", table);
        return getTableMetadata(convertTableHandle(table).toSchemaTableName());
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
        log.info("getTableMetadata - schemaTableName: %s", schemaTableName);

        PulsarTopicDescription table = schemaTableNameToTopicMap.get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        builder.addAll(schemaTableNameToColumnMetadataMap.get(schemaTableName));
        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        log.info("listTables: %s", schemaNameOrNull);

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : schemaTableNameToTopicMap.keySet()) {
            if (schemaNameOrNull == null || tableName.getSchemaName().equals(schemaNameOrNull)) {
                builder.add(tableName);
            }
        }

        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        log.info("getColumnHandles: %s", tableHandle);

        PulsarTableHandle pulsarTableHandle = convertTableHandle(tableHandle);

        PulsarTopicDescription pulsarTopicDescription = schemaTableNameToTopicMap.get(pulsarTableHandle.toSchemaTableName());
        if (pulsarTopicDescription == null) {
            throw new TableNotFoundException(pulsarTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        schemaTableNameToColumnMetadataMap.forEach(new BiConsumer<SchemaTableName, List<ColumnMetadata>>() {
            @Override
            public void accept(SchemaTableName schemaTableName, List<ColumnMetadata> columnMetadataList) {
                if (pulsarTableHandle.toSchemaTableName().equals(schemaTableName)) {
                    for (ColumnMetadata columnMetadata : columnMetadataList) {

                        PulsarColumnHandle pulsarColumnHandle = new PulsarColumnHandle(
                                connectorId,
                                columnMetadata.getName(),
                                columnMetadata.getType(),
                                false,
                                false);
                        log.info("using connectorId: %s", connectorId);
                        log.info("setting pulsarColumnHandle: %s", pulsarColumnHandle);

                        columnHandles.put(
                                columnMetadata.getName(),
                                pulsarColumnHandle);
                    }
                }
            }
        });

        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        log.info("getColumnMetadata: %s - %s", tableHandle, columnHandle);

        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        log.info("listTableColumns: %s", prefix);

        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames;
        if (prefix.getTableName() == null) {
            tableNames = listTables(session, prefix.getSchemaName());
        }
        else {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }

        for (SchemaTableName tableName : tableNames) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (TableNotFoundException e) {

            }
        }

        return columns.build();
    }
}
