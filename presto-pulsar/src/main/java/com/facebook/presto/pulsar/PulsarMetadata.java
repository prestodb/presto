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
package com.facebook.presto.pulsar;

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
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.shade.org.apache.avro.Schema;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.facebook.presto.pulsar.PulsarHandleResolver.convertColumnHandle;
import static com.facebook.presto.pulsar.PulsarHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

public class PulsarMetadata implements ConnectorMetadata {

//    private static Map<String, List<SchemaTableName>> schemaToTableMap = new HashMap<>();
//    static {
//        schemaToTableMap.put("schema1", Arrays.asList(new SchemaTableName("schema1", "schema1_table1"), new SchemaTableName("schema1", "schema1_table2")));
//        schemaToTableMap.put("schema2", Arrays.asList(new SchemaTableName("schema2", "schema1_table1")));
//    }
//
//    private static Map<SchemaTableName, PulsarTopicDescription> schemaTableNameToTopicMap = new HashMap<>();
//    static {
//        schemaTableNameToTopicMap.put(new SchemaTableName("schema1", "schema1_table1"), new PulsarTopicDescription("schema1_table1", "schema1", "schema1_table1_topic"));
//        schemaTableNameToTopicMap.put(new SchemaTableName("schema1", "schema1_table2"), new PulsarTopicDescription("schema1_table2", "schema1", "schema1_table2_topic"));
//        schemaTableNameToTopicMap.put(new SchemaTableName("schema2", "schema2_table1"), new PulsarTopicDescription("schema2_table1", "schema1", "schema2_table1_topic"));
//    }
//
//    private static Map<SchemaTableName, List<ColumnMetadata>> schemaTableNameToColumnMetadataMap = new HashMap<>();
//    static {
//        schemaTableNameToColumnMetadataMap.put(new SchemaTableName("schema1", "schema1_table1"), Arrays.asList(new ColumnMetadata("id", IntegerType.INTEGER), new ColumnMetadata("field1", VarcharType.VARCHAR)));
//        schemaTableNameToColumnMetadataMap.put(new SchemaTableName("schema1", "schema1_table2"), Arrays.asList(new ColumnMetadata("id", IntegerType.INTEGER), new ColumnMetadata("field1", VarcharType.VARCHAR)));
//        schemaTableNameToColumnMetadataMap.put(new SchemaTableName("schema2", "schema2_table1"), Arrays.asList(new ColumnMetadata("id", IntegerType.INTEGER), new ColumnMetadata("field1", VarcharType.VARCHAR), new ColumnMetadata("field2", VarcharType.VARCHAR)));
//    }


    private final String connectorId;
    private final PulsarAdmin pulsarAdmin;

    private static final Logger log = Logger.get(PulsarMetadata.class);

    @Inject
    public PulsarMetadata(PulsarConnectorId connectorId, PulsarConnectorConfig pulsarConnectorConfig) {
        log.info("connectorId: %s", connectorId);
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        try {
            this.pulsarAdmin = pulsarConnectorConfig.getPulsarAdmin();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        log.info("pulsarConnectorConfig: %s", pulsarConnectorConfig);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        log.info("listSchemaNames...");
        List<String> prestoSchemas = new LinkedList<>();
        try {
            List<String> tenants = pulsarAdmin.tenants().getTenants();
            for (String tenant : tenants) {
               prestoSchemas.addAll(pulsarAdmin.namespaces().getNamespaces(tenant));
            }
        } catch (PulsarAdminException e) {
            log.error(e, "Failed to get schemas from pulsar");
            throw new RuntimeException(e);
        }
        return new LinkedList<>(prestoSchemas);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        log.info("getTableHandle: %s", tableName);

        return new PulsarTableHandle(
                this.connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                tableName.getTableName());
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

        List<String> topics;
        try {
            topics = this.pulsarAdmin.topics().getList(schemaTableName.getSchemaName());
            log.info("topics: %s", topics);
            TopicName topicName = TopicName.get(
                    String.format("%s/%s", schemaTableName.getSchemaName(), schemaTableName.getTableName()));
            log.info("topicName.getSchemaName(): %s", topicName.toString());
            if (!topics.contains(topicName.toString())) {
                log.error("table not found");
                throw new TableNotFoundException(schemaTableName);
            }
        } catch (PulsarAdminException e) {
           throw new RuntimeException(e);
        }


        SchemaInfo schemaInfo = null;
        try {
            schemaInfo = this.pulsarAdmin.schemas().getSchemaInfo(
                    String.format("%s/%s", schemaTableName.getSchemaName(), schemaTableName.getTableName()));
        } catch (PulsarAdminException e) {
            log.error(e);
            throw new RuntimeException(e);
        }

        log.info("schema: " + new String(schemaInfo.getSchema()));
        Schema.Parser parser = new Schema.Parser();
        String schemaJson = new String(schemaInfo.getSchema());
        Schema schema = parser.parse(schemaJson);


        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        for (Schema.Field field : schema.getFields()) {
            builder.addAll(getColumns(field.name(), field.schema()));
        }
        
        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    public List<ColumnMetadata> getColumns(String name, Schema fieldSchema) {

        List<ColumnMetadata> columnMetadataList = new LinkedList<>();

        if (isPrimitiveType(fieldSchema.getType())) {
            columnMetadataList.add(new ColumnMetadata(name, convertType(fieldSchema.getType())));
        } else if (fieldSchema.getType() == Schema.Type.UNION) {
            boolean canBeNull = false;
            for (Schema type : fieldSchema.getTypes()) {
                if (isPrimitiveType(type.getType())) {
                    ColumnMetadata columnMetadata;
                    if (type.getType() != Schema.Type.NULL) {
                        if (!canBeNull) {
                            columnMetadata = new ColumnMetadata(name, convertType(type.getType()));
                        } else {
                            columnMetadata = new ColumnMetadata(name, convertType(type.getType()), "field can be null", false);
                        }
                        columnMetadataList.add(columnMetadata);
                    } else {
                        canBeNull = true;
                    }
                }
            }
        } else if (fieldSchema.getType() == Schema.Type.RECORD){

        } else if (fieldSchema.getType() == Schema.Type.ARRAY) {

        } else if (fieldSchema.getType() == Schema.Type.MAP) {

        } else if (fieldSchema.getType() == Schema.Type.ENUM) {

        } else if (fieldSchema.getType() == Schema.Type.FIXED) {

        } else {
            log.error("unknown type: {}", fieldSchema);
        }
        return columnMetadataList;
    }

    private Type convertType(Schema.Type avroType) {

        switch (avroType) {
//            case NULL:
//                break;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case BYTES:
                return VarbinaryType.VARBINARY;
            case STRING:
                return VarcharType.VARCHAR;
            default:
                log.error("cannot convert type: %s", avroType);
                return null;
        }
    }

    private boolean isPrimitiveType(Schema.Type type) {
        return Schema.Type.NULL == type
                || Schema.Type.BOOLEAN == type
                || Schema.Type.INT == type
                || Schema.Type.LONG == type
                || Schema.Type.FLOAT == type
                || Schema.Type.DOUBLE == type
                || Schema.Type.BYTES == type
                || Schema.Type.STRING == type;
    }



    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        log.info("listTables: %s", schemaNameOrNull);

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();

        if (schemaNameOrNull != null) {
            List<String> pulsarTopicList = null;
            try {
                pulsarTopicList = this.pulsarAdmin.topics().getList(schemaNameOrNull);
            } catch (PulsarAdminException e) {
                log.error(e, "Failed to get a list of topics for schema %s", schemaNameOrNull);
            }
            if (pulsarTopicList != null) {
                pulsarTopicList.forEach(topic -> builder.add(
                        new SchemaTableName(schemaNameOrNull, TopicName.get(topic).getLocalName())));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        log.info("getColumnHandles: %s", tableHandle);

        PulsarTableHandle pulsarTableHandle = convertTableHandle(tableHandle);

        ConnectorTableMetadata tableMetaData = getTableMetadata(pulsarTableHandle.toSchemaTableName());

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        
        tableMetaData.getColumns().forEach(new Consumer<ColumnMetadata>() {
            @Override
            public void accept(ColumnMetadata columnMetadata) {
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
