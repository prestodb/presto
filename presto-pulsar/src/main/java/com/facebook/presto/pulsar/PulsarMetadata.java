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
import com.facebook.presto.spi.PrestoException;
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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.facebook.presto.pulsar.PulsarHandleResolver.convertColumnHandle;
import static com.facebook.presto.pulsar.PulsarHandleResolver.convertTableHandle;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static java.util.Objects.requireNonNull;

public class PulsarMetadata implements ConnectorMetadata {

    private final String connectorId;
    private final PulsarAdmin pulsarAdmin;

    private static final Logger log = Logger.get(PulsarMetadata.class);

    @Inject
    public PulsarMetadata(PulsarConnectorId connectorId, PulsarConnectorConfig pulsarConnectorConfig) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        try {
            this.pulsarAdmin = pulsarConnectorConfig.getPulsarAdmin();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        List<String> prestoSchemas = new LinkedList<>();
        try {
            List<String> tenants = pulsarAdmin.tenants().getTenants();
            for (String tenant : tenants) {
                prestoSchemas.addAll(pulsarAdmin.namespaces().getNamespaces(tenant));
            }
        } catch (PulsarAdminException e) {
            throw new RuntimeException("Failed to get schemas from pulsar", e);
        }
        return prestoSchemas;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return new PulsarTableHandle(
                this.connectorId,
                tableName.getSchemaName(),
                tableName.getTableName(),
                tableName.getTableName());
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table,
                                                            Constraint<ColumnHandle> constraint,
                                                            Optional<Set<ColumnHandle>> desiredColumns) {
        PulsarTableHandle handle = convertTableHandle(table);
        ConnectorTableLayout layout = new ConnectorTableLayout(new PulsarTableLayoutHandle(handle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        return getTableMetadata(convertTableHandle(table).toSchemaTableName(), true);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();

        if (schemaNameOrNull != null) {
            List<String> pulsarTopicList = null;
            try {
                pulsarTopicList = this.pulsarAdmin.topics().getList(schemaNameOrNull);
            } catch (PulsarAdminException e) {
                if (e.getStatusCode() == 404) {
                    log.warn("Schema " + schemaNameOrNull + " does not exsit");
                    return builder.build();
                }
                throw new RuntimeException("Failed to get tables/topics in " + schemaNameOrNull, e);
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
        PulsarTableHandle pulsarTableHandle = convertTableHandle(tableHandle);

        ConnectorTableMetadata tableMetaData = getTableMetadata(pulsarTableHandle.toSchemaTableName(), false);

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        tableMetaData.getColumns().forEach(new Consumer<ColumnMetadata>() {
            @Override
            public void accept(ColumnMetadata columnMetadata) {

                PulsarColumnMetadata pulsarColumnMetadata = (PulsarColumnMetadata) columnMetadata;

                PulsarColumnHandle pulsarColumnHandle = new PulsarColumnHandle(
                        connectorId,
                        pulsarColumnMetadata.getNameWithCase(),
                        pulsarColumnMetadata.getType(),
                        pulsarColumnMetadata.isHidden(),
                        pulsarColumnMetadata.isInternal(),
                        pulsarColumnMetadata.getPositionIndex());

                columnHandles.put(
                        columnMetadata.getName(),
                        pulsarColumnHandle);
            }
        });

        PulsarInternalColumn.getInternalFields().stream().forEach(new Consumer<PulsarInternalColumn>() {
            @Override
            public void accept(PulsarInternalColumn pulsarInternalColumn) {
                PulsarColumnHandle pulsarColumnHandle = pulsarInternalColumn.getColumnHandle(connectorId, false);
                columnHandles.put(pulsarColumnHandle.getName(), pulsarColumnHandle);
            }
        });

        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle
            columnHandle) {

        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix
            prefix) {

        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames;
        if (prefix.getTableName() == null) {
            tableNames = listTables(session, prefix.getSchemaName());
        } else {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }

        for (SchemaTableName tableName : tableNames) {
            columns.put(tableName, getTableMetadata(tableName, true).getColumns());
        }

        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName, boolean withInternalColumns) {

        TopicName topicName;

        topicName = TopicName.get(
                String.format("%s/%s", schemaTableName.getSchemaName(), schemaTableName.getTableName()));

        List<String> topics;
        try {
            if (!PulsarConnectorUtils.isPartitionedTopic(topicName, this.pulsarAdmin)) {
                topics = this.pulsarAdmin.topics().getList(schemaTableName.getSchemaName());
            } else {
                topics = this.pulsarAdmin.topics().getPartitionedTopicList((schemaTableName.getSchemaName()));
            }
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 404) {
                throw new PrestoException(NOT_FOUND, "Schema " + schemaTableName.getSchemaName() + " does not exist");
            }
            throw new RuntimeException(e);
        }

        if (!topics.contains(topicName.toString())) {
            log.error("Table %s not found",
                    String.format("%s/%s", schemaTableName.getSchemaName(),
                            schemaTableName.getTableName()));
            throw new TableNotFoundException(schemaTableName);
        }

        SchemaInfo schemaInfo;
        try {
            schemaInfo = this.pulsarAdmin.schemas().getSchemaInfo(
                    String.format("%s/%s", schemaTableName.getSchemaName(), schemaTableName.getTableName()));
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 404) {
                throw new PrestoException(NOT_SUPPORTED, "Topic " + topicName.toString() + " does not have a schema");
            }
            throw new RuntimeException(e);
        }

        String schemaJson = new String(schemaInfo.getSchema());
        if (StringUtils.isBlank(schemaJson)) {
            throw new PrestoException(NOT_SUPPORTED, "Topic " + topicName.toString()
                    + " does not have a valid schema");
        }
        Schema schema;
        try {
            schema = PulsarConnectorUtils.parseSchema(schemaJson);
        } catch (SchemaParseException ex) {
            throw new PrestoException(NOT_SUPPORTED, "Topic " + topicName.toString()
                    + " does not have a valid schema");
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        List<Schema.Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Schema.Field field = fields.get(i);
            builder.addAll(getColumns(field.name(), field.schema(), i));
        }
        if (withInternalColumns) {
            PulsarInternalColumn.getInternalFields().stream().forEach(new Consumer<PulsarInternalColumn>() {
                @Override
                public void accept(PulsarInternalColumn pulsarInternalColumn) {
                    builder.add(pulsarInternalColumn.getColumnMetadata(false));
                }
            });
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    private List<PulsarColumnMetadata> getColumns(String name, Schema fieldSchema, int index) {

        List<PulsarColumnMetadata> columnMetadataList = new LinkedList<>();

        if (isPrimitiveType(fieldSchema.getType())) {
            columnMetadataList.add(new PulsarColumnMetadata(name,
                    convertType(fieldSchema.getType(), fieldSchema.getLogicalType()),
                    null, null, false, false, index));
        } else if (fieldSchema.getType() == Schema.Type.UNION) {
            boolean canBeNull = false;
            for (Schema type : fieldSchema.getTypes()) {
                if (isPrimitiveType(type.getType())) {
                    PulsarColumnMetadata columnMetadata;
                    if (type.getType() != Schema.Type.NULL) {
                        if (!canBeNull) {
                            columnMetadata = new PulsarColumnMetadata(name,
                                    convertType(type.getType(), type.getLogicalType()),
                                    null, null, false, false, index);
                        } else {
                            columnMetadata = new PulsarColumnMetadata(name,
                                    convertType(type.getType(), type.getLogicalType()),
                                    "field can be null", null, false, false, index);
                        }
                        columnMetadataList.add(columnMetadata);
                    } else {
                        canBeNull = true;
                    }
                }
            }
        } else if (fieldSchema.getType() == Schema.Type.RECORD) {

        } else if (fieldSchema.getType() == Schema.Type.ARRAY) {

        } else if (fieldSchema.getType() == Schema.Type.MAP) {

        } else if (fieldSchema.getType() == Schema.Type.ENUM) {

        } else if (fieldSchema.getType() == Schema.Type.FIXED) {

        } else {
            log.error("Unknown column type: {}", fieldSchema);
        }
        return columnMetadataList;
    }

    @VisibleForTesting
    static Type convertType(Schema.Type avroType, LogicalType logicalType) {
        switch (avroType) {
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case INT:
                if (logicalType == LogicalTypes.timeMillis()) {
                    return TIME;
                } else if (logicalType == LogicalTypes.date()) {
                    return DATE;
                }
                return IntegerType.INTEGER;
            case LONG:
                if (logicalType == LogicalTypes.timestampMillis()) {
                    return TIMESTAMP;
                }
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
                log.error("Cannot convert type: %s", avroType);
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
}
