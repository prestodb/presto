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
package com.facebook.presto.kafka;

import com.facebook.presto.kafka.decoder.dummy.DummyKafkaRowDecoder;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ReadOnlyConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.name.Named;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages the Kafka connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns. See {@link KafkaInternalFieldDescription} for a list
 * of per-topic additional columns.
 */
public class KafkaMetadata
        extends ReadOnlyConnectorMetadata
{
    private static final Logger log = Logger.get(KafkaMetadata.class);

    private final String connectorId;
    private final KafkaConnectorConfig kafkaConnectorConfig;
    private final KafkaHandleResolver handleResolver;

    private final Supplier<Map<SchemaTableName, KafkaTopicDescription>> kafkaTableDescriptionSupplier;
    private final Set<KafkaInternalFieldDescription> internalFieldDescriptions;

    @Inject
    KafkaMetadata(@Named("connectorId") String connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaHandleResolver handleResolver,
            Supplier<Map<SchemaTableName, KafkaTopicDescription>> kafkaTableDescriptionSupplier,
            Set<KafkaInternalFieldDescription> internalFieldDescriptions)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.kafkaConnectorConfig = checkNotNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");

        log.debug("Loading kafka table definitions from %s", kafkaConnectorConfig.getTableDescriptionDir().getAbsolutePath());

        this.kafkaTableDescriptionSupplier = Suppliers.memoize(kafkaTableDescriptionSupplier);
        this.internalFieldDescriptions = checkNotNull(internalFieldDescriptions, "internalFieldDescriptions is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (SchemaTableName tableName : getDefinedTables().keySet()) {
            builder.add(tableName.getSchemaName());
        }
        return ImmutableList.copyOf(builder.build());
    }

    @Override
    public KafkaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        KafkaTopicDescription table = getDefinedTables().get(schemaTableName);
        if (table == null) {
            return null;
        }

        return new KafkaTableHandle(connectorId,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                table.getTopicName(),
                getDataFormat(table.getKey()),
                getDataFormat(table.getMessage()));
    }

    private static String getDataFormat(KafkaTopicFieldGroup fieldGroup)
    {
        return (fieldGroup == null) ? DummyKafkaRowDecoder.NAME : fieldGroup.getDataFormat();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        KafkaTableHandle kafkaTableHandle = handleResolver.convertTableHandle(tableHandle);
        return getTableMetadata(kafkaTableHandle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : getDefinedTables().keySet()) {
            if (schemaNameOrNull == null || tableName.getSchemaName().equals(schemaNameOrNull)) {
                builder.add(tableName);
            }
        }

        return builder.build();
    }

    @Override
    public ColumnHandle getSampleWeightColumnHandle(ConnectorTableHandle tableHandle)
    {
        return null;
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorTableHandle tableHandle)
    {
        KafkaTableHandle kafkaTableHandle = handleResolver.convertTableHandle(tableHandle);

        KafkaTopicDescription kafkaTopicDescription = getDefinedTables().get(kafkaTableHandle.toSchemaTableName());
        if (kafkaTopicDescription == null) {
            throw new TableNotFoundException(kafkaTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        int index = 0;
        KafkaTopicFieldGroup key = kafkaTopicDescription.getKey();
        if (key != null) {
            List<KafkaTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription kafkaTopicFieldDescription : fields) {
                    columnHandles.put(kafkaTopicFieldDescription.getName(), kafkaTopicFieldDescription.getColumnHandle(connectorId, true, index++));
                }
            }
        }

        KafkaTopicFieldGroup message = kafkaTopicDescription.getMessage();
        if (message != null) {
            List<KafkaTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription kafkaTopicFieldDescription : fields) {
                    columnHandles.put(kafkaTopicFieldDescription.getName(), kafkaTopicFieldDescription.getColumnHandle(connectorId, false, index++));
                }
            }
        }

        for (KafkaInternalFieldDescription kafkaInternalFieldDescription : internalFieldDescriptions) {
            columnHandles.put(kafkaInternalFieldDescription.getName(), kafkaInternalFieldDescription.getColumnHandle(connectorId, index++, kafkaConnectorConfig.isHideInternalColumns()));
        }

        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames = prefix.getSchemaName() == null ? listTables(session, null) : ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        handleResolver.convertTableHandle(tableHandle);
        KafkaColumnHandle kafkaColumnHandle = handleResolver.convertColumnHandle(columnHandle);

        return kafkaColumnHandle.getColumnMetadata();
    }

    @VisibleForTesting
    Map<SchemaTableName, KafkaTopicDescription> getDefinedTables()
    {
        return kafkaTableDescriptionSupplier.get();
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        KafkaTopicDescription table = getDefinedTables().get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        KafkaTopicFieldGroup key = table.getKey();
        if (key != null) {
            List<KafkaTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        KafkaTopicFieldGroup message = table.getMessage();
        if (message != null) {
            List<KafkaTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }

        for (KafkaInternalFieldDescription fieldDescription : internalFieldDescriptions) {
            builder.add(fieldDescription.getColumnMetadata(kafkaConnectorConfig.isHideInternalColumns()));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }
}
