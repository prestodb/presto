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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.kafka.schema.TableDescriptionSupplier;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
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
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import jakarta.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.kafka.KafkaHandleResolver.convertColumnHandle;
import static com.facebook.presto.kafka.KafkaHandleResolver.convertTableHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Manages the Kafka connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns. See {@link KafkaInternalFieldDescription} for a list
 * of per-topic additional columns.
 */
public class KafkaMetadata
        implements ConnectorMetadata
{
    private final String connectorId;
    private final boolean hideInternalColumns;
    private final TableDescriptionSupplier tableDescriptionSupplier;

    @Inject
    public KafkaMetadata(
            KafkaConnectorId connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            TableDescriptionSupplier tableDescriptionSupplier)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.hideInternalColumns = kafkaConnectorConfig.isHideInternalColumns();
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return tableDescriptionSupplier.listTables().stream()
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    @Override
    public KafkaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return getTopicDescription(schemaTableName)
                .map(kafkaTopicDescription -> new KafkaTableHandle(
                        connectorId,
                        schemaTableName.getSchemaName(),
                        schemaTableName.getTableName(),
                        kafkaTopicDescription.getTopicName(),
                        getDataFormat(kafkaTopicDescription.getKey()),
                        getDataFormat(kafkaTopicDescription.getMessage()),
                        kafkaTopicDescription.getKey().flatMap(KafkaTopicFieldGroup::getDataSchema),
                        kafkaTopicDescription.getMessage().flatMap(KafkaTopicFieldGroup::getDataSchema),
                        getColumnHandles(schemaTableName).values().stream()
                                .map(KafkaColumnHandle.class::cast)
                                .collect(toImmutableList())))
                .orElse(null);
    }

    private static String getDataFormat(Optional<KafkaTopicFieldGroup> fieldGroup)
    {
        return fieldGroup.map(KafkaTopicFieldGroup::getDataFormat).orElse(DummyRowDecoder.NAME);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(session, convertTableHandle(tableHandle).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return tableDescriptionSupplier.listTables().stream()
                .filter(tableName -> schemaName.map(tableName.getSchemaName()::equals).orElse(true))
                .collect(toImmutableList());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KafkaTableHandle kafkaTableHandle = convertTableHandle(tableHandle);
        return getColumnHandles(kafkaTableHandle.toSchemaTableName());
    }

    private Map<String, ColumnHandle> getColumnHandles(SchemaTableName schemaTableName)
    {
        KafkaTopicDescription kafkaTopicDescription = getRequiredTopicDescription(schemaTableName);

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        AtomicInteger index = new AtomicInteger(0);

        kafkaTopicDescription.getKey().ifPresent(key ->
        {
            List<KafkaTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription kafkaTopicFieldDescription : fields) {
                    columnHandles.put(kafkaTopicFieldDescription.getName(), kafkaTopicFieldDescription.getColumnHandle(connectorId, true, index.getAndIncrement()));
                }
            }
        });

        kafkaTopicDescription.getMessage().ifPresent(message ->
        {
            List<KafkaTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription kafkaTopicFieldDescription : fields) {
                    columnHandles.put(kafkaTopicFieldDescription.getName(), kafkaTopicFieldDescription.getColumnHandle(connectorId, false, index.getAndIncrement()));
                }
            }
        });

        for (KafkaInternalFieldDescription kafkaInternalFieldDescription : KafkaInternalFieldDescription.values()) {
            columnHandles.put(kafkaInternalFieldDescription.getColumnName(), kafkaInternalFieldDescription.getColumnHandle(connectorId, index.getAndIncrement(), hideInternalColumns));
        }

        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
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
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            }
            catch (TableNotFoundException e) {
                // Normally it would mean the table disappeared during listing operation
                throw new IllegalStateException(format("Table %s cannot be gone because tables are statically defined", tableName), e);
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @Override
    public ConnectorTableLayoutResult getTableLayoutForConstraint(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        KafkaTableHandle handle = convertTableHandle(table);
        long startTimestamp = 0;
        long endTimestamp = 0;
        Optional<Map<ColumnHandle, Domain>> domains = constraint.getSummary().getDomains();
        if (domains.isPresent()) {
            Map<ColumnHandle, Domain> columnHandleDomainMap = domains.get();
            for (Map.Entry<ColumnHandle, Domain> entry : columnHandleDomainMap.entrySet()) {
                if (entry.getKey() instanceof KafkaColumnHandle && ((KafkaColumnHandle) entry.getKey()).getName().equals(KafkaInternalFieldDescription.OFFSET_TIMESTAMP_FIELD.getColumnName())) {
                    Range span = entry.getValue().getValues().getRanges().getSpan();
                    Marker low = span.getLow();
                    Marker high = span.getHigh();
                    if (!low.isLowerUnbounded()) {
                        startTimestamp = (long) low.getValue();
                    }
                    if (!high.isUpperUnbounded()) {
                        endTimestamp = (long) high.getValue();
                    }
                }
            }
        }

        ConnectorTableLayout layout = new ConnectorTableLayout(new KafkaTableLayoutHandle(handle, startTimestamp, endTimestamp));
        return new ConnectorTableLayoutResult(layout, constraint.getSummary());
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName schemaTableName)
    {
        KafkaTopicDescription table = getRequiredTopicDescription(schemaTableName);

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        table.getKey().ifPresent(key -> {
            List<KafkaTopicFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata(normalizeIdentifier(session, fieldDescription.getName())));
                }
            }
        });

        table.getMessage().ifPresent(message -> {
            List<KafkaTopicFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KafkaTopicFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata(normalizeIdentifier(session, fieldDescription.getName())));
                }
            }
        });

        for (KafkaInternalFieldDescription fieldDescription : KafkaInternalFieldDescription.values()) {
            builder.add(fieldDescription.getColumnMetadata(hideInternalColumns, normalizeIdentifier(session, fieldDescription.getColumnName())));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // TODO: support transactional inserts
        KafkaTableHandle table = (KafkaTableHandle) tableHandle;
        List<KafkaColumnHandle> actualColumns = table.getColumns().stream()
                .filter(col -> !col.isInternal())
                .collect(toImmutableList());

        return new KafkaTableHandle(
                connectorId,
                table.getSchemaName(),
                table.getTableName(),
                table.getTopicName(),
                table.getKeyDataFormat(),
                table.getMessageDataFormat(),
                table.getKeyDataSchemaLocation(),
                table.getMessageDataSchemaLocation(),
                actualColumns);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        // TODO: support transactional inserts
        return Optional.empty();
    }

    private KafkaTopicDescription getRequiredTopicDescription(SchemaTableName schemaTableName)
    {
        return getTopicDescription(schemaTableName).orElseThrow(() -> new TableNotFoundException(schemaTableName));
    }

    private Optional<KafkaTopicDescription> getTopicDescription(SchemaTableName schemaTableName)
    {
        return tableDescriptionSupplier.getTopicDescription(schemaTableName);
    }
}
