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
package com.facebook.presto.kinesis;

import static com.google.common.base.Preconditions.checkNotNull;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.facebook.presto.kinesis.decoder.dummy.DummyKinesisRowDecoder;
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
import com.google.inject.Inject;
import com.google.inject.name.Named;

public class KinesisMetadata
        extends ReadOnlyConnectorMetadata
{
    private static final Logger log = Logger.get(KinesisMetadata.class);

    private final String connectorId;
    private final KinesisConnectorConfig kinesisConnectorConfig;
    private final KinesisHandleResolver handleResolver;

    private final Supplier<Map<SchemaTableName, KinesisStreamDescription>> kinesisTableDescriptionSupplier;
    private final Set<KinesisInternalFieldDescription> internalFieldDescriptions;

    @Inject
    KinesisMetadata(@Named("connectorId") String connectorId,
            KinesisConnectorConfig kinesisConnectorConfig,
            KinesisHandleResolver handleResolver,
            Supplier<Map<SchemaTableName, KinesisStreamDescription>> kinesisTableDescriptionSupplier,
            Set<KinesisInternalFieldDescription> internalFieldDescriptions)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.kinesisConnectorConfig = checkNotNull(kinesisConnectorConfig, "kinesisConfig is null");
        this.handleResolver = checkNotNull(handleResolver, "handleResolver is null");

        log.debug("Loading kinesis table definitions from %s", kinesisConnectorConfig.getTableDescriptionDir().getAbsolutePath());

        this.kinesisTableDescriptionSupplier = Suppliers.memoize(kinesisTableDescriptionSupplier);
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
    public KinesisTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        KinesisStreamDescription table = getDefinedTables().get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        return new KinesisTableHandle(connectorId,
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                table.getStreamName(),
                getDataFormat(table.getMessage()));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorTableHandle tableHandle)
    {
        KinesisTableHandle kinesisTableHandle = handleResolver.convertTableHandle(tableHandle);
        return getTableMetadata(kinesisTableHandle.toSchemaTableName());
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
        KinesisTableHandle kinesisTableHandle = handleResolver.convertTableHandle(tableHandle);

        KinesisStreamDescription kinesisStreamDescription = getDefinedTables().get(kinesisTableHandle.toSchemaTableName());
        if (kinesisStreamDescription == null) {
            throw new TableNotFoundException(kinesisTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        int index = 0;
       /* KinesisStreamFieldGroup key = kinesisStreamDescription.getPartitionKey();
        if (key != null) {
            List<KinesisStreamFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (KinesisStreamFieldDescription kinesisStreamFieldDescription : fields) {
                    columnHandles.put(kinesisStreamFieldDescription.getName(), kinesisStreamFieldDescription.getColumnHandle(connectorId, true, index++));
                }
            }
        }*/

        KinesisStreamFieldGroup message = kinesisStreamDescription.getMessage();
        if (message != null) {
            List<KinesisStreamFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KinesisStreamFieldDescription kinesisStreamFieldDescription : fields) {
                    columnHandles.put(kinesisStreamFieldDescription.getName(), kinesisStreamFieldDescription.getColumnHandle(connectorId, index++));
                }
            }
        }

        for (KinesisInternalFieldDescription kinesisInternalFieldDescription : internalFieldDescriptions) {
            columnHandles.put(kinesisInternalFieldDescription.getName(), kinesisInternalFieldDescription.getColumnHandle(connectorId, index++, kinesisConnectorConfig.isHideInternalColumns()));
        }

        return columnHandles.build();
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        handleResolver.convertTableHandle(tableHandle);
        KinesisColumnHandle kinesisColumnHandle = handleResolver.convertColumnHandle(columnHandle);

        return kinesisColumnHandle.getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        // what if prefix.getTableName == null
        List<SchemaTableName> tableNames = prefix.getSchemaName() == null ? listTables(session, null) : ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private static String getDataFormat(KinesisStreamFieldGroup fieldGroup)
    {
        return (fieldGroup == null) ? DummyKinesisRowDecoder.NAME : fieldGroup.getDataFormat();
    }

    @VisibleForTesting
    Map<SchemaTableName, KinesisStreamDescription> getDefinedTables()
    {
        return kinesisTableDescriptionSupplier.get();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        KinesisStreamDescription table = getDefinedTables().get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        int index = 0;

        /*KinesisStreamFieldGroup key = table.getPartitionKey();
        if (key != null) {
            List<KinesisStreamFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (KinesisStreamFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata(index++));
                }
            }
        }*/

        KinesisStreamFieldGroup message = table.getMessage();
        if (message != null) {
            List<KinesisStreamFieldDescription> fields = message.getFields();
            if (fields != null) {
                for (KinesisStreamFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata(index++));
                }
            }
        }

        for (KinesisInternalFieldDescription fieldDescription : internalFieldDescriptions) {
            builder.add(fieldDescription.getColumnMetadata(index++, kinesisConnectorConfig.isHideInternalColumns()));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }
}
