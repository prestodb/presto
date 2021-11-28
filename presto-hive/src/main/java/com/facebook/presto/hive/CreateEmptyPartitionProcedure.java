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
package com.facebook.presto.hive;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.presto.hive.LocationService.WriteInfo;
import com.facebook.presto.hive.PartitionUpdate.UpdateMode;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.apache.hadoop.hive.common.FileUtils;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.hive.HiveSessionProperties.isOptimizedPartitionUpdateSerializationEnabled;
import static com.facebook.presto.hive.HiveUtil.serializeZstdCompressed;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CreateEmptyPartitionProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle CREATE_EMPTY_PARTITION = methodHandle(
            CreateEmptyPartitionProcedure.class,
            "createEmptyPartition",
            ConnectorSession.class,
            String.class,
            String.class,
            List.class,
            List.class);

    private final Supplier<TransactionalMetadata> hiveMetadataFactory;
    private final ExtendedHiveMetastore metastore;
    private final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateJsonCodec;
    private final SmileCodec<PartitionUpdate> partitionUpdateSmileCodec;

    @Inject
    public CreateEmptyPartitionProcedure(
            Supplier<TransactionalMetadata> hiveMetadataFactory,
            ExtendedHiveMetastore metastore,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            SmileCodec<PartitionUpdate> partitionUpdateSmileCodec)
    {
        this.hiveMetadataFactory = requireNonNull(hiveMetadataFactory, "hiveMetadataFactory is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateJsonCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.partitionUpdateSmileCodec = requireNonNull(partitionUpdateSmileCodec, "partitionUpdateSmileCodec is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "create_empty_partition",
                ImmutableList.of(
                        new Argument("schema_name", VARCHAR),
                        new Argument("table_name", VARCHAR),
                        new Argument("partition_columns", "array(varchar)"),
                        new Argument("partition_values", "array(varchar)")),
                CREATE_EMPTY_PARTITION.bindTo(this));
    }

    public void createEmptyPartition(ConnectorSession session, String schema, String table, List<Object> partitionColumnNames, List<Object> partitionValues)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doCreateEmptyPartition(session, schema, table, partitionColumnNames, partitionValues);
        }
    }

    private void doCreateEmptyPartition(ConnectorSession session, String schema, String table, List<Object> partitionColumnNames, List<Object> partitionValues)
    {
        TransactionalMetadata hiveMetadata = hiveMetadataFactory.get();

        HiveInsertTableHandle hiveInsertTableHandle = (HiveInsertTableHandle) hiveMetadata.beginInsert(session, new HiveTableHandle(schema, table));

        List<String> actualPartitionColumnNames = hiveInsertTableHandle.getInputColumns().stream()
                .filter(HiveColumnHandle::isPartitionKey)
                .map(HiveColumnHandle::getName)
                .collect(toImmutableList());
        if (!Objects.equals(partitionColumnNames, actualPartitionColumnNames)) {
            throw new PrestoException(INVALID_PROCEDURE_ARGUMENT, "input partition column names doesn't match actual partition column names");
        }

        List<String> partitionStringValues = partitionValues.stream()
                .map(String.class::cast)
                .collect(toImmutableList());

        if (metastore.getPartition(new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), getMetastoreHeaders(session)), schema, table, partitionStringValues).isPresent()) {
            throw new PrestoException(ALREADY_EXISTS, "Partition already exists");
        }
        String partitionName = FileUtils.makePartName(actualPartitionColumnNames, partitionStringValues);

        WriteInfo writeInfo = locationService.getPartitionWriteInfo(hiveInsertTableHandle.getLocationHandle(), Optional.empty(), partitionName);
        PartitionUpdate partitionUpdate = new PartitionUpdate(
                partitionName,
                UpdateMode.NEW,
                writeInfo.getWritePath(),
                writeInfo.getTargetPath(),
                ImmutableList.of(),
                0,
                0,
                0,
                writeInfo.getWritePath().getName().matches("\\d+"));
        byte[] serializedPartitionUpdate;
        if (isOptimizedPartitionUpdateSerializationEnabled(session)) {
            serializedPartitionUpdate = serializeZstdCompressed(partitionUpdateSmileCodec, partitionUpdate);
        }
        else {
            serializedPartitionUpdate = partitionUpdateJsonCodec.toJsonBytes(partitionUpdate);
        }

        hiveMetadata.finishInsert(
                session,
                hiveInsertTableHandle,
                ImmutableList.of(Slices.wrappedBuffer(serializedPartitionUpdate)),
                ImmutableList.of());
        hiveMetadata.commit();
    }
}
