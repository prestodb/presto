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

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.type.TypeManager;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class HiveMetadataFactory
{
    private static final Logger log = Logger.get(HiveMetadataFactory.class);

    private final String connectorId;
    private final boolean allowDropTable;
    private final boolean allowRenameTable;
    private final boolean allowAddColumn;
    private final boolean allowRenameColumn;
    private final boolean allowCorruptWritesForTesting;
    private final boolean respectTableFormat;
    private final HiveStorageFormat defaultStorageFormat;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final TableParameterCodec tableParameterCodec;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final BoundedExecutor renameExecution;

    @Inject
    @SuppressWarnings("deprecation")
    public HiveMetadataFactory(
            HiveConnectorId connectorId,
            HiveClientConfig hiveClientConfig,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            @ForHiveClient ExecutorService executorService,
            TypeManager typeManager,
            LocationService locationService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec)
    {
        this(connectorId,
                metastore,
                hdfsEnvironment,
                partitionManager,
                hiveClientConfig.getDateTimeZone(),
                hiveClientConfig.getMaxConcurrentFileRenames(),
                hiveClientConfig.getAllowDropTable(),
                hiveClientConfig.getAllowRenameTable(),
                hiveClientConfig.getAllowAddColumn(),
                hiveClientConfig.getAllowRenameColumn(),
                hiveClientConfig.getAllowCorruptWritesForTesting(),
                hiveClientConfig.isRespectTableFormat(),
                hiveClientConfig.getHiveStorageFormat(),
                typeManager,
                locationService,
                tableParameterCodec,
                partitionUpdateCodec,
                executorService);
    }

    public HiveMetadataFactory(
            HiveConnectorId connectorId,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            DateTimeZone timeZone,
            int maxConcurrentFileRenames,
            boolean allowDropTable,
            boolean allowRenameTable,
            boolean allowAddColumn,
            boolean allowRenameColumn,
            boolean allowCorruptWritesForTesting,
            boolean respectTableFormat,
            HiveStorageFormat defaultStorageFormat,
            TypeManager typeManager,
            LocationService locationService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ExecutorService executorService)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();

        this.allowDropTable = allowDropTable;
        this.allowRenameTable = allowRenameTable;
        this.allowAddColumn = allowAddColumn;
        this.allowRenameColumn = allowRenameColumn;
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;
        this.respectTableFormat = respectTableFormat;
        this.defaultStorageFormat = requireNonNull(defaultStorageFormat, "defaultStorageFormat is null");

        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.tableParameterCodec = requireNonNull(tableParameterCodec, "tableParameterCodec is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");

        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            log.warn("Hive writes are disabled. " +
                            "To write data to Hive, your JVM timezone must match the Hive storage timezone. " +
                            "Add -Duser.timezone=%s to your JVM arguments",
                    timeZone.getID());
        }

        renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);
    }

    public HiveMetadata create()
    {
        return new HiveMetadata(
                connectorId,
                metastore,
                hdfsEnvironment,
                partitionManager,
                timeZone,
                allowDropTable,
                allowRenameTable,
                allowAddColumn,
                allowRenameColumn,
                allowCorruptWritesForTesting,
                respectTableFormat,
                defaultStorageFormat,
                typeManager,
                locationService,
                tableParameterCodec,
                partitionUpdateCodec,
                renameExecution);
    }
}
