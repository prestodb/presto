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

import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.spi.ServerInfo;
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
    private final boolean allowCorruptWritesForTesting;
    private final boolean respectTableFormat;
    private final boolean bucketExecutionEnabled;
    private final boolean bucketWritingEnabled;
    private final boolean forceIntegralToBigint;
    private final HiveStorageFormat defaultStorageFormat;
    private final ExtendedHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final TableParameterCodec tableParameterCodec;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final BoundedExecutor renameExecution;
    private final TypeTranslator typeTranslator;
    private final ServerInfo serverInfo;

    @Inject
    @SuppressWarnings("deprecation")
    public HiveMetadataFactory(
            HiveConnectorId connectorId,
            HiveClientConfig hiveClientConfig,
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            @ForHiveClient ExecutorService executorService,
            TypeManager typeManager,
            LocationService locationService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            TypeTranslator typeTranslator,
            ServerInfo serverInfo)
    {
        this(connectorId,
                metastore,
                hdfsEnvironment,
                partitionManager,
                hiveClientConfig.getDateTimeZone(),
                hiveClientConfig.getMaxConcurrentFileRenames(),
                hiveClientConfig.getAllowCorruptWritesForTesting(),
                hiveClientConfig.isRespectTableFormat(),
                hiveClientConfig.isBucketExecutionEnabled(),
                hiveClientConfig.isBucketWritingEnabled(),
                hiveClientConfig.isForceIntegralToBigint(),
                hiveClientConfig.getHiveStorageFormat(),
                typeManager,
                locationService,
                tableParameterCodec,
                partitionUpdateCodec,
                executorService,
                typeTranslator,
                serverInfo);
    }

    public HiveMetadataFactory(
            HiveConnectorId connectorId,
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            DateTimeZone timeZone,
            int maxConcurrentFileRenames,
            boolean allowCorruptWritesForTesting,
            boolean respectTableFormat,
            boolean bucketExecutionEnabled,
            boolean bucketWritingEnabled,
            boolean forceIntegralToBigint,
            HiveStorageFormat defaultStorageFormat,
            TypeManager typeManager,
            LocationService locationService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ExecutorService executorService,
            TypeTranslator typeTranslator,
            ServerInfo serverInfo)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();

        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;
        this.respectTableFormat = respectTableFormat;
        this.bucketExecutionEnabled = bucketExecutionEnabled;
        this.bucketWritingEnabled = bucketWritingEnabled;
        this.forceIntegralToBigint = forceIntegralToBigint;
        this.defaultStorageFormat = requireNonNull(defaultStorageFormat, "defaultStorageFormat is null");

        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.tableParameterCodec = requireNonNull(tableParameterCodec, "tableParameterCodec is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
        this.serverInfo = requireNonNull(serverInfo, "serverInfo is null");

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
                allowCorruptWritesForTesting,
                respectTableFormat,
                bucketExecutionEnabled,
                bucketWritingEnabled,
                forceIntegralToBigint,
                defaultStorageFormat,
                typeManager,
                locationService,
                tableParameterCodec,
                partitionUpdateCodec,
                renameExecution,
                typeTranslator,
                serverInfo);
    }
}
