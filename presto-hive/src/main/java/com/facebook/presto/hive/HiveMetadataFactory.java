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
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.statistics.MetastoreHiveStatisticsProvider;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class HiveMetadataFactory
        implements Supplier<TransactionalMetadata>
{
    private static final Logger log = Logger.get(HiveMetadataFactory.class);

    private final boolean allowCorruptWritesForTesting;
    private final boolean skipDeletionForAlter;
    private final boolean skipTargetCleanupOnRollback;
    private final boolean undoMetastoreOperationsEnabled;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final int maxPartitionBatchSize;
    private final long perTransactionCacheMaximumSize;
    private final boolean metastoreImpersonationEnabled;
    private final ExtendedHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final StandardFunctionResolution functionResolution;
    private final RowExpressionService rowExpressionService;
    private final FilterStatsCalculatorService filterStatsCalculatorService;
    private final TableParameterCodec tableParameterCodec;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final SmileCodec<PartitionUpdate> partitionUpdateSmileCodec;
    private final ListeningExecutorService fileRenameExecutor;
    private final TypeTranslator typeTranslator;
    private final StagingFileCommitter stagingFileCommitter;
    private final ZeroRowFileCreator zeroRowFileCreator;
    private final String prestoVersion;
    private final PartitionObjectBuilder partitionObjectBuilder;
    private final HiveEncryptionInformationProvider encryptionInformationProvider;
    private final HivePartitionStats hivePartitionStats;
    private final HiveFileRenamer hiveFileRenamer;

    @Inject
    @SuppressWarnings("deprecation")
    public HiveMetadataFactory(
            HiveClientConfig hiveClientConfig,
            MetastoreClientConfig metastoreClientConfig,
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            @ForFileRename ListeningExecutorService fileRenameExecutor,
            TypeManager typeManager,
            LocationService locationService,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            FilterStatsCalculatorService filterStatsCalculatorService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            SmileCodec<PartitionUpdate> partitionUpdateSmileCodec,
            TypeTranslator typeTranslator,
            StagingFileCommitter stagingFileCommitter,
            ZeroRowFileCreator zeroRowFileCreator,
            NodeVersion nodeVersion,
            PartitionObjectBuilder partitionObjectBuilder,
            HiveEncryptionInformationProvider encryptionInformationProvider,
            HivePartitionStats hivePartitionStats,
            HiveFileRenamer hiveFileRenamer)
    {
        this(
                metastore,
                hdfsEnvironment,
                partitionManager,
                hiveClientConfig.getDateTimeZone(),
                hiveClientConfig.getAllowCorruptWritesForTesting(),
                hiveClientConfig.isSkipDeletionForAlter(),
                hiveClientConfig.isSkipTargetCleanupOnRollback(),
                hiveClientConfig.getWritesToNonManagedTablesEnabled(),
                hiveClientConfig.getCreatesOfNonManagedTablesEnabled(),
                hiveClientConfig.isUndoMetastoreOperationsEnabled(),
                hiveClientConfig.getMaxPartitionBatchSize(),
                metastoreClientConfig.getPerTransactionMetastoreCacheMaximumSize(),
                metastoreClientConfig.isMetastoreImpersonationEnabled(),
                typeManager,
                locationService,
                functionResolution,
                rowExpressionService,
                filterStatsCalculatorService,
                tableParameterCodec,
                partitionUpdateCodec,
                partitionUpdateSmileCodec,
                fileRenameExecutor,
                typeTranslator,
                stagingFileCommitter,
                zeroRowFileCreator,
                nodeVersion.toString(),
                partitionObjectBuilder,
                encryptionInformationProvider,
                hivePartitionStats,
                hiveFileRenamer);
    }

    public HiveMetadataFactory(
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            DateTimeZone timeZone,
            boolean allowCorruptWritesForTesting,
            boolean skipDeletionForAlter,
            boolean skipTargetCleanupOnRollback,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean undoMetastoreOperationsEnabled,
            int maxPartitionBatchSize,
            long perTransactionCacheMaximumSize,
            boolean metastoreImpersonationEnabled,
            TypeManager typeManager,
            LocationService locationService,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            FilterStatsCalculatorService filterStatsCalculatorService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            SmileCodec<PartitionUpdate> partitionUpdateSmileCodec,
            ListeningExecutorService fileRenameExecutor,
            TypeTranslator typeTranslator,
            StagingFileCommitter stagingFileCommitter,
            ZeroRowFileCreator zeroRowFileCreator,
            String prestoVersion,
            PartitionObjectBuilder partitionObjectBuilder,
            HiveEncryptionInformationProvider encryptionInformationProvider,
            HivePartitionStats hivePartitionStats,
            HiveFileRenamer hiveFileRenamer)
    {
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;
        this.skipDeletionForAlter = skipDeletionForAlter;
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.undoMetastoreOperationsEnabled = undoMetastoreOperationsEnabled;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;
        this.metastoreImpersonationEnabled = metastoreImpersonationEnabled;
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.filterStatsCalculatorService = requireNonNull(filterStatsCalculatorService, "filterStatsCalculatorService is null");
        this.tableParameterCodec = requireNonNull(tableParameterCodec, "tableParameterCodec is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.partitionUpdateSmileCodec = requireNonNull(partitionUpdateSmileCodec, "partitionUpdateSmileCodec is null");
        this.fileRenameExecutor = requireNonNull(fileRenameExecutor, "fileRenameExecutor is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
        this.stagingFileCommitter = requireNonNull(stagingFileCommitter, "stagingFileCommitter is null");
        this.zeroRowFileCreator = requireNonNull(zeroRowFileCreator, "zeroRowFileCreator is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
        this.partitionObjectBuilder = requireNonNull(partitionObjectBuilder, "partitionObjectBuilder is null");
        this.encryptionInformationProvider = requireNonNull(encryptionInformationProvider, "encryptionInformationProvider is null");
        this.hivePartitionStats = requireNonNull(hivePartitionStats, "hivePartitionStats is null");
        this.hiveFileRenamer = requireNonNull(hiveFileRenamer, "hiveFileRenamer is null");

        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            log.warn("Hive writes are disabled. " +
                            "To write data to Hive, your JVM timezone must match the Hive storage timezone. " +
                            "Add -Duser.timezone=%s to your JVM arguments",
                    timeZone.getID());
        }
    }

    @Override
    public HiveMetadata get()
    {
        SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(
                hdfsEnvironment,
                CachingHiveMetastore.memoizeMetastore(this.metastore, metastoreImpersonationEnabled, perTransactionCacheMaximumSize), // per-transaction cache
                fileRenameExecutor,
                skipDeletionForAlter,
                skipTargetCleanupOnRollback,
                undoMetastoreOperationsEnabled);

        return new HiveMetadata(
                metastore,
                hdfsEnvironment,
                partitionManager,
                timeZone,
                allowCorruptWritesForTesting,
                writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled,
                maxPartitionBatchSize,
                typeManager,
                locationService,
                functionResolution,
                rowExpressionService,
                filterStatsCalculatorService,
                tableParameterCodec,
                partitionUpdateCodec,
                partitionUpdateSmileCodec,
                typeTranslator,
                prestoVersion,
                new MetastoreHiveStatisticsProvider(metastore),
                stagingFileCommitter,
                zeroRowFileCreator,
                partitionObjectBuilder,
                encryptionInformationProvider,
                hivePartitionStats,
                hiveFileRenamer);
    }
}
