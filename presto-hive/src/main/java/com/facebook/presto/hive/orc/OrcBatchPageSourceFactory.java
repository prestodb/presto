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
package com.facebook.presto.hive.orc;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveBatchPageSourceFactory;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileSplit;
import com.facebook.presto.hive.HiveOrcAggregatedMemoryContext;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isOrcBloomFiltersEnabled;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isOrcZstdJniDecompressionEnabled;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isUseOrcColumnNames;
import static com.facebook.presto.hive.HiveUtil.checkRowIDPartitionComponent;
import static com.facebook.presto.hive.HiveUtil.getPhysicalHiveColumnHandles;
import static com.facebook.presto.hive.orc.OrcPageSourceFactoryUtils.getOrcDataSource;
import static com.facebook.presto.hive.orc.OrcPageSourceFactoryUtils.getOrcReader;
import static com.facebook.presto.hive.orc.OrcPageSourceFactoryUtils.mapToPrestoException;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class OrcBatchPageSourceFactory
        implements HiveBatchPageSourceFactory
{
    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final int domainCompactionThreshold;
    private final OrcFileTailSource orcFileTailSource;
    private final StripeMetadataSourceFactory stripeMetadataSourceFactory;

    @Inject
    public OrcBatchPageSourceFactory(
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            HiveClientConfig config,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory)
    {
        this(
                typeManager,
                hdfsEnvironment,
                stats,
                config.getDomainCompactionThreshold(),
                orcFileTailSource,
                stripeMetadataSourceFactory);
    }

    public OrcBatchPageSourceFactory(
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            int domainCompactionThreshold,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.domainCompactionThreshold = domainCompactionThreshold;
        this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailSource is null");
        this.stripeMetadataSourceFactory = requireNonNull(stripeMetadataSourceFactory, "stripeMetadataSourceFactory is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            HiveFileSplit fileSplit,
            Storage storage,
            SchemaTableName tableName,
            Map<String, String> tableParameters,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            HiveFileContext hiveFileContext,
            Optional<EncryptionInformation> encryptionInformation,
            Optional<byte[]> rowIDPartitionComponent)
    {
        if (!OrcSerde.class.getName().equals(storage.getStorageFormat().getSerDe())) {
            return Optional.empty();
        }

        // per HIVE-13040 and ORC-162, empty files are allowed
        if (fileSplit.getFileSize() == 0) {
            return Optional.of(new FixedPageSource(ImmutableList.of()));
        }

        return Optional.of(createOrcPageSource(
                ORC,
                hdfsEnvironment,
                configuration,
                fileSplit,
                columns,
                isUseOrcColumnNames(session),
                effectivePredicate,
                hiveStorageTimeZone,
                typeManager,
                isOrcBloomFiltersEnabled(session),
                stats,
                domainCompactionThreshold,
                orcFileTailSource,
                stripeMetadataSourceFactory,
                hiveFileContext,
                OrcReaderOptions.builder()
                        .withMaxMergeDistance(getOrcMaxMergeDistance(session))
                        .withTinyStripeThreshold(getOrcTinyStripeThreshold(session))
                        .withMaxBlockSize(getOrcMaxReadBlockSize(session))
                        .withZstdJniDecompressionEnabled(isOrcZstdJniDecompressionEnabled(session))
                        .build(),
                encryptionInformation,
                NO_ENCRYPTION,
                session,
                rowIDPartitionComponent));
    }

    public static ConnectorPageSource createOrcPageSource(
            OrcEncoding orcEncoding,
            HdfsEnvironment hdfsEnvironment,
            Configuration configuration,
            HiveFileSplit fileSplit,
            List<HiveColumnHandle> columns,
            boolean useOrcColumnNames,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            boolean orcBloomFiltersEnabled,
            FileFormatDataSourceStats stats,
            int domainCompactionThreshold,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory,
            HiveFileContext hiveFileContext,
            OrcReaderOptions orcReaderOptions,
            Optional<EncryptionInformation> encryptionInformation,
            DwrfEncryptionProvider dwrfEncryptionProvider,
            ConnectorSession session,
            Optional<byte[]> rowIDPartitionComponent)
    {
        checkArgument(domainCompactionThreshold >= 1, "domainCompactionThreshold must be at least 1");
        checkRowIDPartitionComponent(columns, rowIDPartitionComponent);

        OrcDataSource orcDataSource = getOrcDataSource(session, fileSplit, hdfsEnvironment, configuration, hiveFileContext, stats);
        Path path = new Path(fileSplit.getPath());

        OrcAggregatedMemoryContext systemMemoryUsage = new HiveOrcAggregatedMemoryContext();
        try {
            OrcReader reader = getOrcReader(
                    orcEncoding,
                    columns,
                    useOrcColumnNames,
                    orcFileTailSource,
                    stripeMetadataSourceFactory,
                    hiveFileContext,
                    orcReaderOptions,
                    encryptionInformation,
                    dwrfEncryptionProvider,
                    orcDataSource,
                    path);

            List<HiveColumnHandle> physicalColumns = getPhysicalHiveColumnHandles(columns, useOrcColumnNames, reader.getTypes(), path);
            ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
            ImmutableList.Builder<ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();
            for (HiveColumnHandle column : physicalColumns) {
                if (column.getColumnType() == REGULAR) {
                    Type type = typeManager.getType(column.getTypeSignature());
                    includedColumns.put(column.getHiveColumnIndex(), type);
                    columnReferences.add(new ColumnReference<>(column, column.getHiveColumnIndex(), type));
                }
            }

            OrcPredicate predicate = new TupleDomainOrcPredicate<>(effectivePredicate, columnReferences.build(), orcBloomFiltersEnabled, Optional.of(domainCompactionThreshold));

            OrcBatchRecordReader recordReader = reader.createBatchRecordReader(
                    includedColumns.build(),
                    predicate,
                    fileSplit.getStart(),
                    fileSplit.getLength(),
                    hiveStorageTimeZone,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE);

            byte[] partitionID = rowIDPartitionComponent.orElseGet(() -> new byte[0]);
            String rowGroupID = path.getName();

            // none of the columns are row numbers
            List<Boolean> isRowNumberList = nCopies(physicalColumns.size(), false);
            return new OrcBatchPageSource(
                    recordReader,
                    reader.getOrcDataSource(),
                    physicalColumns,
                    typeManager,
                    systemMemoryUsage,
                    stats,
                    hiveFileContext.getStats(),
                    isRowNumberList,
                    partitionID,
                    rowGroupID);
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
            throw mapToPrestoException(e, path, fileSplit);
        }
    }
}
