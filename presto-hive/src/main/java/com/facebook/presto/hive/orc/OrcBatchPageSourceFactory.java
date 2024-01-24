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
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.AGGREGATED;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcLazyReadSmallRanges;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isOrcBloomFiltersEnabled;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isOrcZstdJniDecompressionEnabled;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveUtil.getPhysicalHiveColumnHandles;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrcBatchPageSourceFactory
        implements HiveBatchPageSourceFactory
{
    private final TypeManager typeManager;
    private final StandardFunctionResolution functionResolution;
    private final boolean useOrcColumnNames;
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
                functionResolution,
                requireNonNull(config, "hiveClientConfig is null").isUseOrcColumnNames(),
                hdfsEnvironment,
                stats,
                config.getDomainCompactionThreshold(),
                orcFileTailSource,
                stripeMetadataSourceFactory);
    }

    public OrcBatchPageSourceFactory(
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            boolean useOrcColumnNames,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            int domainCompactionThreshold,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.useOrcColumnNames = useOrcColumnNames;
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
            Optional<EncryptionInformation> encryptionInformation)
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
                session.getUser(),
                configuration,
                fileSplit,
                columns,
                useOrcColumnNames,
                effectivePredicate,
                hiveStorageTimeZone,
                typeManager,
                functionResolution,
                getOrcMaxBufferSize(session),
                getOrcStreamBufferSize(session),
                getOrcLazyReadSmallRanges(session),
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
                NO_ENCRYPTION));
    }

    public static ConnectorPageSource createOrcPageSource(
            OrcEncoding orcEncoding,
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            Configuration configuration,
            HiveFileSplit fileSplit,
            List<HiveColumnHandle> columns,
            boolean useOrcColumnNames,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            DataSize maxBufferSize,
            DataSize streamBufferSize,
            boolean lazyReadSmallRanges,
            boolean orcBloomFiltersEnabled,
            FileFormatDataSourceStats stats,
            int domainCompactionThreshold,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory,
            HiveFileContext hiveFileContext,
            OrcReaderOptions orcReaderOptions,
            Optional<EncryptionInformation> encryptionInformation,
            DwrfEncryptionProvider dwrfEncryptionProvider)
    {
        checkArgument(domainCompactionThreshold >= 1, "domainCompactionThreshold must be at least 1");

        OrcDataSource orcDataSource;
        Path path = new Path(fileSplit.getPath());
        try {
            FSDataInputStream inputStream = hdfsEnvironment.getFileSystem(sessionUser, path, configuration).openFile(path, hiveFileContext);

            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(fileSplit.getPath()),
                    fileSplit.getFileSize(),
                    orcReaderOptions.getMaxMergeDistance(),
                    maxBufferSize,
                    streamBufferSize,
                    lazyReadSmallRanges,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, fileSplit), e);
        }

        OrcAggregatedMemoryContext systemMemoryUsage = new HiveOrcAggregatedMemoryContext();
        try {
            DwrfKeyProvider dwrfKeyProvider = new ProjectionBasedDwrfKeyProvider(encryptionInformation, columns, useOrcColumnNames, path);
            OrcReader reader = new OrcReader(
                    orcDataSource,
                    orcEncoding,
                    orcFileTailSource,
                    stripeMetadataSourceFactory,
                    new HiveOrcAggregatedMemoryContext(),
                    orcReaderOptions,
                    hiveFileContext.isCacheable(),
                    dwrfEncryptionProvider,
                    dwrfKeyProvider,
                    hiveFileContext.getStats());

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

            if (!physicalColumns.isEmpty() && physicalColumns.stream().allMatch(hiveColumnHandle -> hiveColumnHandle.getColumnType() == AGGREGATED)) {
                return new AggregatedOrcPageSource(physicalColumns, reader.getFooter(), typeManager, functionResolution);
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

            return new OrcBatchPageSource(
                    recordReader,
                    reader.getOrcDataSource(),
                    physicalColumns,
                    typeManager,
                    systemMemoryUsage,
                    stats,
                    hiveFileContext.getStats());
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = splitError(e, fileSplit);
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static String splitError(Throwable t, HiveFileSplit fileSplit)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s",
                fileSplit.getPath(),
                fileSplit.getStart(),
                fileSplit.getLength(),
                t.getMessage());
    }
}
