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

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveAggregatedPageSourceFactory;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileSplit;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.google.common.collect.ImmutableList;
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isOrcZstdJniDecompressionEnabled;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isUseOrcColumnNames;
import static com.facebook.presto.hive.HiveUtil.getPhysicalHiveColumnHandles;
import static com.facebook.presto.hive.orc.OrcPageSourceFactoryUtils.getOrcDataSource;
import static com.facebook.presto.hive.orc.OrcPageSourceFactoryUtils.getOrcReader;
import static com.facebook.presto.hive.orc.OrcPageSourceFactoryUtils.mapToPrestoException;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static java.util.Objects.requireNonNull;

public class OrcAggregatedPageSourceFactory
        implements HiveAggregatedPageSourceFactory
{
    private final TypeManager typeManager;
    private final StandardFunctionResolution functionResolution;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final OrcFileTailSource orcFileTailSource;
    private final StripeMetadataSourceFactory stripeMetadataSourceFactory;

    @Inject
    public OrcAggregatedPageSourceFactory(
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailCache is null");
        this.stripeMetadataSourceFactory = requireNonNull(stripeMetadataSourceFactory, "stripeMetadataSourceFactory is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            HiveFileSplit fileSplit,
            Storage storage,
            List<HiveColumnHandle> columns,
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
                session,
                ORC,
                hdfsEnvironment,
                configuration,
                fileSplit,
                columns,
                isUseOrcColumnNames(session),
                typeManager,
                functionResolution,
                stats,
                orcFileTailSource,
                stripeMetadataSourceFactory,
                hiveFileContext,
                encryptionInformation,
                NO_ENCRYPTION,
                false,
                Optional.empty()));
    }

    public static ConnectorPageSource createOrcPageSource(
            ConnectorSession session,
            OrcEncoding orcEncoding,
            HdfsEnvironment hdfsEnvironment,
            Configuration configuration,
            HiveFileSplit fileSplit,
            List<HiveColumnHandle> columns,
            boolean useOrcColumnNames,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            FileFormatDataSourceStats stats,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory,
            HiveFileContext hiveFileContext,
            Optional<EncryptionInformation> encryptionInformation,
            DwrfEncryptionProvider dwrfEncryptionProvider,
            boolean appendRowNumberEnabled,
            Optional<byte[]> rowIDPartitionComponent)
    {
        OrcDataSource orcDataSource = getOrcDataSource(session, fileSplit, hdfsEnvironment, configuration, hiveFileContext, stats);

        DataSize maxMergeDistance = getOrcMaxMergeDistance(session);
        DataSize tinyStripeThreshold = getOrcTinyStripeThreshold(session);
        DataSize maxReadBlockSize = getOrcMaxReadBlockSize(session);

        Path path = new Path(fileSplit.getPath());

        OrcReaderOptions orcReaderOptions = OrcReaderOptions.builder()
                .withMaxMergeDistance(maxMergeDistance)
                .withTinyStripeThreshold(tinyStripeThreshold)
                .withMaxBlockSize(maxReadBlockSize)
                .withZstdJniDecompressionEnabled(isOrcZstdJniDecompressionEnabled(session))
                .withAppendRowNumber(appendRowNumberEnabled)
                .build();
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

            return new AggregatedOrcPageSource(physicalColumns, reader.getFooter(), typeManager, functionResolution);
        }
        catch (Exception e) {
            throw mapToPrestoException(e, path, fileSplit);
        }
        finally {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
        }
    }
}
