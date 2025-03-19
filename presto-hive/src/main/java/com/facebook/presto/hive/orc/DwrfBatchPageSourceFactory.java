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

import com.facebook.hive.orc.OrcSerde;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveBatchPageSourceFactory;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveDwrfEncryptionProvider;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileSplit;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isOrcZstdJniDecompressionEnabled;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.orc.OrcBatchPageSourceFactory.createOrcPageSource;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static java.util.Objects.requireNonNull;

public class DwrfBatchPageSourceFactory
        implements HiveBatchPageSourceFactory
{
    private final TypeManager typeManager;
    private final StandardFunctionResolution functionResolution;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final int domainCompactionThreshold;
    private final OrcFileTailSource orcFileTailSource;
    private final StripeMetadataSourceFactory stripeMetadataSourceFactory;
    private final DwrfEncryptionProvider dwrfEncryptionProvider;

    @Inject
    public DwrfBatchPageSourceFactory(
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            HiveClientConfig config,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory,
            HiveDwrfEncryptionProvider dwrfEncryptionProvider)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.domainCompactionThreshold = requireNonNull(config, "config is null").getDomainCompactionThreshold();
        this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailSource is null");
        this.stripeMetadataSourceFactory = requireNonNull(stripeMetadataSourceFactory, "stripeMetadataSourceFactory is null");
        this.dwrfEncryptionProvider = requireNonNull(dwrfEncryptionProvider, "dwrfEncryptionProvider is null").toDwrfEncryptionProvider();
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

        if (fileSplit.getFileSize() == 0) {
            throw new PrestoException(HIVE_BAD_DATA, "ORC file is empty: " + fileSplit.getPath());
        }

        return Optional.of(createOrcPageSource(
                DWRF,
                hdfsEnvironment,
                configuration,
                fileSplit,
                columns,
                false,
                effectivePredicate,
                hiveStorageTimeZone,
                typeManager,
                false,
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
                dwrfEncryptionProvider,
                session,
                rowIDPartitionComponent));
    }
}
