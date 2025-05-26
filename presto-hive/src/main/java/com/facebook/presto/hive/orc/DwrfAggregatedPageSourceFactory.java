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
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveAggregatedPageSourceFactory;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileSplit;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.orc.StripeMetadataSourceFactory;
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveCommonSessionProperties.isUseOrcColumnNames;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_BAD_DATA;
import static com.facebook.presto.hive.orc.OrcAggregatedPageSourceFactory.createOrcPageSource;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static java.util.Objects.requireNonNull;

public class DwrfAggregatedPageSourceFactory
        implements HiveAggregatedPageSourceFactory
{
    private final TypeManager typeManager;
    private final StandardFunctionResolution functionResolution;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final OrcFileTailSource orcFileTailSource;
    private final StripeMetadataSourceFactory stripeMetadataSourceFactory;

    @Inject
    public DwrfAggregatedPageSourceFactory(
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

        if (fileSplit.getFileSize() == 0) {
            throw new PrestoException(HIVE_BAD_DATA, "ORC file is empty: " + fileSplit.getPath());
        }

        return Optional.of(createOrcPageSource(
                session,
                DWRF,
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
}
