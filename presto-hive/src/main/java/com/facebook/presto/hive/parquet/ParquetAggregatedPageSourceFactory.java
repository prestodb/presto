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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveAggregatedPageSourceFactory;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileSplit;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.cache.ParquetMetadataSource;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.HiveCommonSessionProperties.getReadNullMaskedParquetEncryptedValue;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.createDecryptor;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactoryUtils.mapToPrestoException;
import static java.util.Objects.requireNonNull;

public class ParquetAggregatedPageSourceFactory
        implements HiveAggregatedPageSourceFactory
{
    private static final Set<String> PARQUET_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
            .add("parquet.hive.serde.ParquetHiveSerDe")
            .build();

    private final TypeManager typeManager;
    private final StandardFunctionResolution functionResolution;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final ParquetMetadataSource parquetMetadataSource;

    @Inject
    public ParquetAggregatedPageSourceFactory(TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats stats,
            ParquetMetadataSource parquetMetadataSource)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.parquetMetadataSource = requireNonNull(parquetMetadataSource, "parquetMetadataSource is null");
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
        if (!PARQUET_SERDE_CLASS_NAMES.contains(storage.getStorageFormat().getSerDe())) {
            return Optional.empty();
        }

        return Optional.of(createParquetPageSource(
                hdfsEnvironment,
                session,
                configuration,
                fileSplit,
                columns,
                typeManager,
                functionResolution,
                stats,
                hiveFileContext,
                parquetMetadataSource));
    }

    public static ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            ConnectorSession session,
            Configuration configuration,
            HiveFileSplit fileSplit,
            List<HiveColumnHandle> columns,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            FileFormatDataSourceStats stats,
            HiveFileContext hiveFileContext,
            ParquetMetadataSource parquetMetadataSource)
    {
        String user = session.getUser();
        boolean readMaskedValue = getReadNullMaskedParquetEncryptedValue(session);

        ParquetDataSource dataSource = null;
        Path path = new Path(fileSplit.getPath());
        try {
            FSDataInputStream inputStream = hdfsEnvironment.getFileSystem(user, path, configuration).openFile(path, hiveFileContext);
            // Lambda expression below requires final variable, so we define a new variable parquetDataSource.
            final ParquetDataSource parquetDataSource = buildHdfsParquetDataSource(inputStream, path, stats);
            dataSource = parquetDataSource;
            Optional<InternalFileDecryptor> fileDecryptor = createDecryptor(configuration, path);
            ParquetMetadata parquetMetadata = hdfsEnvironment.doAs(user, () -> parquetMetadataSource.getParquetMetadata(
                    parquetDataSource,
                    fileSplit.getFileSize(),
                    hiveFileContext.isCacheable(),
                    hiveFileContext.getModificationTime(),
                    fileDecryptor,
                    readMaskedValue).getParquetMetadata());

            return new AggregatedParquetPageSource(columns, parquetMetadata, typeManager, functionResolution);
        }
        catch (Exception e) {
            throw mapToPrestoException(e, path, fileSplit);
        }
        finally {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
        }
    }
}
