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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.hive.parquet.predicate.ParquetPredicate;
import com.facebook.presto.hive.parquet.reader.ParquetMetadataReader;
import com.facebook.presto.hive.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetOptimizedReaderEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetPredicatePushdownEnabled;
import static com.facebook.presto.hive.HiveUtil.getDeserializerClassName;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getParquetType;
import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.buildParquetPredicate;
import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.predicateMatches;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.CHAR;
import static com.facebook.presto.spi.type.StandardTypes.DATE;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.REAL;
import static com.facebook.presto.spi.type.StandardTypes.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.TIMESTAMP;
import static com.facebook.presto.spi.type.StandardTypes.TINYINT;
import static com.facebook.presto.spi.type.StandardTypes.VARBINARY;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ParquetPageSourceFactory
        implements HivePageSourceFactory
{
    private static final Set<String> PARQUET_SERDE_CLASS_NAMES = ImmutableSet.<String>builder()
            .add("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
            .add("parquet.hive.serde.ParquetHiveSerDe")
            .build();
    private static final Set<String> SUPPORTED_COLUMN_TYPES = ImmutableSet.of(INTEGER, BIGINT, BOOLEAN, DOUBLE, REAL, TIMESTAMP, VARCHAR, CHAR, VARBINARY, DATE, DECIMAL);
    private static final Set<String> SUPPORTED_PARTITION_TYPES = ImmutableSet.of(TINYINT, SMALLINT, INTEGER, BIGINT, BOOLEAN, DOUBLE, REAL, TIMESTAMP, VARCHAR, CHAR, DATE, DECIMAL);

    private final TypeManager typeManager;
    private final boolean useParquetColumnNames;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public ParquetPageSourceFactory(TypeManager typeManager, HiveClientConfig config, HdfsEnvironment hdfsEnvironment)
    {
        this(typeManager, requireNonNull(config, "hiveClientConfig is null").isUseParquetColumnNames(), hdfsEnvironment);
    }

    public ParquetPageSourceFactory(TypeManager typeManager, boolean useParquetColumnNames, HdfsEnvironment hdfsEnvironment)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.useParquetColumnNames = useParquetColumnNames;
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone)
    {
        if (!isParquetOptimizedReaderEnabled(session)) {
            return Optional.empty();
        }

        if (!PARQUET_SERDE_CLASS_NAMES.contains(getDeserializerClassName(schema))) {
            return Optional.empty();
        }

        if (!columnTypeSupported(columns)) {
            return Optional.empty();
        }

        return Optional.of(createParquetPageSource(
                hdfsEnvironment,
                session.getUser(),
                configuration,
                path,
                start,
                length,
                schema,
                columns,
                useParquetColumnNames,
                typeManager,
                isParquetPredicatePushdownEnabled(session),
                effectivePredicate));
    }

    public static ParquetPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            boolean useParquetColumnNames,
            TypeManager typeManager,
            boolean predicatePushdownEnabled,
            TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            dataSource = buildHdfsParquetDataSource(fileSystem, path, start, length);
            ParquetMetadata parquetMetadata = ParquetMetadataReader.readFooter(fileSystem, path);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            List<parquet.schema.Type> fields = columns.stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getParquetType(column, fileSchema, useParquetColumnNames))
                    .filter(Objects::nonNull)
                    .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), fields);

            List<BlockMetaData> blocks = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= start && firstDataPage < start + length) {
                    blocks.add(block);
                }
            }

            if (predicatePushdownEnabled) {
                ParquetPredicate parquetPredicate = buildParquetPredicate(columns, effectivePredicate, fileMetaData.getSchema(), typeManager);
                final ParquetDataSource finalDataSource = dataSource;
                blocks = blocks.stream()
                        .filter(block -> predicateMatches(parquetPredicate, block, finalDataSource, requestedSchema, effectivePredicate))
                        .collect(toList());
            }

            ParquetReader parquetReader = new ParquetReader(
                    requestedSchema,
                    blocks,
                    dataSource);

            return new ParquetPageSource(
                    parquetReader,
                    dataSource,
                    fileSchema,
                    requestedSchema,
                    length,
                    schema,
                    columns,
                    effectivePredicate,
                    typeManager,
                    useParquetColumnNames);
        }
        catch (Exception e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    // TODO: support complex types
    private static boolean columnTypeSupported(List<HiveColumnHandle> columns)
    {
        boolean regularColumnsSupported = columns.stream()
                .filter(column -> column.getColumnType() == REGULAR)
                .map(HiveColumnHandle::getTypeSignature)
                .map(TypeSignature::getBase)
                .allMatch(SUPPORTED_COLUMN_TYPES::contains);

        boolean partitionColumnsSupported = columns.stream()
                .filter(HiveColumnHandle::isPartitionKey)
                .map(HiveColumnHandle::getTypeSignature)
                .map(TypeSignature::getBase)
                .allMatch(SUPPORTED_PARTITION_TYPES::contains);

        return regularColumnsSupported && partitionColumnsSupported;
    }
}
