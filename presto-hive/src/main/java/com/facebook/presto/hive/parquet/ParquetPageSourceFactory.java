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

import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSourceFactory;
import com.facebook.presto.hive.parquet.predicate.ParquetPredicate;
import com.facebook.presto.hive.parquet.reader.ParquetMetadataReader;
import com.facebook.presto.hive.parquet.reader.ParquetReader;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.MessageColumnIO;
import parquet.schema.MessageType;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetOptimizedReaderEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetPredicatePushdownEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isUseParquetColumnNames;
import static com.facebook.presto.hive.HiveUtil.getDeserializerClassName;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getParquetType;
import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.buildParquetPredicate;
import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.getParquetTupleDomain;
import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.predicateMatches;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.google.common.base.Strings.nullToEmpty;
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

    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;

    @Inject
    public ParquetPageSourceFactory(TypeManager typeManager, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
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

        return Optional.of(createParquetPageSource(
                hdfsEnvironment,
                session.getUser(),
                configuration,
                path,
                start,
                length,
                fileSize,
                schema,
                columns,
                isUseParquetColumnNames(session),
                typeManager,
                isParquetPredicatePushdownEnabled(session),
                effectivePredicate,
                stats));
    }

    public static ParquetPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            boolean useParquetColumnNames,
            TypeManager typeManager,
            boolean predicatePushdownEnabled,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            FileFormatDataSourceStats stats)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FSDataInputStream inputStream = fileSystem.open(path);
            ParquetMetadata parquetMetadata = ParquetMetadataReader.readFooter(inputStream, path, fileSize);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();
            dataSource = buildHdfsParquetDataSource(inputStream, path, fileSize, stats);

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
                Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
                TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
                ParquetPredicate parquetPredicate = buildParquetPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);
                final ParquetDataSource finalDataSource = dataSource;
                blocks = blocks.stream()
                        .filter(block -> predicateMatches(parquetPredicate, block, finalDataSource, descriptorsByPath, parquetTupleDomain))
                        .collect(toList());
            }
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    messageColumnIO,
                    blocks,
                    dataSource,
                    systemMemoryContext);

            return new ParquetPageSource(
                    parquetReader,
                    fileSchema,
                    messageColumnIO,
                    typeManager,
                    schema,
                    columns,
                    effectivePredicate,
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
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }
}
