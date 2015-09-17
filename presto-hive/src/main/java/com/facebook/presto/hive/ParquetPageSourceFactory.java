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

import com.facebook.presto.hive.parquet.ParquetMetadataReader;
import com.facebook.presto.hive.parquet.ParquetPredicate;
import com.facebook.presto.hive.parquet.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Properties;

import static com.facebook.presto.hive.HiveSessionProperties.isParquetOptimizedReaderEnabled;
import static com.facebook.presto.hive.HiveUtil.getDeserializerClassName;
import static com.facebook.presto.hive.parquet.ParquetUtils.buildParquetPredicate;
import static com.facebook.presto.hive.parquet.ParquetUtils.getParquetType;
import static com.facebook.presto.hive.parquet.ParquetUtils.getDictionariesByColumnOrdinal;
import static com.facebook.presto.hive.parquet.ParquetUtils.getStatisticsByColumnOrdinal;
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
    private final boolean useParquetColumnNames;

    @Inject
    public ParquetPageSourceFactory(TypeManager typeManager, HiveClientConfig config)
    {
        this(typeManager,
            requireNonNull(config, "hiveClientConfig is null").isUseParquetColumnNames());
    }

    public ParquetPageSourceFactory(TypeManager typeManager, boolean useParquetColumnNames)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.useParquetColumnNames = useParquetColumnNames;
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
            List<HivePartitionKey> partitionKeys,
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
                configuration,
                path,
                start,
                length,
                schema,
                columns,
                partitionKeys,
                effectivePredicate,
                useParquetColumnNames,
                typeManager));
    }

    public static ParquetPageSource createParquetPageSource(
            Configuration configuration,
            Path path,
            long start,
            long length,
            Properties schema,
            List<HiveColumnHandle> columns,
            List<HivePartitionKey> partitionKeys,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            boolean useParquetColumnNames,
            TypeManager typeManager)
    {
        try {
            ParquetMetadata parquetMetadata = ParquetMetadataReader.readFooter(configuration, path);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            List<parquet.schema.Type> fields = columns.stream()
                .filter(column -> !column.isPartitionKey())
                .map(column -> getParquetType(column, fileSchema, useParquetColumnNames))
                .filter(Objects::nonNull)
                .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), fields);

            ParquetPredicate parquetPredicate = buildParquetPredicate(columns, effectivePredicate, fileSchema, typeManager);
            List<BlockMetaData> blocks = new ArrayList<>();
            long splitStart = start;
            long splitLength = length;
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= splitStart &&
                    firstDataPage < splitStart + splitLength &&
                    parquetPredicate.matches(block.getRowCount(), getStatisticsByColumnOrdinal(block)) &&
                    parquetPredicate.matches(getDictionariesByColumnOrdinal(block, path, configuration, requestedSchema, effectivePredicate))) {
                    blocks.add(block);
                }
            }

            ParquetReader parquetReader = new ParquetReader(fileMetaData.getSchema(),
                                                            fileMetaData.getKeyValueMetaData(),
                                                            requestedSchema,
                                                            path,
                                                            blocks,
                                                            configuration);
            return new ParquetPageSource(parquetReader,
                                        requestedSchema,
                                        configuration,
                                        path,
                                        start,
                                        length,
                                        schema,
                                        columns,
                                        partitionKeys,
                                        effectivePredicate,
                                        typeManager);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    // TODO: support complex types
    private static boolean columnTypeSupported(List<HiveColumnHandle> columns)
    {
        List<String> fields = columns.stream()
            .map(HiveColumnHandle::getTypeSignature)
            .map(TypeSignature::getBase)
            .filter(base -> StandardTypes.ARRAY.equals(base) || StandardTypes.MAP.equals(base) || StandardTypes.ROW.equals(base))
            .collect(toList());
        return fields.isEmpty();
    }
}
