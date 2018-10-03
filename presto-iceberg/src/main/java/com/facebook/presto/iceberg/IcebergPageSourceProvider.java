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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HivePageSource;
import com.facebook.presto.hive.HivePageSourceProvider;
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.iceberg.parquet.ParquetDataSource;
import com.facebook.presto.iceberg.parquet.ParquetPageSource;
import com.facebook.presto.iceberg.parquet.memory.AggregatedMemoryContext;
import com.facebook.presto.iceberg.parquet.predicate.ParquetPredicate;
import com.facebook.presto.iceberg.parquet.reader.ParquetMetadataReader;
import com.facebook.presto.iceberg.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static com.facebook.presto.hive.util.ConfigurationUtils.getInitialConfiguration;
import static com.facebook.presto.iceberg.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.iceberg.parquet.ParquetTypeUtils.getParquetType;
import static com.facebook.presto.iceberg.parquet.predicate.ParquetPredicateUtils.buildParquetPredicate;
import static com.facebook.presto.iceberg.parquet.predicate.ParquetPredicateUtils.predicateMatches;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class IcebergPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final HiveClientConfig hiveClientConfig;

    @Inject
    public IcebergPageSourceProvider(
            HiveClientConfig hiveClientConfig,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager)
    {
        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        this.hiveClientConfig = hiveClientConfig;
        this.hdfsEnvironment = hdfsEnvironment;
        this.typeManager = typeManager;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns)
    {
        IcebergSplit icebergSplit = (IcebergSplit) split;
        final Path path = new Path(icebergSplit.getPath());
        final long start = icebergSplit.getStart();
        final long length = icebergSplit.getLength();
        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());
        return createParquetPageSource(hdfsEnvironment,
                session.getUser(),
                getInitialConfiguration(),
                path,
                start,
                length,
                hiveColumns,
                icebergSplit.getNameToId(),
                hiveClientConfig.isUseParquetColumnNames(),
                typeManager,
                icebergSplit.getEffectivePredicate(),
                ((IcebergSplit) split).getPartitionKeys());
    }

    public static ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<HiveColumnHandle> columns,
            Map<String, Integer> icebergNameToId,
            boolean useParquetColumnNames,
            TypeManager typeManager,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HivePartitionKey> partitionKeys)
    {
        AggregatedMemoryContext systemMemoryContext = new AggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            dataSource = buildHdfsParquetDataSource(fileSystem, path, start, length);
            ParquetMetadata parquetMetadata = ParquetMetadataReader.readFooter(fileSystem, path);
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // We need to transform columns so they have the parquet column name and not table column name.
            // In order to make that transformation we need to pass the iceberg schema (not the hive schema) in split and
            // use that here to map from iceberg schema column name to ID, lookup parquet column with same ID and all of its children
            // and use the index of all those columns as requested schema.

            final List<HiveColumnHandle> parquetColumns = convertToParquetNames(columns, icebergNameToId, fileSchema);
            List<org.apache.parquet.schema.Type> fields = parquetColumns.stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getParquetType(column, fileSchema, true)) // we always use parquet column names in case of iceberg.
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

            ParquetPredicate parquetPredicate = buildParquetPredicate(parquetColumns, effectivePredicate, fileMetaData.getSchema(), typeManager);
            final ParquetDataSource finalDataSource = dataSource;
            blocks = blocks.stream()
                    .filter(block -> predicateMatches(parquetPredicate, block, finalDataSource, requestedSchema, effectivePredicate))
                    .collect(toList());

            ParquetReader parquetReader = new ParquetReader(
                    fileSchema,
                    requestedSchema,
                    blocks,
                    dataSource,
                    typeManager,
                    systemMemoryContext);

            List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(partitionKeys, columns, Collections.EMPTY_LIST, Collections.emptyMap(), path, OptionalInt.empty());

            return new HivePageSource(
                    columnMappings,
                    Optional.empty(),
                    DateTimeZone.UTC,
                    typeManager,
                    new ParquetPageSource(
                            parquetReader,
                            dataSource,
                            fileSchema,
                            requestedSchema,
                            length,
                            new Properties(), // unused.
                            parquetColumns.stream().filter(col -> col.getColumnType().equals(REGULAR)).collect(toList()),
                            effectivePredicate,
                            typeManager,
                            useParquetColumnNames,
                            systemMemoryContext));
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

    /**
     * This method maps the iceberg column names to corresponding parquet column names by matching their Ids rather then relying on name or index.
     * If no id match is found, it will return the original column name as is.
     *
     * @param columns iceberg columns
     * @param icebergNameToId
     * @param parquetSchema
     * @return columns with iceberg column names replaced with parquet column names.
     */
    private static List<HiveColumnHandle> convertToParquetNames(List<HiveColumnHandle> columns, Map<String, Integer> icebergNameToId, MessageType parquetSchema)
    {
        final List<Type> fields = parquetSchema.getFields();
        final List<HiveColumnHandle> result = new ArrayList<>();
        for (HiveColumnHandle column : columns) {
            Integer parquetId = icebergNameToId.get(column.getName());
            //we default to parquet name when no match is found to support migrated tables, this should be controlled by a config.
            final String parquetName = fields.stream().filter(f -> f.getId() != null && f.getId().intValue() == parquetId).map(m -> m.getName()).findFirst().orElse(column.getName());
            result.add(new HiveColumnHandle(parquetName, column.getHiveType(), column.getTypeSignature(), column.getHiveColumnIndex(), column.getColumnType(), column.getComment()));
        }
        return result;
    }
}
