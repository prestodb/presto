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

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.parquet.ParquetPageSource;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.iceberg.FileFormat;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveFileContext.DEFAULT_HIVE_FILE_CONTEXT;
import static com.facebook.presto.hive.HiveSessionProperties.getParquetMaxReadBlockSize;
import static com.facebook.presto.hive.HiveSessionProperties.isFailOnCorruptedParquetStatistics;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetBatchReaderVerificationEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetBatchReadsEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isUseParquetColumnNames;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_MISSING_DATA;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetTypeByName;
import static com.facebook.presto.parquet.predicate.PredicateUtils.buildPredicate;
import static com.facebook.presto.parquet.predicate.PredicateUtils.predicateMatches;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.parquet.io.ColumnIOConverter.constructField;

public class IcebergPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;

    @Inject
    public IcebergPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> columns,
            SplitContext splitContext)
    {
        IcebergSplit split = (IcebergSplit) connectorSplit;
        IcebergTableLayoutHandle icebergLayout = (IcebergTableLayoutHandle) layout;
        IcebergTableHandle table = icebergLayout.getTable();

        List<IcebergColumnHandle> icebergColumns = columns.stream()
                .map(IcebergColumnHandle.class::cast)
                .collect(toImmutableList());

        Map<Integer, String> partitionKeys = split.getPartitionKeys();

        List<IcebergColumnHandle> regularColumns = columns.stream()
                .map(IcebergColumnHandle.class::cast)
                .filter(column -> !partitionKeys.containsKey(column.getId()))
                .collect(toImmutableList());

        // TODO: pushdownFilter for icebergLayout
        HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
        ConnectorPageSource dataPageSource = createDataPageSource(
                session,
                hdfsContext,
                new Path(split.getPath()),
                split.getStart(),
                split.getLength(),
                split.getFileFormat(),
                table.getSchemaTableName(),
                regularColumns,
                table.getPredicate());

        return new IcebergPageSource(icebergColumns, partitionKeys, dataPageSource, session.getSqlFunctionProperties().getTimeZoneKey());
    }

    private ConnectorPageSource createDataPageSource(
            ConnectorSession session,
            HdfsContext hdfsContext,
            Path path,
            long start,
            long length,
            FileFormat fileFormat,
            SchemaTableName tableName,
            List<IcebergColumnHandle> dataColumns,
            TupleDomain<IcebergColumnHandle> predicate)
    {
        switch (fileFormat) {
            // TODO: support ORC for iceberg
            case PARQUET:
                return createParquetPageSource(
                        hdfsEnvironment,
                        session.getUser(),
                        hdfsEnvironment.getConfiguration(hdfsContext, path),
                        path,
                        start,
                        length,
                        tableName,
                        dataColumns,
                        isUseParquetColumnNames(session),
                        isFailOnCorruptedParquetStatistics(session),
                        getParquetMaxReadBlockSize(session),
                        isParquetBatchReadsEnabled(session),
                        isParquetBatchReaderVerificationEnabled(session),
                        predicate,
                        fileFormatDataSourceStats);
        }
        throw new PrestoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + fileFormat);
    }

    private static ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            SchemaTableName tableName,
            List<IcebergColumnHandle> regularColumns,
            boolean useParquetColumnNames,
            boolean failOnCorruptedParquetStatistics,
            DataSize maxReadBlockSize,
            boolean batchReaderEnabled,
            boolean verificationEnabled,
            TupleDomain<IcebergColumnHandle> effectivePredicate,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            ExtendedFileSystem filesystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            long fileSize = filesystem.getFileStatus(path).getLen();
            FSDataInputStream inputStream = filesystem.openFile(path, DEFAULT_HIVE_FILE_CONTEXT);
            dataSource = buildHdfsParquetDataSource(inputStream, path, fileFormatDataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(inputStream, path, fileSize).getParquetMetadata();
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // Mapping from Iceberg field ID to Parquet fields.
            Map<Integer, org.apache.parquet.schema.Type> parquetIdToField = fileSchema.getFields().stream()
                    .filter(field -> field.getId() != null)
                    .collect(toImmutableMap(field -> field.getId().intValue(), Function.identity()));

            List<org.apache.parquet.schema.Type> parquetFields = regularColumns.stream()
                    .map(column -> {
                        if (parquetIdToField.isEmpty()) {
                            // This is a migrated table
                            return getParquetTypeByName(column.getName(), fileSchema);
                        }
                        return parquetIdToField.get(column.getId());
                    })
                    .collect(toList());

            // TODO: support subfield pushdown
            MessageType requestedSchema = new MessageType(fileSchema.getName(), parquetFields.stream().filter(Objects::nonNull).collect(toImmutableList()));
            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);

            List<BlockMetaData> blocks = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if ((firstDataPage >= start) && (firstDataPage < (start + length)) &&
                        predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain, failOnCorruptedParquetStatistics)) {
                    blocks.add(block);
                }
            }

            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    messageColumnIO,
                    blocks,
                    dataSource,
                    systemMemoryContext,
                    maxReadBlockSize,
                    batchReaderEnabled,
                    verificationEnabled);

            ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> prestoTypes = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
            for (int columnIndex = 0; columnIndex < regularColumns.size(); columnIndex++) {
                IcebergColumnHandle column = regularColumns.get(columnIndex);
                namesBuilder.add(column.getName());
                org.apache.parquet.schema.Type parquetField = parquetFields.get(columnIndex);

                Type prestoType = column.getType();

                prestoTypes.add(prestoType);

                if (parquetField == null) {
                    internalFields.add(Optional.empty());
                }
                else {
                    internalFields.add(constructField(column.getType(), messageColumnIO.getChild(parquetField.getName())));
                }
            }

            return new ParquetPageSource(parquetReader, prestoTypes.build(), internalFields.build(), namesBuilder.build());
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
            String message = format("Error opening Iceberg split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());

            if (e instanceof ParquetCorruptionException) {
                throw new PrestoException(ICEBERG_BAD_DATA, message, e);
            }

            if (e instanceof BlockMissingException) {
                throw new PrestoException(ICEBERG_MISSING_DATA, message, e);
            }
            throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<IcebergColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().get().forEach((columnHandle, domain) -> {
            String baseType = columnHandle.getType().getTypeSignature().getBase();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!baseType.equals(StandardTypes.MAP) && !baseType.equals(StandardTypes.ARRAY) && !baseType.equals(StandardTypes.ROW)) {
                RichColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
                if (descriptor != null) {
                    predicate.put(descriptor, domain);
                }
            }
        });
        return TupleDomain.withColumnDomains(predicate.build());
    }
}
