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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveDwrfEncryptionProvider;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveOrcAggregatedMemoryContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.orc.HdfsOrcDataSource;
import com.facebook.presto.hive.orc.OrcBatchPageSource;
import com.facebook.presto.hive.orc.ProjectionBasedDwrfKeyProvider;
import com.facebook.presto.hive.parquet.ParquetPageSource;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
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
import com.facebook.presto.orc.cache.OrcFileTailSource;
import com.facebook.presto.orc.metadata.OrcType;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.stream.IntStream;

import static com.facebook.presto.hive.CacheQuota.NO_CACHE_CONSTRAINTS;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveSessionProperties.getParquetMaxReadBlockSize;
import static com.facebook.presto.hive.HiveSessionProperties.isFailOnCorruptedParquetStatistics;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetBatchReaderVerificationEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetBatchReadsEnabled;
import static com.facebook.presto.hive.HiveSessionProperties.isUseParquetColumnNames;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_MISSING_DATA;
import static com.facebook.presto.iceberg.IcebergOrcColumn.ROOT_COLUMN_ID;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getOrcLazyReadSmallRanges;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isOrcBloomFiltersEnabled;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isOrcZstdJniDecompressionEnabled;
import static com.facebook.presto.iceberg.TypeConverter.ORC_ICEBERG_ID_KEY;
import static com.facebook.presto.iceberg.TypeConverter.toHiveType;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetTypeByName;
import static com.facebook.presto.parquet.predicate.PredicateUtils.buildPredicate;
import static com.facebook.presto.parquet.predicate.PredicateUtils.predicateMatches;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.parquet.io.ColumnIOConverter.constructField;
import static org.joda.time.DateTimeZone.UTC;

public class IcebergPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final TypeManager typeManager;
    private final OrcFileTailSource orcFileTailSource;
    private final StripeMetadataSourceFactory stripeMetadataSourceFactory;
    private final DwrfEncryptionProvider dwrfEncryptionProvider;
    private final HiveClientConfig hiveClientConfig;

    @Inject
    public IcebergPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            TypeManager typeManager,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory,
            HiveDwrfEncryptionProvider dwrfEncryptionProvider,
            HiveClientConfig hiveClientConfig)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailSource is null");
        this.stripeMetadataSourceFactory = requireNonNull(stripeMetadataSourceFactory, "stripeMetadataSourceFactory is null");
        this.dwrfEncryptionProvider = requireNonNull(dwrfEncryptionProvider, "DwrfEncryptionProvider is null").toDwrfEncryptionProvider();
        this.hiveClientConfig = requireNonNull(hiveClientConfig, "hiveClientConfig is null");
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
                table.getPredicate(),
                splitContext.isCacheable());

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
            TupleDomain<IcebergColumnHandle> predicate,
            boolean isCacheable)
    {
        switch (fileFormat) {
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
            case ORC:
                FileStatus fileStatus = null;
                try {
                    fileStatus = hdfsEnvironment.doAs(session.getUser(), () -> hdfsEnvironment.getFileSystem(hdfsContext, path).getFileStatus(path));
                }
                catch (IOException e) {
                    throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, e);
                }
                long fileSize = fileStatus.getLen();
                OrcReaderOptions readerOptions = new OrcReaderOptions(
                        getOrcMaxMergeDistance(session),
                        getOrcTinyStripeThreshold(session),
                        getOrcMaxReadBlockSize(session),
                        isOrcZstdJniDecompressionEnabled(session));

                // TODO: Implement EncryptionInformation in IcebergSplit instead of Optional.empty()
                return createBatchOrcPageSource(
                        hdfsEnvironment,
                        session.getUser(),
                        hdfsEnvironment.getConfiguration(hdfsContext, path),
                        path,
                        start,
                        length,
                        fileSize,
                        isCacheable,
                        dataColumns,
                        typeManager,
                        predicate,
                        readerOptions,
                        ORC,
                        getOrcMaxBufferSize(session),
                        getOrcStreamBufferSize(session),
                        getOrcLazyReadSmallRanges(session),
                        isOrcBloomFiltersEnabled(session),
                        hiveClientConfig.getDomainCompactionThreshold(),
                        orcFileTailSource,
                        stripeMetadataSourceFactory,
                        fileFormatDataSourceStats,
                        Optional.empty(),
                        dwrfEncryptionProvider);
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
            FileStatus fileStatus = filesystem.getFileStatus(path);
            long fileSize = fileStatus.getLen();
            long modificationTime = fileStatus.getModificationTime();
            HiveFileContext hiveFileContext = new HiveFileContext(true, NO_CACHE_CONSTRAINTS,
                    Optional.empty(), Optional.of(fileSize), modificationTime, false);
            FSDataInputStream inputStream = filesystem.openFile(path, hiveFileContext);
            dataSource = buildHdfsParquetDataSource(inputStream, path, fileFormatDataSourceStats);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, fileSize).getParquetMetadata();
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

            return new ParquetPageSource(parquetReader, prestoTypes.build(), internalFields.build(), namesBuilder.build(), new RuntimeStats());
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

    private static ConnectorPageSource createBatchOrcPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            boolean isCacheable,
            List<IcebergColumnHandle> regularColumns,
            TypeManager typeManager,
            TupleDomain<IcebergColumnHandle> effectivePredicate,
            OrcReaderOptions options,
            OrcEncoding orcEncoding,
            DataSize maxBufferSize,
            DataSize streamBufferSize,
            boolean lazyReadSmallRanges,
            boolean orcBloomFiltersEnabled,
            int domainCompactionThreshold,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory,
            FileFormatDataSourceStats stats,
            Optional<EncryptionInformation> encryptionInformation,
            DwrfEncryptionProvider dwrfEncryptionProvider)
    {
        OrcDataSource orcDataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(user, () -> fileSystem.open(path));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    options.getMaxMergeDistance(),
                    maxBufferSize,
                    streamBufferSize,
                    lazyReadSmallRanges,
                    inputStream,
                    stats);

            // Todo: pass real columns to ProjectionBasedDwrfKeyProvider instead of ImmutableList.of()
            DwrfKeyProvider dwrfKeyProvider = new ProjectionBasedDwrfKeyProvider(encryptionInformation, ImmutableList.of(), true, path);
            RuntimeStats runtimeStats = new RuntimeStats();
            OrcReader reader = new OrcReader(
                    orcDataSource,
                    orcEncoding,
                    orcFileTailSource,
                    stripeMetadataSourceFactory,
                    new HiveOrcAggregatedMemoryContext(),
                    options,
                    isCacheable,
                    dwrfEncryptionProvider,
                    dwrfKeyProvider,
                    runtimeStats);

            List<HiveColumnHandle> physicalColumnHandles = new ArrayList<>(regularColumns.size());
            ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
            ImmutableList.Builder<TupleDomainOrcPredicate.ColumnReference<HiveColumnHandle>> columnReferences = ImmutableList.builder();

            List<IcebergOrcColumn> fileOrcColumns = getFileOrcColumns(reader);

            Map<Integer, IcebergOrcColumn> fileOrcColumnByIcebergId = fileOrcColumns.stream()
                    .filter(orcColumn -> orcColumn.getAttributes().containsKey(ORC_ICEBERG_ID_KEY))
                    .collect(toImmutableMap(
                            orcColumn -> Integer.parseInt(orcColumn.getAttributes().get(ORC_ICEBERG_ID_KEY)),
                            orcColumn -> IcebergOrcColumn.copy(orcColumn).setIcebergColumnId(Optional.of(Integer.parseInt(orcColumn.getAttributes().get(ORC_ICEBERG_ID_KEY))))));

            Map<String, IcebergOrcColumn> fileOrcColumnsByName = uniqueIndex(fileOrcColumns, orcColumn -> orcColumn.getColumnName().toLowerCase(ENGLISH));

            int nextMissingColumnIndex = fileOrcColumnsByName.size();
            for (IcebergColumnHandle column : regularColumns) {
                IcebergOrcColumn icebergOrcColumn;
                boolean isExcludeColumn = false;

                if (fileOrcColumnByIcebergId.isEmpty()) {
                    icebergOrcColumn = fileOrcColumnsByName.get(column.getName());
                }
                else {
                    icebergOrcColumn = fileOrcColumnByIcebergId.get(column.getId());
                    if (icebergOrcColumn == null) {
                        // Cannot get orc column from 'fileOrcColumnByIcebergId', which means SchemaEvolution may have happened, so we get orc column by column name.
                        icebergOrcColumn = fileOrcColumnsByName.get(column.getName());
                        if (icebergOrcColumn != null) {
                            isExcludeColumn = true;
                        }
                    }
                }

                if (icebergOrcColumn != null) {
                    HiveColumnHandle columnHandle = new HiveColumnHandle(
                            // Todo: using orc file column name
                            column.getName(),
                            toHiveType(column.getType()),
                            column.getType().getTypeSignature(),
                            icebergOrcColumn.getOrcColumnId(),
                            icebergOrcColumn.getColumnType(),
                            Optional.empty(),
                            Optional.empty());

                    physicalColumnHandles.add(columnHandle);
                    // Skip SchemaEvolution column
                    if (!isExcludeColumn) {
                        includedColumns.put(columnHandle.getHiveColumnIndex(), typeManager.getType(columnHandle.getTypeSignature()));
                        columnReferences.add(new TupleDomainOrcPredicate.ColumnReference<>(columnHandle, columnHandle.getHiveColumnIndex(), typeManager.getType(columnHandle.getTypeSignature())));
                    }
                }
                else {
                    physicalColumnHandles.add(new HiveColumnHandle(
                            column.getName(),
                            toHiveType(column.getType()),
                            column.getType().getTypeSignature(),
                            nextMissingColumnIndex++,
                            REGULAR,
                            Optional.empty(),
                            Optional.empty()));
                }
            }

            TupleDomain<HiveColumnHandle> hiveColumnHandleTupleDomain = effectivePredicate.transform(column -> {
                IcebergOrcColumn icebergOrcColumn;
                if (fileOrcColumnByIcebergId.isEmpty()) {
                    icebergOrcColumn = fileOrcColumnsByName.get(column.getName());
                }
                else {
                    icebergOrcColumn = fileOrcColumnByIcebergId.get(column.getId());
                    if (icebergOrcColumn == null) {
                        // Cannot get orc column from 'fileOrcColumnByIcebergId', which means SchemaEvolution may have happened, so we get orc column by column name.
                        icebergOrcColumn = fileOrcColumnsByName.get(column.getName());
                    }
                }

                return new HiveColumnHandle(
                        column.getName(),
                        toHiveType(column.getType()),
                        column.getType().getTypeSignature(),
                        // Note: the HiveColumnHandle.hiveColumnIndex starts from '0' while the IcebergColumnHandle.id starts from '1'
                        icebergOrcColumn != null ? icebergOrcColumn.getOrcColumnId() : column.getId() - 1,
                        icebergOrcColumn != null ? icebergOrcColumn.getColumnType() : REGULAR,
                        Optional.empty(),
                        Optional.empty());
            });

            OrcPredicate predicate = new TupleDomainOrcPredicate<>(hiveColumnHandleTupleDomain, columnReferences.build(), orcBloomFiltersEnabled, Optional.of(domainCompactionThreshold));

            OrcAggregatedMemoryContext systemMemoryUsage = new HiveOrcAggregatedMemoryContext();
            OrcBatchRecordReader recordReader = reader.createBatchRecordReader(
                    includedColumns.build(),
                    predicate,
                    start,
                    length,
                    UTC,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE);

            return new OrcBatchPageSource(
                    recordReader,
                    orcDataSource,
                    physicalColumnHandles,
                    typeManager,
                    systemMemoryUsage,
                    stats,
                    runtimeStats);
        }
        catch (Exception e) {
            if (orcDataSource != null) {
                try {
                    orcDataSource.close();
                }
                catch (IOException ignored) {
                }
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = format("Error opening Iceberg split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e instanceof BlockMissingException) {
                throw new PrestoException(ICEBERG_MISSING_DATA, message, e);
            }
            throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static List<IcebergOrcColumn> getFileOrcColumns(OrcReader reader)
    {
        List<OrcType> orcTypes = reader.getFooter().getTypes();
        OrcType rootOrcType = orcTypes.get(ROOT_COLUMN_ID);

        List<IcebergOrcColumn> columnAttributes = ImmutableList.of();
        if (rootOrcType.getOrcTypeKind() == OrcType.OrcTypeKind.STRUCT) {
            columnAttributes = IntStream.range(0, rootOrcType.getFieldCount())
                    .mapToObj(fieldId -> new IcebergOrcColumn(
                            fieldId,
                            rootOrcType.getFieldTypeIndex(fieldId),
                            // We will filter out iceberg column by 'ORC_ICEBERG_ID_KEY' later,
                            // so we use 'Optional.empty()' temporarily.
                            Optional.empty(),
                            rootOrcType.getFieldName(fieldId),
                            REGULAR,
                            orcTypes.get(rootOrcType.getFieldTypeIndex(fieldId)).getOrcTypeKind(),
                            orcTypes.get(rootOrcType.getFieldTypeIndex(fieldId)).getAttributes()))
                    .collect(toImmutableList());
        }
        else if (rootOrcType.getOrcTypeKind() == OrcType.OrcTypeKind.LIST) {
            columnAttributes = ImmutableList.of(
                    new IcebergOrcColumn(
                            0,
                            rootOrcType.getFieldTypeIndex(0),
                            Optional.empty(),
                            "item",
                            REGULAR,
                            orcTypes.get(rootOrcType.getFieldTypeIndex(0)).getOrcTypeKind(),
                            orcTypes.get(rootOrcType.getFieldTypeIndex(0)).getAttributes()));
        }
        else if (rootOrcType.getOrcTypeKind() == OrcType.OrcTypeKind.MAP) {
            columnAttributes = ImmutableList.of(
                    new IcebergOrcColumn(
                            0,
                            rootOrcType.getFieldTypeIndex(0),
                            Optional.empty(),
                            "key",
                            REGULAR,
                            orcTypes.get(rootOrcType.getFieldTypeIndex(0)).getOrcTypeKind(),
                            orcTypes.get(rootOrcType.getFieldTypeIndex(0)).getAttributes()),
                    new IcebergOrcColumn(
                            1,
                            rootOrcType.getFieldTypeIndex(1),
                            Optional.empty(),
                            "value",
                            REGULAR,
                            orcTypes.get(rootOrcType.getFieldTypeIndex(1)).getOrcTypeKind(),
                            orcTypes.get(rootOrcType.getFieldTypeIndex(1)).getAttributes()));
        }
        else if (rootOrcType.getOrcTypeKind() == OrcType.OrcTypeKind.UNION) {
            columnAttributes = IntStream.range(0, rootOrcType.getFieldCount())
                    .mapToObj(fieldId -> new IcebergOrcColumn(
                            fieldId,
                            rootOrcType.getFieldTypeIndex(fieldId),
                            Optional.empty(),
                            "field" + fieldId,
                            REGULAR,
                            orcTypes.get(rootOrcType.getFieldTypeIndex(fieldId)).getOrcTypeKind(),
                            orcTypes.get(rootOrcType.getFieldTypeIndex(fieldId)).getAttributes()))
                    .collect(toImmutableList());
        }
        return columnAttributes;
    }
}
