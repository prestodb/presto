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
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
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
import com.facebook.presto.hive.HivePartitionKey;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.orc.HdfsOrcDataSource;
import com.facebook.presto.hive.orc.OrcBatchPageSource;
import com.facebook.presto.hive.orc.ProjectionBasedDwrfKeyProvider;
import com.facebook.presto.hive.parquet.ParquetPageSource;
import com.facebook.presto.iceberg.changelog.ChangelogPageSource;
import com.facebook.presto.iceberg.delete.DeleteFile;
import com.facebook.presto.iceberg.delete.DeleteFilter;
import com.facebook.presto.iceberg.delete.PositionDeleteFilter;
import com.facebook.presto.iceberg.delete.RowPredicate;
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
import com.facebook.presto.parquet.cache.ParquetMetadataSource;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.base.Suppliers;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Conversions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.CacheQuota.NO_CACHE_CONSTRAINTS;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcLazyReadSmallRanges;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxBufferSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxMergeDistance;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcMaxReadBlockSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcStreamBufferSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getOrcTinyStripeThreshold;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getParquetMaxReadBlockSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getReadNullMaskedParquetEncryptedValue;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isOrcBloomFiltersEnabled;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isOrcZstdJniDecompressionEnabled;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isParquetBatchReaderVerificationEnabled;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isParquetBatchReadsEnabled;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.createDecryptor;
import static com.facebook.presto.iceberg.IcebergColumnHandle.getPushedDownSubfield;
import static com.facebook.presto.iceberg.IcebergColumnHandle.isPushedDownSubfield;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_MISSING_DATA;
import static com.facebook.presto.iceberg.IcebergOrcColumn.ROOT_COLUMN_ID;
import static com.facebook.presto.iceberg.TypeConverter.ORC_ICEBERG_ID_KEY;
import static com.facebook.presto.iceberg.TypeConverter.toHiveType;
import static com.facebook.presto.iceberg.delete.EqualityDeleteFilter.readEqualityDeletes;
import static com.facebook.presto.iceberg.delete.PositionDeleteFilter.readPositionDeletes;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetTypeByName;
import static com.facebook.presto.parquet.ParquetTypeUtils.getSubfieldType;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.facebook.presto.parquet.ParquetTypeUtils.nestedColumnPath;
import static com.facebook.presto.parquet.cache.MetadataReader.findFirstNonHiddenColumnId;
import static com.facebook.presto.parquet.predicate.PredicateUtils.buildPredicate;
import static com.facebook.presto.parquet.predicate.PredicateUtils.predicateMatches;
import static com.facebook.presto.parquet.reader.ColumnIndexFilterUtils.getColumnIndexStore;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.FileContent.EQUALITY_DELETES;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.apache.parquet.io.ColumnIOConverter.constructField;
import static org.apache.parquet.io.ColumnIOConverter.findNestedColumnIO;

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

    private final ParquetMetadataSource parquetMetadataSource;

    @Inject
    public IcebergPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            TypeManager typeManager,
            OrcFileTailSource orcFileTailSource,
            StripeMetadataSourceFactory stripeMetadataSourceFactory,
            HiveDwrfEncryptionProvider dwrfEncryptionProvider,
            HiveClientConfig hiveClientConfig,
            ParquetMetadataSource parquetMetadataSource)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.orcFileTailSource = requireNonNull(orcFileTailSource, "orcFileTailSource is null");
        this.stripeMetadataSourceFactory = requireNonNull(stripeMetadataSourceFactory, "stripeMetadataSourceFactory is null");
        this.dwrfEncryptionProvider = requireNonNull(dwrfEncryptionProvider, "DwrfEncryptionProvider is null").toDwrfEncryptionProvider();
        this.hiveClientConfig = requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        this.parquetMetadataSource = requireNonNull(parquetMetadataSource, "parquetMetadataSource is null");
    }

    private static ConnectorPageSourceWithRowPositions createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            ConnectorSession session,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<IcebergColumnHandle> regularColumns,
            TupleDomain<IcebergColumnHandle> effectivePredicate,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetMetadataSource parquetMetadataSource)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        String user = session.getUser();
        boolean readMaskedValue = getReadNullMaskedParquetEncryptedValue(session);

        ParquetDataSource dataSource = null;
        try {
            ExtendedFileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            long fileSize = fileStatus.getLen();
            long modificationTime = fileStatus.getModificationTime();
            HiveFileContext hiveFileContext = new HiveFileContext(
                    true,
                    NO_CACHE_CONSTRAINTS,
                    Optional.empty(),
                    OptionalLong.of(fileSize),
                    OptionalLong.of(start),
                    OptionalLong.of(length),
                    modificationTime,
                    false);
            FSDataInputStream inputStream = fileSystem.openFile(path, hiveFileContext);
            // Lambda expression below requires final variable, so we define a new variable parquetDataSource.
            final ParquetDataSource parquetDataSource = buildHdfsParquetDataSource(inputStream, path, fileFormatDataSourceStats);
            dataSource = parquetDataSource;
            Optional<InternalFileDecryptor> fileDecryptor = createDecryptor(configuration, path);
            ParquetMetadata parquetMetadata = hdfsEnvironment.doAs(user, () -> parquetMetadataSource.getParquetMetadata(
                    parquetDataSource,
                    fileSize,
                    hiveFileContext.isCacheable(),
                    hiveFileContext.getModificationTime(),
                    fileDecryptor,
                    readMaskedValue).getParquetMetadata());
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            // Mapping from Iceberg field ID to Parquet fields.
            Map<Integer, org.apache.parquet.schema.Type> parquetIdToField = fileSchema.getFields().stream()
                    .filter(field -> field.getId() != null)
                    .collect(toImmutableMap(field -> field.getId().intValue(), Function.identity()));

            Optional<MessageType> messageType = regularColumns.stream()
                    .map(column -> getColumnType(parquetIdToField, fileSchema, column))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(type -> new MessageType(fileSchema.getName(), type))
                    .reduce(MessageType::union);

            MessageType requestedSchema = messageType.orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));
            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);
            final ParquetDataSource finalDataSource = dataSource;

            long nextStart = 0;
            Optional<Long> startRowPosition = Optional.empty();
            Optional<Long> endRowPosition = Optional.empty();
            ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();
            List<BlockMetaData> blocks = new ArrayList<>();
            List<ColumnIndexStore> blockIndexStores = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                Optional<Integer> firstIndex = findFirstNonHiddenColumnId(block);
                if (firstIndex.isPresent()) {
                    long firstDataPage = block.getColumns().get(firstIndex.get()).getFirstDataPageOffset();
                    Optional<ColumnIndexStore> columnIndexStore = getColumnIndexStore(parquetPredicate, finalDataSource, block, descriptorsByPath, false);
                    if ((firstDataPage >= start) && (firstDataPage < (start + length)) &&
                            predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain, columnIndexStore, false, Optional.of(session.getWarningCollector()))) {
                        blocks.add(block);
                        blockIndexStores.add(columnIndexStore.orElse(null));
                        blockStarts.add(nextStart);
                        if (!startRowPosition.isPresent()) {
                            startRowPosition = Optional.of(nextStart);
                        }
                        endRowPosition = Optional.of(nextStart + block.getRowCount());
                    }
                    nextStart += block.getRowCount();
                }
            }

            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    messageColumnIO,
                    blocks,
                    Optional.of(blockStarts.build()),
                    dataSource,
                    systemMemoryContext,
                    getParquetMaxReadBlockSize(session),
                    isParquetBatchReadsEnabled(session),
                    isParquetBatchReaderVerificationEnabled(session),
                    parquetPredicate,
                    blockIndexStores,
                    false,
                    fileDecryptor);

            ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> prestoTypes = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
            List<Boolean> isRowPositionList = new ArrayList<>();
            for (int columnIndex = 0; columnIndex < regularColumns.size(); columnIndex++) {
                IcebergColumnHandle column = regularColumns.get(columnIndex);
                namesBuilder.add(column.getName());

                Type prestoType = column.getType();

                prestoTypes.add(prestoType);

                if (column.getColumnType() == IcebergColumnHandle.ColumnType.SYNTHESIZED) {
                    Subfield pushedDownSubfield = getPushedDownSubfield(column);
                    List<String> nestedColumnPath = nestedColumnPath(pushedDownSubfield);
                    Optional<ColumnIO> columnIO = findNestedColumnIO(lookupColumnByName(messageColumnIO, pushedDownSubfield.getRootName()), nestedColumnPath);
                    if (columnIO.isPresent()) {
                        internalFields.add(constructField(prestoType, columnIO.get()));
                    }
                    else {
                        internalFields.add(Optional.empty());
                    }
                }
                else {
                    Optional<org.apache.parquet.schema.Type> parquetField = getColumnType(parquetIdToField, fileSchema, column);
                    if (!parquetField.isPresent()) {
                        internalFields.add(Optional.empty());
                    }
                    else {
                        internalFields.add(constructField(column.getType(), messageColumnIO.getChild(parquetField.get().getName())));
                    }
                }
                isRowPositionList.add(column.isRowPositionColumn());
            }

            return new ConnectorPageSourceWithRowPositions(
                    new ParquetPageSource(parquetReader, prestoTypes.build(), internalFields.build(), isRowPositionList, namesBuilder.build(), new RuntimeStats()),
                    startRowPosition,
                    endRowPosition);
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

    public static Optional<org.apache.parquet.schema.Type> getColumnType(
            Map<Integer, org.apache.parquet.schema.Type> parquetIdToField,
            MessageType messageType,
            IcebergColumnHandle column)
    {
        if (isPushedDownSubfield(column)) {
            Subfield pushedDownSubfield = getPushedDownSubfield(column);
            return getSubfieldType(messageType, pushedDownSubfield.getRootName(), nestedColumnPath(pushedDownSubfield));
        }

        if (parquetIdToField.isEmpty()) {
            // This is a migrated table
            return Optional.ofNullable(getParquetTypeByName(column.getName(), messageType));
        }
        return Optional.ofNullable(parquetIdToField.get(column.getId()));
    }

    private static HiveColumnHandle.ColumnType getHiveColumnHandleColumnType(IcebergColumnHandle.ColumnType columnType)
    {
        switch (columnType) {
            case REGULAR:
                return REGULAR;
            case SYNTHESIZED:
                return SYNTHESIZED;
        }

        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unknown ColumnType: " + columnType);
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

    private static ConnectorPageSourceWithRowPositions createBatchOrcPageSource(
            HdfsEnvironment hdfsEnvironment,
            String user,
            Configuration configuration,
            Path path,
            long start,
            long length,
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
            ExtendedFileSystem fileSystem = hdfsEnvironment.getFileSystem(user, path, configuration);
            FileStatus fileStatus = fileSystem.getFileStatus(path);
            long fileSize = fileStatus.getLen();
            long modificationTime = fileStatus.getModificationTime();
            HiveFileContext hiveFileContext = new HiveFileContext(
                    true,
                    NO_CACHE_CONSTRAINTS,
                    Optional.empty(),
                    OptionalLong.of(fileSize),
                    OptionalLong.of(start),
                    OptionalLong.of(length),
                    modificationTime,
                    false);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(user, () -> fileSystem.openFile(path, hiveFileContext));
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
            List<Boolean> isRowPositionList = new ArrayList<>();
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
                            getHiveColumnHandleColumnType(column.getColumnType()),
                            column.getComment(),
                            column.getRequiredSubfields(),
                            Optional.empty()));
                }
                isRowPositionList.add(column.isRowPositionColumn());
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

            return new ConnectorPageSourceWithRowPositions(
                    new OrcBatchPageSource(
                            recordReader,
                            orcDataSource,
                            physicalColumnHandles,
                            typeManager,
                            systemMemoryUsage,
                            stats,
                            runtimeStats,
                            isRowPositionList),
                    Optional.empty(),
                    Optional.empty());
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

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableLayoutHandle layout,
            List<ColumnHandle> desiredColumns,
            SplitContext splitContext)
    {
        IcebergTableLayoutHandle icebergLayout = (IcebergTableLayoutHandle) layout;
        if (icebergLayout.isPushdownFilterEnabled()) {
            throw new PrestoException(NOT_SUPPORTED, "Filter Pushdown not supported for Iceberg Java Connector");
        }

        IcebergSplit split = (IcebergSplit) connectorSplit;
        IcebergTableHandle table = icebergLayout.getTable();

        List<ColumnHandle> columns = desiredColumns;
        if (split.getChangelogSplitInfo().isPresent()) {
            columns = (List<ColumnHandle>) (List<?>) split.getChangelogSplitInfo().get().getIcebergColumns();
        }

        List<IcebergColumnHandle> icebergColumns = columns.stream()
                .map(IcebergColumnHandle.class::cast)
                .collect(toImmutableList());

        Map<Integer, HivePartitionKey> partitionKeys = split.getPartitionKeys();

        List<IcebergColumnHandle> regularColumns = columns.stream()
                .map(IcebergColumnHandle.class::cast)
                .filter(column -> column.getColumnType() != PARTITION_KEY &&
                        !partitionKeys.containsKey(column.getId()) &&
                        !IcebergMetadataColumn.isMetadataColumnId(column.getId()))
                .collect(Collectors.toList());

        Optional<String> tableSchemaJson = table.getTableSchemaJson();
        verify(tableSchemaJson.isPresent(), "tableSchemaJson is null");
        Schema tableSchema = SchemaParser.fromJson(tableSchemaJson.get());

        boolean equalityDeletesRequired = table.getIcebergTableName().getTableType() == IcebergTableType.DATA;
        Set<IcebergColumnHandle> deleteFilterRequiredColumns = requiredColumnsForDeletes(tableSchema, split.getDeletes(), equalityDeletesRequired);

        deleteFilterRequiredColumns.stream()
                .filter(not(icebergColumns::contains))
                .forEach(regularColumns::add);

        // TODO: pushdownFilter for icebergLayout
        HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getIcebergTableName().getTableName());
        ConnectorPageSourceWithRowPositions connectorPageSourceWithRowPositions = createDataPageSource(
                session,
                hdfsContext,
                new Path(split.getPath()),
                split.getStart(),
                split.getLength(),
                split.getFileFormat(),
                regularColumns,
                table.getPredicate(),
                splitContext.isCacheable());
        ConnectorPageSource dataPageSource = connectorPageSourceWithRowPositions.getConnectorPageSource();

        Supplier<Optional<RowPredicate>> deletePredicate = Suppliers.memoize(() -> {
            // If equality deletes are optimized into a join they don't need to be applied here
            List<DeleteFile> deletesToApply = split
                    .getDeletes()
                    .stream()
                    .filter(deleteFile -> deleteFile.content() == POSITION_DELETES || equalityDeletesRequired)
                    .collect(toImmutableList());
            List<DeleteFilter> deleteFilters = readDeletes(
                    session,
                    tableSchema,
                    split.getPath(),
                    deletesToApply,
                    connectorPageSourceWithRowPositions.getStartRowPosition(),
                    connectorPageSourceWithRowPositions.getEndRowPosition());
            return deleteFilters.stream()
                    .map(filter -> filter.createPredicate(regularColumns))
                    .reduce(RowPredicate::and);
        });

        HashMap<Integer, Object> metadataValues = new HashMap<>();
        for (IcebergColumnHandle icebergColumn : icebergColumns) {
            if (icebergColumn.isPathColumn()) {
                metadataValues.put(icebergColumn.getColumnIdentity().getId(), utf8Slice(split.getPath()));
            }
            else if (icebergColumn.isDataSequenceNumberColumn()) {
                metadataValues.put(icebergColumn.getColumnIdentity().getId(), split.getDataSequenceNumber());
            }
        }

        ConnectorPageSource dataSource = new IcebergPageSource(icebergColumns, metadataValues, partitionKeys, dataPageSource, deletePredicate);
        if (split.getChangelogSplitInfo().isPresent()) {
            dataSource = new ChangelogPageSource(dataSource, split.getChangelogSplitInfo().get(), (List<IcebergColumnHandle>) (List<?>) desiredColumns, icebergColumns);
        }
        return dataSource;
    }

    private Set<IcebergColumnHandle> requiredColumnsForDeletes(Schema schema, List<DeleteFile> deletes, boolean equalityDeletesRequired)
    {
        ImmutableSet.Builder<IcebergColumnHandle> requiredColumns = ImmutableSet.builder();
        for (DeleteFile deleteFile : deletes) {
            if (deleteFile.content() == POSITION_DELETES) {
                requiredColumns.add(IcebergColumnHandle.create(ROW_POSITION, typeManager, IcebergColumnHandle.ColumnType.REGULAR));
            }
            else if (deleteFile.content() == EQUALITY_DELETES && equalityDeletesRequired) {
                deleteFile.equalityFieldIds().stream()
                        .map(id -> IcebergColumnHandle.create(schema.findField(id), typeManager, IcebergColumnHandle.ColumnType.REGULAR))
                        .forEach(requiredColumns::add);
            }
        }

        return requiredColumns.build();
    }

    private List<DeleteFilter> readDeletes(
            ConnectorSession session,
            Schema schema,
            String dataFilePath,
            List<DeleteFile> deleteFiles,
            Optional<Long> startRowPosition,
            Optional<Long> endRowPosition)
    {
        verify(startRowPosition.isPresent() == endRowPosition.isPresent(), "startRowPosition and endRowPosition must be specified together");

        Slice targetPath = utf8Slice(dataFilePath);
        List<DeleteFilter> filters = new ArrayList<>();
        LongBitmapDataProvider deletedRows = new Roaring64Bitmap();

        IcebergColumnHandle deleteFilePath = IcebergColumnHandle.create(DELETE_FILE_PATH, typeManager, IcebergColumnHandle.ColumnType.REGULAR);
        IcebergColumnHandle deleteFilePos = IcebergColumnHandle.create(DELETE_FILE_POS, typeManager, IcebergColumnHandle.ColumnType.REGULAR);
        List<IcebergColumnHandle> deleteColumns = ImmutableList.of(deleteFilePath, deleteFilePos);
        TupleDomain<IcebergColumnHandle> deleteDomain = TupleDomain.fromFixedValues(ImmutableMap.of(deleteFilePath, NullableValue.of(VARCHAR, targetPath)));
        if (startRowPosition.isPresent()) {
            Range positionRange = Range.range(deleteFilePos.getType(), startRowPosition.get(), true, endRowPosition.get(), true);
            TupleDomain<IcebergColumnHandle> positionDomain = TupleDomain.withColumnDomains(ImmutableMap.of(deleteFilePos, Domain.create(ValueSet.ofRanges(positionRange), false)));
            deleteDomain = deleteDomain.intersect(positionDomain);
        }

        for (DeleteFile delete : deleteFiles) {
            if (delete.content() == POSITION_DELETES) {
                if (startRowPosition.isPresent()) {
                    byte[] lowerBoundBytes = delete.getLowerBounds().get(DELETE_FILE_POS.fieldId());
                    Optional<Long> positionLowerBound = Optional.ofNullable(lowerBoundBytes)
                            .map(bytes -> Conversions.fromByteBuffer(DELETE_FILE_POS.type(), ByteBuffer.wrap(bytes)));

                    byte[] upperBoundBytes = delete.getUpperBounds().get(DELETE_FILE_POS.fieldId());
                    Optional<Long> positionUpperBound = Optional.ofNullable(upperBoundBytes)
                            .map(bytes -> Conversions.fromByteBuffer(DELETE_FILE_POS.type(), ByteBuffer.wrap(bytes)));

                    if ((positionLowerBound.isPresent() && positionLowerBound.get() > endRowPosition.get()) ||
                            (positionUpperBound.isPresent() && positionUpperBound.get() < startRowPosition.get())) {
                        continue;
                    }
                }

                try (ConnectorPageSource pageSource = openDeletes(session, delete, deleteColumns, deleteDomain)) {
                    readPositionDeletes(pageSource, targetPath, deletedRows);
                }
                catch (IOException e) {
                    throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, format("Cannot open Iceberg delete file: %s", delete.path()), e);
                }
            }
            else if (delete.content() == EQUALITY_DELETES) {
                List<Integer> fieldIds = delete.equalityFieldIds();
                verify(!fieldIds.isEmpty(), "equality field IDs are missing");
                List<IcebergColumnHandle> columns = fieldIds.stream()
                        .map(id -> IcebergColumnHandle.create(schema.findField(id), typeManager, IcebergColumnHandle.ColumnType.REGULAR))
                        .collect(toImmutableList());

                try (ConnectorPageSource pageSource = openDeletes(session, delete, columns, TupleDomain.all())) {
                    filters.add(readEqualityDeletes(pageSource, columns, schema));
                }
                catch (IOException e) {
                    throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, format("Cannot open Iceberg delete file: %s", delete.path()), e);
                }
            }
            else {
                throw new VerifyException("Unknown delete content: " + delete.content());
            }
        }

        if (!deletedRows.isEmpty()) {
            filters.add(new PositionDeleteFilter(deletedRows));
        }

        return filters;
    }

    private ConnectorPageSource openDeletes(
            ConnectorSession session,
            DeleteFile delete,
            List<IcebergColumnHandle> columns,
            TupleDomain<IcebergColumnHandle> tupleDomain)
    {
        return createDataPageSource(
                session,
                new HdfsContext(session),
                new Path(delete.path()),
                0,
                delete.fileSizeInBytes(),
                delete.format(),
                columns,
                tupleDomain,
                false)
                .getConnectorPageSource();
    }

    private ConnectorPageSourceWithRowPositions createDataPageSource(
            ConnectorSession session,
            HdfsContext hdfsContext,
            Path path,
            long start,
            long length,
            FileFormat fileFormat,
            List<IcebergColumnHandle> dataColumns,
            TupleDomain<IcebergColumnHandle> predicate,
            boolean isCacheable)
    {
        switch (fileFormat) {
            case PARQUET:
                return createParquetPageSource(
                        hdfsEnvironment,
                        session,
                        hdfsEnvironment.getConfiguration(hdfsContext, path),
                        path,
                        start,
                        length,
                        dataColumns,
                        predicate,
                        fileFormatDataSourceStats,
                        parquetMetadataSource);
            case ORC:
                OrcReaderOptions readerOptions = OrcReaderOptions.builder()
                        .withMaxMergeDistance(getOrcMaxMergeDistance(session))
                        .withTinyStripeThreshold(getOrcTinyStripeThreshold(session))
                        .withMaxBlockSize(getOrcMaxReadBlockSize(session))
                        .withZstdJniDecompressionEnabled(isOrcZstdJniDecompressionEnabled(session))
                        .build();

                // TODO: Implement EncryptionInformation in IcebergSplit instead of Optional.empty()
                return createBatchOrcPageSource(
                        hdfsEnvironment,
                        session.getUser(),
                        hdfsEnvironment.getConfiguration(hdfsContext, path),
                        path,
                        start,
                        length,
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

    private static final class ConnectorPageSourceWithRowPositions
    {
        private final ConnectorPageSource connectorPageSource;
        private final Optional<Long> startRowPosition;
        private final Optional<Long> endRowPosition;

        public ConnectorPageSourceWithRowPositions(
                ConnectorPageSource connectorPageSource,
                Optional<Long> startRowPosition,
                Optional<Long> endRowPosition)
        {
            this.connectorPageSource = requireNonNull(connectorPageSource, "connectorPageSource is null");
            this.startRowPosition = requireNonNull(startRowPosition, "startRowPosition is null");
            this.endRowPosition = requireNonNull(endRowPosition, "endRowPosition is null");
        }

        public ConnectorPageSource getConnectorPageSource()
        {
            return connectorPageSource;
        }

        public Optional<Long> getStartRowPosition()
        {
            return startRowPosition;
        }

        public Optional<Long> getEndRowPosition()
        {
            return endRowPosition;
        }
    }
}
