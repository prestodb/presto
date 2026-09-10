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
package com.facebook.presto.ducklake.reader;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.UuidType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.split.DuckLakeSplit;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.parquet.ParquetPageSource;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.ParquetCorruptionException;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.RichColumnDescriptor;
import com.facebook.presto.parquet.cache.ParquetMetadataSource;
import com.facebook.presto.parquet.predicate.Predicate;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Function;

import static com.facebook.presto.common.Utils.nativeValueToBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_BAD_DATA;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_MISSING_DATA;
import static com.facebook.presto.ducklake.reader.DuckLakeTransformingPageSource.ChannelTransform.constant;
import static com.facebook.presto.ducklake.reader.DuckLakeTransformingPageSource.ChannelTransform.fixUuidByteOrder;
import static com.facebook.presto.ducklake.reader.DuckLakeTransformingPageSource.ChannelTransform.nanosToMillis;
import static com.facebook.presto.ducklake.reader.DuckLakeTransformingPageSource.ChannelTransform.passthrough;
import static com.facebook.presto.ducklake.reader.DuckLakeTransformingPageSource.ChannelTransform.rowId;
import static com.facebook.presto.hive.CacheQuota.NO_CACHE_CONSTRAINTS;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getParquetMaxReadBlockSize;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getReadNullMaskedParquetEncryptedValue;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isParquetBatchReaderVerificationEnabled;
import static com.facebook.presto.hive.HiveCommonSessionProperties.isParquetBatchReadsEnabled;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.createDecryptor;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.ParquetTypeUtils.getDescriptors;
import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetTypeByName;
import static com.facebook.presto.parquet.ParquetTypeUtils.lookupColumnByName;
import static com.facebook.presto.parquet.cache.MetadataReader.findFirstNonHiddenColumnId;
import static com.facebook.presto.parquet.predicate.PredicateUtils.buildPredicate;
import static com.facebook.presto.parquet.predicate.PredicateUtils.predicateMatches;
import static com.facebook.presto.parquet.reader.ColumnIndexFilterUtils.getColumnIndexStore;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * The Parquet read path for one {@link DuckLakeSplit}, adapted from {@code
 * IcebergPageSourceProvider#createParquetPageSource}: open the file, read its footer through the
 * cached {@link ParquetMetadataSource}, resolve each requested column against the file's Parquet
 * {@code field_id}s (recursing into nested types through {@link DuckLakeParquetFields}, falling
 * back to name matching only when the file carries no ids at all), build a {@link ParquetReader}
 * and a {@link ParquetPageSource}, then layer on the two wrappers that Iceberg does not need:
 * {@link DefaultValuePageSource} for a column absent from the file, whose {@code initial_default}
 * is converted to a native value by {@link DefaultValueParser} (which throws rather than silently
 * substituting {@code NULL} for a default it cannot represent faithfully), and {@link
 * DuckLakeTransformingPageSource} for the synthesized {@code $path}/{@code $row_id} columns, for
 * a {@code timestamp_ns} column (which Presto's Parquet reader has no native support for and
 * which is therefore read back as raw {@code BIGINT} nanoseconds and converted here), and for a
 * top-level {@code UUID} column (whose on-disk byte order Presto's Parquet UUID reader only gets
 * right for files its own writer produced; see {@link
 * DuckLakeTransformingPageSource.ChannelTransform#fixUuidByteOrder}).
 */
public class ParquetPageSourceFactory
{
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final ParquetMetadataSource parquetMetadataSource;

    @Inject
    public ParquetPageSourceFactory(
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            ParquetMetadataSource parquetMetadataSource)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.parquetMetadataSource = requireNonNull(parquetMetadataSource, "parquetMetadataSource is null");
    }

    public ConnectorPageSource createPageSource(
            ConnectorSession session,
            DuckLakeSplit split,
            String schemaName,
            String tableName,
            List<DuckLakeColumnHandle> columns,
            TupleDomain<DuckLakeColumnHandle> effectivePredicate)
    {
        return openParquetFile(session, split.getPath(), split.getStart(), split.getLength(), split.getRowIdStart(), schemaName, tableName, columns, effectivePredicate);
    }

    /**
     * Opens {@code path} in full (start 0, the given {@code fileSize} as the length) for a fixed
     * set of columns with no predicate and no row-id offset, addressing the file directly rather
     * than through a {@link DuckLakeSplit}. Used to read a DuckLake positional delete file, which
     * is just another Parquet file named by {@link com.facebook.presto.ducklake.split.DeleteFile}
     * rather than by a split.
     */
    public ConnectorPageSource openParquetFile(
            ConnectorSession session,
            String path,
            long fileSize,
            List<DuckLakeColumnHandle> columns,
            String schemaName,
            String tableName)
    {
        return openParquetFile(session, path, 0, fileSize, OptionalLong.empty(), schemaName, tableName, columns, TupleDomain.all());
    }

    private ConnectorPageSource openParquetFile(
            ConnectorSession session,
            String pathString,
            long start,
            long length,
            OptionalLong rowIdStart,
            String schemaName,
            String tableName,
            List<DuckLakeColumnHandle> columns,
            TupleDomain<DuckLakeColumnHandle> effectivePredicate)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        Path path = new Path(pathString);
        HdfsContext hdfsContext = new HdfsContext(session, schemaName, tableName);
        Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);
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

            // Mapping from DuckLake column id to the matching Parquet field, by field_id (spec
            // ss4.4: DuckDB stamps every top-level and nested field's Parquet field_id with the
            // DuckLake column_id). Falls back to name matching only when the file has no ids at
            // all (not something this fixture produces, but cheap to keep for a migrated file).
            Map<Integer, org.apache.parquet.schema.Type> parquetIdToField = fileSchema.getFields().stream()
                    .filter(field -> field.getId() != null)
                    .collect(ImmutableMap.toImmutableMap(field -> field.getId().intValue(), Function.identity()));

            Map<Long, org.apache.parquet.schema.Type> resolvedTypeByColumnId = new HashMap<>();
            for (DuckLakeColumnHandle column : columns) {
                if (isMetadataColumn(column)) {
                    continue;
                }
                resolveColumnType(parquetIdToField, fileSchema, column).ifPresent(type -> resolvedTypeByColumnId.put(column.getId(), type));
            }

            Optional<MessageType> messageType = resolvedTypeByColumnId.values().stream()
                    .map(type -> new MessageType(fileSchema.getName(), type))
                    .reduce(MessageType::union);
            MessageType requestedSchema = messageType.orElseGet(() -> new MessageType(fileSchema.getName(), ImmutableList.of()));

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate, resolvedTypeByColumnId);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath);
            final ParquetDataSource finalDataSource = dataSource;

            long nextStart = 0;
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
                    fileDecryptor,
                    Optional.empty());

            return buildPageSource(parquetReader, columns, resolvedTypeByColumnId, messageColumnIO, rowIdStart, path.toString());
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
            String message = format("Error opening DuckLake file %s (offset=%s, length=%s): %s", pathString, start, length, e.getMessage());
            if (e instanceof ParquetCorruptionException) {
                throw new PrestoException(DUCKLAKE_BAD_DATA, message, e);
            }
            if (e instanceof BlockMissingException) {
                throw new PrestoException(DUCKLAKE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    /**
     * Builds the channel list ({@code names}/{@code types}/{@code fields}), wraps the raw {@link
     * ParquetReader} in a {@link ParquetPageSource}, and layers on {@link DefaultValuePageSource}
     * and {@link DuckLakeTransformingPageSource} as needed.
     */
    private static ConnectorPageSource buildPageSource(
            ParquetReader parquetReader,
            List<DuckLakeColumnHandle> columns,
            Map<Long, org.apache.parquet.schema.Type> resolvedTypeByColumnId,
            MessageColumnIO messageColumnIO,
            OptionalLong rowIdStart,
            String pathString)
    {
        // $row_position and $row_id both want the ParquetReader's raw row-position channel;
        // ParquetPageSource allows only one, so prefer $row_position's own index (a plain
        // passthrough) and have $row_id (if also requested) derive from it in the wrapper.
        int rowPositionRequestedIndex = -1;
        int rowIdRequestedIndex = -1;
        for (int i = 0; i < columns.size(); i++) {
            DuckLakeColumnHandle column = columns.get(i);
            if (column.isRowPositionColumn()) {
                rowPositionRequestedIndex = i;
            }
            if (column.isRowIdColumn()) {
                rowIdRequestedIndex = i;
            }
        }
        int positionChannelIndex = rowPositionRequestedIndex >= 0 ? rowPositionRequestedIndex : rowIdRequestedIndex;
        OptionalInt rowPositionColumnIndex = positionChannelIndex >= 0 ? OptionalInt.of(positionChannelIndex) : OptionalInt.empty();

        ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> prestoTypesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
        ImmutableMap.Builder<Long, Object> defaultValues = ImmutableMap.builder();
        List<Integer> nanosChannels = new ArrayList<>();
        List<Integer> uuidChannels = new ArrayList<>();
        int pathChannel = -1;

        for (int i = 0; i < columns.size(); i++) {
            DuckLakeColumnHandle column = columns.get(i);
            namesBuilder.add(column.getName());

            if (column.isPathColumn()) {
                prestoTypesBuilder.add(column.getType());
                fieldsBuilder.add(Optional.empty());
                pathChannel = i;
                continue;
            }
            if (column.isRowIdColumn() || column.isRowPositionColumn()) {
                prestoTypesBuilder.add(BIGINT);
                fieldsBuilder.add(Optional.empty());
                continue;
            }

            org.apache.parquet.schema.Type parquetField = resolvedTypeByColumnId.get(column.getId());
            if (parquetField == null) {
                prestoTypesBuilder.add(column.getType());
                fieldsBuilder.add(Optional.empty());
                column.getDefaultValue().ifPresent(defaultValue ->
                        defaultValues.put(column.getId(), DefaultValueParser.parse(column, defaultValue)));
                continue;
            }

            boolean nanosTimestamp = isNanosTimestamp(column.getType(), parquetField);
            Type fieldType = nanosTimestamp ? BIGINT : column.getType();
            prestoTypesBuilder.add(fieldType);
            if (nanosTimestamp) {
                nanosChannels.add(i);
            }
            else if (column.getType() instanceof UuidType) {
                uuidChannels.add(i);
            }
            ColumnIO columnIO = lookupColumnByName(messageColumnIO, parquetField.getName());
            fieldsBuilder.add(DuckLakeParquetFields.constructField(fieldType, column.getColumnIdentity(), columnIO));
        }

        List<String> names = namesBuilder.build();
        List<Type> prestoTypes = prestoTypesBuilder.build();
        List<Optional<Field>> fields = fieldsBuilder.build();

        ConnectorPageSource pageSource = new ParquetPageSource(parquetReader, prestoTypes, fields, rowPositionColumnIndex, names, new RuntimeStats());

        Map<Long, Object> defaults = defaultValues.build();
        if (!defaults.isEmpty()) {
            pageSource = new DefaultValuePageSource(pageSource, columns, defaults);
        }

        boolean needsPath = pathChannel >= 0;
        boolean needsRowId = rowIdRequestedIndex >= 0;
        if (needsPath || needsRowId || !nanosChannels.isEmpty() || !uuidChannels.isEmpty()) {
            pageSource = new DuckLakeTransformingPageSource(
                    pageSource,
                    buildTransforms(columns, pathString, rowIdStart, positionChannelIndex, rowIdRequestedIndex, nanosChannels, uuidChannels));
        }

        return pageSource;
    }

    private static List<DuckLakeTransformingPageSource.ChannelTransform> buildTransforms(
            List<DuckLakeColumnHandle> columns,
            String pathString,
            OptionalLong rowIdStart,
            int positionChannelIndex,
            int rowIdRequestedIndex,
            List<Integer> nanosChannels,
            List<Integer> uuidChannels)
    {
        ImmutableList.Builder<DuckLakeTransformingPageSource.ChannelTransform> transforms = ImmutableList.builder();
        for (int i = 0; i < columns.size(); i++) {
            DuckLakeColumnHandle column = columns.get(i);
            if (column.isPathColumn()) {
                transforms.add(constant(nativeValueToBlock(VARCHAR, Slices.utf8Slice(pathString))));
            }
            else if (i == rowIdRequestedIndex) {
                transforms.add(rowId(positionChannelIndex, rowIdStart));
            }
            else if (nanosChannels.contains(i)) {
                transforms.add(nanosToMillis(i));
            }
            else if (uuidChannels.contains(i)) {
                transforms.add(fixUuidByteOrder(i));
            }
            else {
                transforms.add(passthrough(i));
            }
        }
        return transforms.build();
    }

    private static boolean isMetadataColumn(DuckLakeColumnHandle column)
    {
        return column.isPathColumn() || column.isRowIdColumn() || column.isRowPositionColumn();
    }

    private static Optional<org.apache.parquet.schema.Type> resolveColumnType(
            Map<Integer, org.apache.parquet.schema.Type> parquetIdToField,
            MessageType fileSchema,
            DuckLakeColumnHandle column)
    {
        if (parquetIdToField.isEmpty()) {
            // A migrated table with no field ids at all: match by name instead.
            return Optional.ofNullable(getParquetTypeByName(column.getName(), fileSchema));
        }
        return Optional.ofNullable(parquetIdToField.get(toIntExact(column.getId())));
    }

    /**
     * True when {@code column}'s Presto type is {@code TIMESTAMP} but the matching Parquet
     * primitive is an INT64 stamped only with the new-style {@code TimestampLogicalTypeAnnotation}
     * at nanosecond precision and no legacy converted type. Presto's Parquet reader has no
     * dispatch for that combination (see {@code ColumnReaderFactory}) and would otherwise read the
     * raw nanosecond value straight into a {@code TIMESTAMP} block; the caller reads this column as
     * {@code BIGINT} instead and converts nanoseconds to milliseconds in {@link
     * DuckLakeTransformingPageSource}.
     */
    private static boolean isNanosTimestamp(Type prestoType, org.apache.parquet.schema.Type parquetType)
    {
        if (!(prestoType instanceof TimestampType) || !parquetType.isPrimitive()) {
            return false;
        }
        PrimitiveType primitiveType = parquetType.asPrimitiveType();
        if (primitiveType.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT64) {
            return false;
        }
        LogicalTypeAnnotation annotation = primitiveType.getLogicalTypeAnnotation();
        return annotation instanceof TimestampLogicalTypeAnnotation && ((TimestampLogicalTypeAnnotation) annotation).getUnit() == TimeUnit.NANOS;
    }

    /**
     * Parquet row-group/page pruning domain, restricted to top-level primitive columns of a type
     * Parquet keeps statistics for, keyed by the descriptor at the file's own field name (not the
     * DuckLake column name, since a rename keeps the id but not the name).
     */
    private static TupleDomain<ColumnDescriptor> getParquetTupleDomain(
            Map<List<String>, RichColumnDescriptor> descriptorsByPath,
            TupleDomain<DuckLakeColumnHandle> effectivePredicate,
            Map<Long, org.apache.parquet.schema.Type> resolvedTypeByColumnId)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().get().forEach((column, domain) -> {
            if (isMetadataColumn(column) || !isPushdownEligible(column.getType())) {
                return;
            }
            org.apache.parquet.schema.Type parquetType = resolvedTypeByColumnId.get(column.getId());
            if (parquetType == null || !parquetType.isPrimitive()) {
                return;
            }
            RichColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(parquetType.getName()));
            if (descriptor != null) {
                predicate.put(descriptor, domain);
            }
        });
        return TupleDomain.withColumnDomains(predicate.build());
    }

    private static boolean isPushdownEligible(Type type)
    {
        return type instanceof BooleanType ||
                type instanceof TinyintType ||
                type instanceof SmallintType ||
                type instanceof IntegerType ||
                type instanceof BigintType ||
                type instanceof RealType ||
                type instanceof DoubleType ||
                type instanceof DecimalType ||
                type instanceof VarcharType ||
                type instanceof DateType;
    }
}
