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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.PartitionTransforms.ColumnTransform;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageIndexer;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.facebook.presto.common.type.Decimals.readBigDecimal;
import static com.facebook.presto.hive.util.ConfigurationUtils.toJobConf;
import static com.facebook.presto.iceberg.FileContent.DATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_TOO_MANY_OPEN_PARTITIONS;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_WRITER_OPEN_ERROR;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.PartitionTransforms.getColumnTransform;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class IcebergPageSink
        implements ConnectorPageSink
{
    private static final int MAX_PAGE_POSITIONS = 4096;

    @SuppressWarnings({"FieldCanBeLocal", "FieldMayBeStatic"})
    private final int maxOpenWriters;
    private final Schema outputSchema;
    private final PartitionSpec partitionSpec;
    private final LocationProvider locationProvider;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final JobConf jobConf;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final FileFormat fileFormat;
    private final PagePartitioner pagePartitioner;
    private final Table table;

    private final List<WriteContext> writers = new ArrayList<>();

    private long writtenBytes;
    private long systemMemoryUsage;
    private long validationCpuNanos;

    private final List<SortField> sortOrder;
    private final Path tempDirectory;
    private final List<Type> columnTypes;
    private final List<Integer> sortColumnIndexes;
    private final List<SortOrder> sortOrders;
    private final SortParameters sortParameters;

    public IcebergPageSink(
            Table table,
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            List<IcebergColumnHandle> inputColumns,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            FileFormat fileFormat,
            int maxOpenWriters,
            List<SortField> sortOrder,
            SortParameters sortParameters)
    {
        requireNonNull(inputColumns, "inputColumns is null");
        this.table = requireNonNull(table, "table is null");
        this.outputSchema = table.schema();
        this.partitionSpec = table.spec();
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        this.jobConf = toJobConf(hdfsEnvironment.getConfiguration(hdfsContext, new Path(locationProvider.newDataLocation("data-file"))));
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.session = requireNonNull(session, "session is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.maxOpenWriters = maxOpenWriters;
        this.pagePartitioner = new PagePartitioner(pageIndexerFactory,
                toPartitionColumns(inputColumns, partitionSpec),
                session);
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
        String tempDirectoryPath = locationProvider.newDataLocation("sort-tmp-files");
        this.tempDirectory = new Path(tempDirectoryPath);
        this.columnTypes = getColumns(outputSchema, partitionSpec, requireNonNull(sortParameters.getTypeManager(), "typeManager is null")).stream()
                .map(IcebergColumnHandle::getType)
                .collect(toImmutableList());
        this.sortParameters = sortParameters;
        if (!sortOrder.isEmpty()) {
            ImmutableList.Builder<Integer> sortColumnIndexes = ImmutableList.builder();
            ImmutableList.Builder<SortOrder> sortOrders = ImmutableList.builder();
            for (SortField sortField : sortOrder) {
                Types.NestedField column = outputSchema.findField(sortField.getSourceColumnId());
                if (column == null) {
                    throw new PrestoException(ICEBERG_INVALID_METADATA, "Unable to find sort field source column in the table schema: " + sortField);
                }
                sortColumnIndexes.add(outputSchema.columns().indexOf(column));
                sortOrders.add(sortField.getSortOrder());
            }
            this.sortColumnIndexes = sortColumnIndexes.build();
            this.sortOrders = sortOrders.build();
        }
        else {
            this.sortColumnIndexes = ImmutableList.of();
            this.sortOrders = ImmutableList.of();
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryUsage;
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        hdfsEnvironment.doAs(session.getUser(), () -> doAppend(page));

        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Collection<Slice> commitTasks = new ArrayList<>();

        for (WriteContext context : writers) {
            context.getWriter().commit();

            CommitTaskData task = new CommitTaskData(
                    context.getPath().toString(),
                    context.writer.getFileSizeInBytes(),
                    new MetricsWrapper(context.writer.getMetrics()),
                    partitionSpec.specId(),
                    context.getPartitionData().map(PartitionData::toJson),
                    fileFormat,
                    null,
                    DATA);

            commitTasks.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));
        }

        writtenBytes = writers.stream()
                .mapToLong(writer -> writer.getWriter().getWrittenBytes())
                .sum();
        validationCpuNanos = writers.stream()
                .mapToLong(writer -> writer.getWriter().getValidationCpuNanos())
                .sum();

        return completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        RuntimeException error = null;
        for (WriteContext context : writers) {
            try {
                if (context != null) {
                    context.getWriter().rollback();
                }
            }
            catch (Throwable t) {
                if (error == null) {
                    error = new RuntimeException("Exception during rollback");
                }
                error.addSuppressed(t);
            }
        }
        if (error != null) {
            throw error;
        }
    }

    private void doAppend(Page page)
    {
        while (page.getPositionCount() > MAX_PAGE_POSITIONS) {
            Page chunk = page.getRegion(0, MAX_PAGE_POSITIONS);
            page = page.getRegion(MAX_PAGE_POSITIONS, page.getPositionCount() - MAX_PAGE_POSITIONS);
            writePage(chunk);
        }

        writePage(page);
    }

    private void writePage(Page page)
    {
        int[] writerIndexes = getWriterIndexes(page);

        // position count for each writer
        int[] sizes = new int[writers.size()];
        for (int index : writerIndexes) {
            sizes[index]++;
        }

        // record which positions are used by which writer
        int[][] writerPositions = new int[writers.size()][];
        int[] counts = new int[writers.size()];

        for (int position = 0; position < page.getPositionCount(); position++) {
            int index = writerIndexes[position];

            int count = counts[index];
            if (count == 0) {
                writerPositions[index] = new int[sizes[index]];
            }
            writerPositions[index][count] = position;
            counts[index]++;
        }

        // invoke the writers
        for (int index = 0; index < writerPositions.length; index++) {
            int[] positions = writerPositions[index];
            if (positions == null) {
                continue;
            }

            // if write is partitioned across multiple writers, filter page using dictionary blocks
            Page pageForWriter = page;
            if (positions.length != page.getPositionCount()) {
                verify(positions.length == counts[index]);
                pageForWriter = pageForWriter.getPositions(positions, 0, positions.length);
            }

            IcebergFileWriter writer = writers.get(index).getWriter();

            long currentWritten = writer.getWrittenBytes();
            long currentMemory = writer.getSystemMemoryUsage();

            writer.appendRows(pageForWriter);

            writtenBytes += (writer.getWrittenBytes() - currentWritten);
            systemMemoryUsage += (writer.getSystemMemoryUsage() - currentMemory);
        }
    }

    private int[] getWriterIndexes(Page page)
    {
        int[] writerIndexes = pagePartitioner.partitionPage(page);

        if (pagePartitioner.getMaxIndex() >= maxOpenWriters) {
            throw new PrestoException(ICEBERG_TOO_MANY_OPEN_PARTITIONS, format("Exceeded limit of %s open writers for partitions", maxOpenWriters));
        }

        // expand writers list to new size
        while (writers.size() <= pagePartitioner.getMaxIndex()) {
            writers.add(null);
        }

        // create missing writers
        Page transformedPage = pagePartitioner.getTransformedPage();
        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = writerIndexes[position];
            WriteContext writer = writers.get(writerIndex);
            if (writer != null) {
                continue;
            }

            Optional<PartitionData> partitionData = getPartitionData(pagePartitioner.getColumns(), transformedPage, position);

            String fileName = fileFormat.addExtension(randomUUID().toString());
            Path outputPath = partitionData.map(partition -> new Path(locationProvider.newDataLocation(partitionSpec, partition, fileName)))
                    .orElse(new Path(locationProvider.newDataLocation(fileName)));

            try {
                FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), outputPath, jobConf);
                if (!sortOrder.isEmpty()) {
                    Path tempFilePrefix = new Path(tempDirectory, format("sorting-file-writer-%s-%s", session.getQueryId(), randomUUID()));
                    WriteContext writerContext = createWriter(partitionData, outputPath);
                    IcebergFileWriter sortedFileWriter = new IcebergSortingFileWriter(
                            fileSystem,
                            tempFilePrefix,
                            writerContext.getWriter(),
                            columnTypes,
                            sortColumnIndexes,
                            sortOrders,
                            false,
                            session,
                            sortParameters);
                    writer = new WriteContext(sortedFileWriter, outputPath, partitionData);
                }
                else {
                    writer = createWriter(partitionData, outputPath);
                }
                writers.set(writerIndex, writer);
            }
            catch (IOException e) {
                throw new PrestoException(ICEBERG_WRITER_OPEN_ERROR, e);
            }
        }
        verify(writers.size() == pagePartitioner.getMaxIndex() + 1);
        verify(!writers.contains(null));
        return writerIndexes;
    }

    private WriteContext createWriter(Optional<PartitionData> partitionData, Path outputPath)
    {
        IcebergFileWriter writer = fileWriterFactory.createFileWriter(
                outputPath,
                outputSchema,
                jobConf,
                session,
                hdfsContext,
                fileFormat,
                MetricsConfig.forTable(table));

        return new WriteContext(writer, outputPath, partitionData);
    }

    private Optional<PartitionData> getPartitionData(List<PartitionColumn> columns, Page transformedPage, int position)
    {
        if (columns.isEmpty()) {
            return Optional.empty();
        }

        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            PartitionColumn column = columns.get(i);
            Block block = transformedPage.getBlock(i);
            Type type = column.getResultType();
            values[i] = getIcebergValue(block, position, type);
        }
        return Optional.of(new PartitionData(values));
    }

    public static Object getIcebergValue(Block block, int position, Type type)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type instanceof BigintType) {
            return type.getLong(block, position);
        }
        if (type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType || type instanceof DateType) {
            return toIntExact(type.getLong(block, position));
        }
        if (type instanceof BooleanType) {
            return type.getBoolean(block, position);
        }
        if (type instanceof DecimalType) {
            return readBigDecimal((DecimalType) type, block, position);
        }
        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact(type.getLong(block, position)));
        }
        if (type instanceof DoubleType) {
            return type.getDouble(block, position);
        }
        if (type instanceof VarbinaryType) {
            return type.getSlice(block, position).getBytes();
        }
        if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (type instanceof TimestampType) {
            long timestamp = type.getLong(block, position);
            return ((TimestampType) type).getPrecision() == MILLISECONDS ? MILLISECONDS.toMicros(timestamp) : timestamp;
        }
        if (type instanceof TimeType) {
            long time = type.getLong(block, position);
            return MILLISECONDS.toMicros(time);
        }
        throw new UnsupportedOperationException("Type not supported as partition column: " + type.getDisplayName());
    }

    public static Object adjustTimestampForPartitionTransform(SqlFunctionProperties functionProperties, Type type, Object value)
    {
        if (type instanceof TimestampType && functionProperties.isLegacyTimestamp()) {
            long timestampValue = (long) value;
            TimestampType timestampType = (TimestampType) type;
            Instant instant = Instant.ofEpochSecond(timestampType.getPrecision().toSeconds(timestampValue),
                    timestampType.getPrecision().toNanos(timestampValue % timestampType.getPrecision().convert(1, SECONDS)));
            LocalDateTime localDateTime = instant
                    .atZone(ZoneId.of(functionProperties.getTimeZoneKey().getId()))
                    .toLocalDateTime();

            return timestampType.getPrecision().convert(localDateTime.toEpochSecond(ZoneOffset.UTC), SECONDS) +
                    timestampType.getPrecision().convert(localDateTime.getNano(), NANOSECONDS);
        }
        return value;
    }

    private static List<PartitionColumn> toPartitionColumns(List<IcebergColumnHandle> handles, PartitionSpec partitionSpec)
    {
        Map<Integer, Integer> idChannels = new HashMap<>();
        for (int i = 0; i < handles.size(); i++) {
            idChannels.put(handles.get(i).getId(), i);
        }

        return partitionSpec.fields().stream()
                .map(field -> {
                    Integer channel = idChannels.get(field.sourceId());
                    checkArgument(channel != null, "partition field not found: %s", field);
                    Type inputType = handles.get(channel).getType();
                    ColumnTransform transform = getColumnTransform(field, inputType);
                    return new PartitionColumn(field, channel, inputType, transform.getType(), transform.getTransform());
                })
                .collect(toImmutableList());
    }

    private static class WriteContext
    {
        private final IcebergFileWriter writer;
        private final Path path;
        private final Optional<PartitionData> partitionData;

        public WriteContext(IcebergFileWriter writer, Path path, Optional<PartitionData> partitionData)
        {
            this.writer = requireNonNull(writer, "writer is null");
            this.path = requireNonNull(path, "path is null");
            this.partitionData = requireNonNull(partitionData, "partitionData is null");
        }

        public IcebergFileWriter getWriter()
        {
            return writer;
        }

        public Path getPath()
        {
            return path;
        }

        public Optional<PartitionData> getPartitionData()
        {
            return partitionData;
        }
    }

    private static class PagePartitioner
    {
        private final PageIndexer pageIndexer;
        private final List<PartitionColumn> columns;
        private final ConnectorSession session;
        private Page transformedPage;

        public PagePartitioner(PageIndexerFactory pageIndexerFactory,
                List<PartitionColumn> columns,
                ConnectorSession session)
        {
            this.pageIndexer = pageIndexerFactory.createPageIndexer(columns.stream()
                    .map(PartitionColumn::getResultType)
                    .collect(toImmutableList()));
            this.columns = ImmutableList.copyOf(columns);
            this.session = session;
        }

        public int[] partitionPage(Page page)
        {
            Block[] blocks = new Block[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                PartitionColumn column = columns.get(i);
                Block block = adjustBlockIfNecessary(column, page.getBlock(column.getSourceChannel()));
                blocks[i] = column.getBlockTransform().apply(block);
            }
            this.transformedPage = new Page(page.getPositionCount(), blocks);

            return pageIndexer.indexPage(transformedPage);
        }

        public Page getTransformedPage()
        {
            return this.transformedPage;
        }

        public int getMaxIndex()
        {
            return pageIndexer.getMaxIndex();
        }

        public List<PartitionColumn> getColumns()
        {
            return columns;
        }

        private Block adjustBlockIfNecessary(PartitionColumn column, Block block)
        {
            // adjust legacy timestamp value to compatible with Iceberg non-identity transform calculation
            if (column.sourceType instanceof TimestampType && session.getSqlFunctionProperties().isLegacyTimestamp() && !column.getField().transform().isIdentity()) {
                TimestampType timestampType = (TimestampType) column.sourceType;
                BlockBuilder blockBuilder = timestampType.createBlockBuilder(null, block.getPositionCount());
                for (int t = 0; t < block.getPositionCount(); t++) {
                    if (block.isNull(t)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        long adjustedTimestampValue = (long) adjustTimestampForPartitionTransform(
                                session.getSqlFunctionProperties(),
                                timestampType,
                                timestampType.getLong(block, t));
                        timestampType.writeLong(blockBuilder, adjustedTimestampValue);
                    }
                }
                return blockBuilder.build();
            }
            return block;
        }
    }

    private static class PartitionColumn
    {
        private final PartitionField field;
        private final int sourceChannel;
        private final Type sourceType;
        private final Type resultType;
        private final Function<Block, Block> blockTransform;

        public PartitionColumn(PartitionField field, int sourceChannel, Type sourceType, Type resultType, Function<Block, Block> blockTransform)
        {
            this.field = requireNonNull(field, "field is null");
            this.sourceChannel = sourceChannel;
            this.sourceType = requireNonNull(sourceType, "sourceType is null");
            this.resultType = requireNonNull(resultType, "resultType is null");
            this.blockTransform = requireNonNull(blockTransform, "blockTransform is null");
        }

        public PartitionField getField()
        {
            return field;
        }

        public int getSourceChannel()
        {
            return sourceChannel;
        }

        public Type getSourceType()
        {
            return sourceType;
        }

        public Type getResultType()
        {
            return resultType;
        }

        public Function<Block, Block> getBlockTransform()
        {
            return blockTransform;
        }
    }
}
