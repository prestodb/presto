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
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.delete.IcebergDeletePageSink;
import com.facebook.presto.spi.ConnectorMergeSink;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.MergePage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;
import org.roaringbitmap.longlong.ImmutableLongBitmapDataProvider;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.plugin.base.util.Closables.closeAllSuppress;
import static com.facebook.presto.spi.connector.MergePage.createDeleteAndInsertPages;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

// TODO #20578: WIP - Class implementation under development.

public class IcebergMergeSink
        implements ConnectorMergeSink
{
    private final LocationProvider locationProvider;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final HdfsEnvironment hdfsEnvironment;
//    private final FileIoProvider fileIoProvider; // TODO #20578: Seems that it is only necessary to support Iceberg using AVRO file format. Presto does not have that support, so it looks like we can ignore this parameter for now.
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final FileFormat fileFormat;
    private final Map<String, String> storageProperties;
    private final Schema schema;
//    private final Map<Integer, PartitionSpec> partitionsSpecs; // TODO #20578: Should we use "partitionsSpec" or "partitionsSpecs" like Trino does?
    private final PartitionSpec partitionsSpec;
    private final ConnectorPageSink insertPageSink;
    private final int columnCount;
    private final Map<Slice, FileDeletion> fileDeletions = new HashMap<>();

    public IcebergMergeSink(
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            HdfsEnvironment hdfsEnvironment,
//            FileIoProvider fileIoProvider, // TODO #20578: Seems that it is only necessary to support Iceberg using AVRO file format. Presto does not have that support, so it looks like we can ignore this parameter for now.
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            FileFormat fileFormat,
            Map<String, String> storageProperties,
            Schema schema,
            PartitionSpec partitionsSpec,
            ConnectorPageSink insertPageSink,
            int columnCount)
    {
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
//        this.fileIoProvider = requireNonNull(fileIoProvider, "fileIoProvider is null");  // TODO #20578: Seems that it is only necessary to support Iceberg using AVRO file format. Presto does not have that support, so it looks like we can ignore this parameter for now.
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.session = requireNonNull(session, "session is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.storageProperties = ImmutableMap.copyOf(requireNonNull(storageProperties, "storageProperties is null"));
        this.schema = requireNonNull(schema, "schema is null");
//        this.partitionsSpec = ImmutableMap.copyOf(requireNonNull(partitionsSpec, "partitionsSpecs is null"));  // TODO #20578: Should we use "partitionsSpec" or "partitionsSpecs" like Trino does?
        this.partitionsSpec = requireNonNull(partitionsSpec, "partitionsSpecs is null");
        this.insertPageSink = requireNonNull(insertPageSink, "insertPageSink is null");
        this.columnCount = columnCount;
    }

    @Override
    public void storeMergedRows(Page page)
    {
        MergePage mergePage = createDeleteAndInsertPages(page, columnCount);

        mergePage.getInsertionsPage().ifPresent(insertPageSink::appendPage);

        mergePage.getDeletionsPage().ifPresent(deletions -> {
            ColumnarRow rowIdRow = toColumnarRow(deletions.getBlock(deletions.getChannelCount() - 1));

            for (int position = 0; position < rowIdRow.getPositionCount(); position++) {
                Slice filePath = VarcharType.VARCHAR.getSlice(rowIdRow.getField(0), position);
                long rowPosition = BIGINT.getLong(rowIdRow.getField(1), position);

                int index = position;
                FileDeletion deletion = fileDeletions.computeIfAbsent(filePath, ignored -> {
                    long fileRecordCount = BIGINT.getLong(rowIdRow.getField(2), index);
                    int partitionSpecId = toIntExact(INTEGER.getLong(rowIdRow.getField(3), index));
                    String partitionData = VarcharType.VARCHAR.getSlice(rowIdRow.getField(4), index).toStringUtf8();
                    return new FileDeletion(partitionSpecId, partitionData, fileRecordCount);
                });

                deletion.rowsToDelete().addLong(rowPosition);
            }
        });
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<Slice> fragments = new ArrayList<>(insertPageSink.finish().join());

        fileDeletions.forEach((dataFilePath, deletion) -> {
            ConnectorPageSink sink = createPositionDeletePageSink(
                    dataFilePath.toStringUtf8(),
//                    partitionsSpec.get(deletion.partitionSpecId()),  // TODO #20578: original Trino method parameter value.
                    partitionsSpec,
                    deletion.partitionDataJson(),
                    deletion.fileRecordCount());

            fragments.addAll(writePositionDeletes(sink, deletion.rowsToDelete()));
        });

        return completedFuture(fragments);
    }

    @Override
    public void abort()
    {
        insertPageSink.abort();
    }

    private ConnectorPageSink createPositionDeletePageSink(String dataFilePath, PartitionSpec partitionSpec, String partitionDataJson, long fileRecordCount)
    {
        // TODO #20578: Presto does that in IcebergUtil.partitionDataFromJson(). So, we can remove the following block of code.
//        Optional<PartitionData> partitionData = Optional.empty();
//        if (partitionSpec.isPartitioned()) {
//            Type[] columnTypes = partitionSpec.fields().stream()
//                    .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
//                    .toArray(Type[]::new);
//            partitionData = Optional.of(PartitionData.fromJson(partitionDataJson, columnTypes));
//        }

        return new IcebergDeletePageSink(
                partitionSpec,
                Optional.of(partitionDataJson),
                locationProvider,
                fileWriterFactory,
                hdfsEnvironment,
                new HdfsContext(session),
                jsonCodec,
                session,
                dataFilePath,
                fileFormat);
    }

    private static Collection<Slice> writePositionDeletes(ConnectorPageSink sink, ImmutableLongBitmapDataProvider rowsToDelete)
    {
        try {
            return doWritePositionDeletes(sink, rowsToDelete);
        }
        catch (Throwable t) {
            closeAllSuppress(t, sink::abort);
            throw t;
        }
    }

    private static Collection<Slice> doWritePositionDeletes(ConnectorPageSink sink, ImmutableLongBitmapDataProvider rowsToDelete)
    {
        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(BIGINT));

        rowsToDelete.forEach(rowPosition -> {
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), rowPosition);
            pageBuilder.declarePosition();
            if (pageBuilder.isFull()) {
                sink.appendPage(pageBuilder.build());
                pageBuilder.reset();
            }
        });

        if (!pageBuilder.isEmpty()) {
            sink.appendPage(pageBuilder.build());
        }

        return sink.finish().join();
    }

    private static class FileDeletion
    {
        private final int partitionSpecId;
        private final String partitionDataJson;
        private final long fileRecordCount;
        private final LongBitmapDataProvider rowsToDelete = new Roaring64Bitmap();

        public FileDeletion(int partitionSpecId, String partitionDataJson, long fileRecordCount)
        {
            this.partitionSpecId = partitionSpecId;
            this.partitionDataJson = requireNonNull(partitionDataJson, "partitionDataJson is null");
            this.fileRecordCount = fileRecordCount;
        }

        public int partitionSpecId()
        {
            return partitionSpecId;
        }

        public String partitionDataJson()
        {
            return partitionDataJson;
        }

        public long fileRecordCount()
        {
            return fileRecordCount;
        }

        public LongBitmapDataProvider rowsToDelete()
        {
            return rowsToDelete;
        }
    }
}
