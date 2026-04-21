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
package com.facebook.presto.iceberg.delete;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.CommitTaskData;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.MetricsWrapper;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.iceberg.FileContent.POSITION_DELETES;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergUtil.partitionDataFromJson;
import static com.facebook.presto.iceberg.delete.IcebergDeletionVectorWriterFactory.createPuffinOutputFileFactory;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Writes Iceberg V3 Puffin deletion-vector (DV) files via the native
 * {@link BaseDVFileWriter}.  The native writer emits spec-compliant
 * {@code deletion-vector-v1} blobs (64-bit Roaring portable bitmap with the
 * required magic / length / CRC framing) and populates blob properties
 * ({@code referenced-data-file}, {@code cardinality}) automatically.
 */
public class IcebergDeletionVectorPageSink
        implements ConnectorPageSink
{
    private final PartitionSpec partitionSpec;
    private final Optional<PartitionData> partitionData;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final String dataFile;
    private final LocationProvider locationProvider;

    private final Roaring64Bitmap collectedPositions = new Roaring64Bitmap();

    public IcebergDeletionVectorPageSink(
            PartitionSpec partitionSpec,
            Optional<String> partitionDataAsJson,
            LocationProvider locationProvider,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            String dataFile)
    {
        this.partitionSpec = requireNonNull(partitionSpec, "partitionSpec is null");
        this.partitionData = partitionDataFromJson(partitionSpec, partitionDataAsJson);
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.session = requireNonNull(session, "session is null");
        this.dataFile = requireNonNull(dataFile, "dataFile is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return collectedPositions.getLongSizeInBytes();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getChannelCount() != 1) {
            throw new PrestoException(ICEBERG_BAD_DATA,
                    "Expecting Page with one channel but got " + page.getChannelCount());
        }

        Block block = page.getBlock(0);
        for (int i = 0; i < block.getPositionCount(); i++) {
            collectedPositions.addLong(BigintType.BIGINT.getLong(block, i));
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (collectedPositions.isEmpty()) {
            return completedFuture(ImmutableList.of());
        }

        OutputFileFactory fileFactory = createPuffinOutputFileFactory(
                hdfsEnvironment,
                hdfsContext,
                locationProvider,
                partitionSpec,
                UUID.randomUUID().toString(),
                0,
                0L);

        // The writer is wrapped in try-with-resources so the underlying Puffin output
        // stream is always closed, even if writer.delete(...) throws midway.
        // First-write context: no previous-DV loader needed (the page-sink path always
        // produces a brand-new DV; merging with existing DVs happens later in
        // IcebergAbstractMetadata.finishWrite via createMergedDeletionVector).
        org.apache.iceberg.io.DeleteWriteResult writeResult;
        try (BaseDVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null)) {
            collectedPositions.forEach((org.roaringbitmap.longlong.LongConsumer) pos ->
                    writer.delete(dataFile, pos, partitionSpec, partitionData.orElse(null)));
            writer.close();
            writeResult = writer.result();
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "Failed to write deletion vector puffin file", e);
        }
        catch (UncheckedIOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "Failed to write deletion vector puffin file", e.getCause());
        }

        ImmutableList.Builder<Slice> outputs = ImmutableList.builder();
        for (DeleteFile deleteFile : writeResult.deleteFiles()) {
            CommitTaskData task = new CommitTaskData(
                    deleteFile.path().toString(),
                    deleteFile.fileSizeInBytes(),
                    new MetricsWrapper(new Metrics(deleteFile.recordCount(), null, null, null, null)),
                    deleteFile.specId(),
                    partitionData.map(PartitionData::toJson),
                    FileFormat.PUFFIN,
                    deleteFile.referencedDataFile() != null ? deleteFile.referencedDataFile() : dataFile,
                    POSITION_DELETES,
                    OptionalLong.of(deleteFile.contentOffset()),
                    OptionalLong.of(deleteFile.contentSizeInBytes()),
                    OptionalLong.of(deleteFile.recordCount()));
            outputs.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));
        }
        return completedFuture(outputs.build());
    }

    @Override
    public void abort()
    {
        // Nothing to clean up since we write the Puffin file atomically in finish().
    }
}
