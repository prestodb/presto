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
import com.facebook.presto.iceberg.HdfsOutputFile;
import com.facebook.presto.iceberg.MetricsWrapper;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.iceberg.FileContent.POSITION_DELETES;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergUtil.partitionDataFromJson;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class IcebergDeletionVectorPageSink
        implements ConnectorPageSink
{
    private static final int SERIAL_COOKIE_NO_RUNCONTAINER = 12346;

    private final PartitionSpec partitionSpec;
    private final Optional<PartitionData> partitionData;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final String dataFile;
    private final LocationProvider locationProvider;

    private final List<Integer> collectedPositions = new ArrayList<>();

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
        return collectedPositions.size() * (long) Integer.BYTES;
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
            long position = BigintType.BIGINT.getLong(block, i);
            collectedPositions.add((int) position);
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (collectedPositions.isEmpty()) {
            return completedFuture(ImmutableList.of());
        }

        Collections.sort(collectedPositions);

        byte[] roaringBitmapBytes = serializeRoaringBitmap(collectedPositions);

        String fileName = "dv-" + randomUUID() + ".puffin";
        Path puffinPath = partitionData
                .map(partition -> new Path(locationProvider.newDataLocation(partitionSpec, partition, fileName)))
                .orElseGet(() -> new Path(locationProvider.newDataLocation(fileName)));

        OutputFile outputFile = new HdfsOutputFile(puffinPath, hdfsEnvironment, hdfsContext);

        long puffinFileSize;
        long blobOffset;
        long blobLength;

        try {
            PuffinWriter writer = hdfsEnvironment.doAs(session.getUser(), () ->
                    Puffin.write(outputFile).createdBy("presto").build());
            try {
                writer.add(new Blob(
                        "deletion-vector-v2",
                        ImmutableList.of(),
                        0,
                        0,
                        ByteBuffer.wrap(roaringBitmapBytes)));
                hdfsEnvironment.doAs(session.getUser(), () -> {
                    writer.finish();
                    return null;
                });
                puffinFileSize = writer.fileSize();
                blobOffset = writer.writtenBlobsMetadata().get(0).offset();
                blobLength = writer.writtenBlobsMetadata().get(0).length();
            }
            finally {
                hdfsEnvironment.doAs(session.getUser(), () -> {
                    writer.close();
                    return null;
                });
            }
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "Failed to write deletion vector puffin file", e);
        }

        CommitTaskData task = new CommitTaskData(
                puffinPath.toString(),
                puffinFileSize,
                new MetricsWrapper(new Metrics((long) collectedPositions.size(), null, null, null, null)),
                partitionSpec.specId(),
                partitionData.map(PartitionData::toJson),
                FileFormat.PUFFIN,
                dataFile,
                POSITION_DELETES,
                OptionalLong.of(blobOffset),
                OptionalLong.of(blobLength),
                OptionalLong.of(collectedPositions.size()));

        return completedFuture(ImmutableList.of(wrappedBuffer(jsonCodec.toJsonBytes(task))));
    }

    @Override
    public void abort()
    {
        // Nothing to clean up since we write the Puffin file atomically in finish()
    }

    /**
     * Serializes sorted positions into the Roaring Bitmap format as defined by the
     * <a href="https://github.com/RoaringBitmap/RoaringFormatSpec">Roaring Bitmap Format Spec</a>.
     *
     * <p>This implementation produces a single-container array bitmap in little-endian byte order:
     * <pre>
     *   [cookie: 4 bytes] SERIAL_COOKIE_NO_RUNCONTAINER (12346) in the lower 16 bits,
     *                     (containerCount - 1) in the upper 16 bits
     *   [key: 2 bytes]    high 16-bit key of the container (always 0 for positions < 65536)
     *   [card: 2 bytes]   (cardinality - 1) encoded as a 16-bit unsigned short
     *   [values: 2 bytes each] low 16 bits of each position, stored as unsigned shorts
     * </pre>
     *
     * <p>Limitations: assumes all positions fit within a single container (i.e., positions < 65536
     * and cardinality <= 4096 for array encoding). For deletion vectors in typical Iceberg splits
     * this is sufficient since row positions within a single data file split are bounded.
     */
    private static byte[] serializeRoaringBitmap(List<Integer> sortedPositions)
    {
        // Group positions by container (high 16 bits)
        Map<Integer, List<Integer>> containers = new TreeMap<>();
        for (int pos : sortedPositions) {
            int key = pos >>> 16;
            containers.computeIfAbsent(key, k -> new ArrayList<>()).add(pos & 0xFFFF);
        }

        // Calculate size: cookie(4) + numContainers * (key(2) + cardinality(2)) + data
        int numContainers = containers.size();
        int dataSize = 4; // cookie
        for (List<Integer> values : containers.values()) {
            dataSize += 4; // key + cardinality
            if (values.size() <= 4096) {
                dataSize += values.size() * 2; // array container
            }
            else {
                dataSize += 1024 * 8; // bitmap container (65536 bits)
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(dataSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Cookie: 12346 in low 16 bits, (numContainers - 1) in high 16 bits
        buffer.putInt(12346 | ((numContainers - 1) << 16));

        // Container headers: key + (cardinality - 1)
        for (Map.Entry<Integer, List<Integer>> entry : containers.entrySet()) {
            buffer.putShort((short) (entry.getKey() & 0xFFFF));
            buffer.putShort((short) (entry.getValue().size() - 1));
        }

        // Container data
        for (List<Integer> values : containers.values()) {
            if (values.size() <= 4096) {
                // Array container
                for (int v : values) {
                    buffer.putShort((short) (v & 0xFFFF));
                }
            }
            else {
                // Bitmap container
                long[] bitmap = new long[1024];
                for (int v : values) {
                    bitmap[v / 64] |= 1L << (v % 64);
                }
                for (long word : bitmap) {
                    buffer.putLong(word);
                }
            }
        }
        return buffer.array();
    }
}
