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
package com.facebook.presto.lance;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Fragment;
import org.lance.FragmentMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class LancePageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(LancePageSink.class);

    private final String datasetUri;
    private final Schema arrowSchema;
    private final List<Type> columnTypes;
    private final JsonCodec<LanceCommitTaskData> jsonCodec;
    private final BufferAllocator allocator;

    private final List<Page> bufferedPages = new ArrayList<>();
    private long writtenBytes;
    private long rowCount;
    private boolean finished;

    public LancePageSink(
            String datasetUri,
            Schema arrowSchema,
            List<LanceColumnHandle> columns,
            JsonCodec<LanceCommitTaskData> jsonCodec,
            BufferAllocator parentAllocator)
    {
        this.datasetUri = requireNonNull(datasetUri, "datasetUri is null");
        this.arrowSchema = requireNonNull(arrowSchema, "arrowSchema is null");
        this.columnTypes = columns.stream()
                .map(LanceColumnHandle::getColumnType)
                .collect(toImmutableList());
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.allocator = requireNonNull(parentAllocator, "parentAllocator is null")
                .newChildAllocator("page-sink", 0, Long.MAX_VALUE);
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        bufferedPages.add(page);
        rowCount += page.getPositionCount();
        writtenBytes += page.getSizeInBytes();
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        if (finished) {
            throw new IllegalStateException("PageSink already finished");
        }
        finished = true;

        try {
            String fragmentsJson;
            if (bufferedPages.isEmpty()) {
                fragmentsJson = "[]";
            }
            else {
                fragmentsJson = writeFragments();
            }

            LanceCommitTaskData commitData = new LanceCommitTaskData(
                    fragmentsJson, writtenBytes, rowCount);

            Slice slice = Slices.wrappedBuffer(jsonCodec.toJsonBytes(commitData));
            return completedFuture(ImmutableList.of(slice));
        }
        catch (PrestoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new PrestoException(LanceErrorCode.LANCE_ERROR,
                    "Failed to write Lance fragments: " + e.getMessage(), e);
        }
        finally {
            cleanup();
        }
    }

    private String writeFragments()
    {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            long totalRowsLong = bufferedPages.stream()
                    .mapToLong(Page::getPositionCount)
                    .sum();
            if (totalRowsLong > Integer.MAX_VALUE) {
                throw new PrestoException(LanceErrorCode.LANCE_ERROR,
                        "Total row count exceeds maximum: " + totalRowsLong);
            }
            int totalRows = (int) totalRowsLong;

            root.allocateNew();

            int currentOffset = 0;
            for (Page page : bufferedPages) {
                int pageRows = page.getPositionCount();
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    LancePageToArrowConverter.writeBlockToVectorAtOffset(
                            page.getBlock(channel),
                            root.getVector(channel),
                            columnTypes.get(channel),
                            pageRows,
                            currentOffset);
                }
                currentOffset += pageRows;
            }
            root.setRowCount(totalRows);

            List<FragmentMetadata> fragments = Fragment.create(
                    datasetUri, allocator, root,
                    new org.lance.WriteParams.Builder().build());

            return LanceFragmentData.serializeFragments(fragments);
        }
    }

    @Override
    public void abort()
    {
        cleanup();
    }

    private void cleanup()
    {
        bufferedPages.clear();
        try {
            allocator.close();
        }
        catch (Exception e) {
            log.warn(e, "Failed to close allocator");
        }
    }
}
