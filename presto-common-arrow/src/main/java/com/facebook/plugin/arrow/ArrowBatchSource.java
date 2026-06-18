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
package com.facebook.plugin.arrow;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPageSource;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;
import java.util.List;

import static com.facebook.plugin.arrow.BlockArrowWriter.createArrowWriters;
import static java.util.Objects.requireNonNull;

public class ArrowBatchSource
        implements AutoCloseable
{
    private final VectorSchemaRoot root;
    private final List<BlockArrowWriter.ArrowVectorWriter> writers;
    private final int maxRowsPerBatch;
    private final ConnectorPageSource pageSource;

    private Page currentPage;
    private int currentPosition;

    public ArrowBatchSource(BufferAllocator allocator, List<ColumnMetadata> columns, ConnectorPageSource pageSource, int maxRowsPerBatch)
    {
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
        this.maxRowsPerBatch = maxRowsPerBatch;
        ImmutableList.Builder<BlockArrowWriter.ArrowVectorWriter> writerBuilder = ImmutableList.builder();
        this.root = createArrowWriters(allocator, columns, writerBuilder);
        this.writers = writerBuilder.build();
    }

    public VectorSchemaRoot getVectorSchemaRoot()
    {
        return root;
    }

    /**
     * Loads the next record batch from the source.
     * Returns false if there are no more batches from the source.
     */
    public boolean nextBatch()
    {
        // Release previous buffers
        root.clear();

        // Reserve capacity for next batch
        allocateVectorCapacity(root, maxRowsPerBatch);

        int batchRowIndex = 0;
        while (batchRowIndex < maxRowsPerBatch) {
            if (currentPage == null || currentPosition >= currentPage.getPositionCount()) {
                if (pageSource.isFinished()) {
                    break;
                }

                currentPage = pageSource.getNextPage();
                currentPosition = 0;

                if (currentPage == null || currentPage.getPositionCount() == 0) {
                    continue;
                }
            }

            for (int column = 0; column < writers.size(); column++) {
                Block block = currentPage.getBlock(column);
                BlockArrowWriter.ArrowVectorWriter writer = writers.get(column);

                if (block.isNull(currentPosition)) {
                    writer.writeNull(batchRowIndex);
                }
                else {
                    writer.writeBlock(batchRowIndex, block, currentPosition);
                }
            }
            currentPosition++;
            batchRowIndex++;
        }
        root.setRowCount(batchRowIndex);
        return batchRowIndex > 0;
    }

    @Override
    public void close()
            throws IOException
    {
        root.close();
        pageSource.close();
    }

    private static void allocateVectorCapacity(VectorSchemaRoot root, int capacity)
    {
        for (ValueVector vector : root.getFieldVectors()) {
            vector.setInitialCapacity(capacity);
            AllocationHelper.allocateNew(vector, capacity);
        }
    }
}
