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

import com.facebook.plugin.arrow.ArrowBlockBuilder;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.lance.ipc.LanceScanner;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LanceArrowToPageScanner
        implements AutoCloseable
{
    private final ScannerFactory scannerFactory;
    private final ArrowReader arrowReader;
    private final List<LanceColumnHandle> columns;
    private final ArrowBlockBuilder arrowBlockBuilder;
    private long lastBatchBytes;

    public LanceArrowToPageScanner(
            BufferAllocator allocator,
            List<LanceColumnHandle> columns,
            ScannerFactory scannerFactory,
            ArrowBlockBuilder arrowBlockBuilder)
    {
        this.columns = requireNonNull(columns, "columns is null");
        this.scannerFactory = requireNonNull(scannerFactory, "scannerFactory is null");
        this.arrowBlockBuilder = requireNonNull(arrowBlockBuilder, "arrowBlockBuilder is null");
        List<String> columnNames = columns.stream()
                .map(LanceColumnHandle::getColumnName)
                .collect(toImmutableList());
        LanceScanner scanner = scannerFactory.open(allocator, columnNames);
        this.arrowReader = scanner.scanBatches();
    }

    public boolean read()
    {
        try {
            boolean hasNext = arrowReader.loadNextBatch();
            if (hasNext) {
                VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
                lastBatchBytes = 0;
                for (FieldVector vector : root.getFieldVectors()) {
                    for (ArrowBuf buf : vector.getFieldBuffers()) {
                        if (buf != null) {
                            lastBatchBytes += buf.capacity();
                        }
                    }
                }
            }
            return hasNext;
        }
        catch (IOException e) {
            throw new PrestoException(LanceErrorCode.LANCE_ERROR, "Failed to read Arrow batch", e);
        }
    }

    public long getLastBatchBytes()
    {
        return lastBatchBytes;
    }

    public Page convert()
    {
        VectorSchemaRoot root;
        try {
            root = arrowReader.getVectorSchemaRoot();
        }
        catch (IOException e) {
            throw new PrestoException(LanceErrorCode.LANCE_ERROR, "Failed to get VectorSchemaRoot", e);
        }

        int rowCount = root.getRowCount();
        Block[] blocks = new Block[columns.size()];

        for (int col = 0; col < columns.size(); col++) {
            LanceColumnHandle column = columns.get(col);
            FieldVector vector = root.getVector(column.getColumnName());
            Type type = column.getColumnType();
            blocks[col] = arrowBlockBuilder.buildBlockFromFieldVector(vector, type, null);
        }

        return new Page(rowCount, blocks);
    }

    @Override
    public void close()
    {
        try {
            arrowReader.close();
        }
        catch (IOException e) {
            // ignore
        }
        scannerFactory.close();
    }
}
