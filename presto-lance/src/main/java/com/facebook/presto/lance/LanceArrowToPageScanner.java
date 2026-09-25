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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.lance.ipc.LanceScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LanceArrowToPageScanner
        implements AutoCloseable
{
    private final ScannerFactory scannerFactory;
    private final ArrowReader arrowReader;
    private final BufferAllocator allocator;
    private final List<LanceColumnHandle> columns;
    private final ArrowBlockBuilder arrowBlockBuilder;
    private long lastBatchBytes;

    public LanceArrowToPageScanner(
            BufferAllocator allocator,
            List<LanceColumnHandle> columns,
            ScannerFactory scannerFactory,
            ArrowBlockBuilder arrowBlockBuilder,
            Optional<String> filter)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.scannerFactory = requireNonNull(scannerFactory, "scannerFactory is null");
        this.arrowBlockBuilder = requireNonNull(arrowBlockBuilder, "arrowBlockBuilder is null");
        List<String> columnNames = columns.stream()
                .map(LanceColumnHandle::getColumnName)
                .collect(toImmutableList());
        LanceScanner scanner = scannerFactory.open(allocator, columnNames, filter);
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
        List<FieldVector> coercedVectors = new ArrayList<>();

        try {
            for (int col = 0; col < columns.size(); col++) {
                LanceColumnHandle column = columns.get(col);
                FieldVector vector = root.getVector(column.getColumnName());
                Type type = column.getColumnType();
                vector = coerceVector(vector, coercedVectors);
                blocks[col] = arrowBlockBuilder.buildBlockFromFieldVector(vector, type, null);
            }
            return new Page(rowCount, blocks);
        }
        finally {
            for (FieldVector v : coercedVectors) {
                v.close();
            }
        }
    }

    /**
     * Coerce unsupported Arrow vector types to types that ArrowBlockBuilder can handle.
     * Tracks newly allocated vectors in coercedVectors for cleanup.
     *
     * - Float2Vector (float16) -> Float4Vector (float32)
     * - UInt8Vector (uint64) -> BigIntVector (int64, treats as signed)
     * - List/FixedSizeList containing Float2Vector -> widen inner data vector
     */
    private FieldVector coerceVector(FieldVector vector, List<FieldVector> coercedVectors)
    {
        if (vector instanceof Float2Vector) {
            Float4Vector widened = widenFloat2ToFloat4((Float2Vector) vector, allocator);
            coercedVectors.add(widened);
            return widened;
        }
        if (vector instanceof UInt8Vector) {
            BigIntVector converted = convertUInt8ToBigInt((UInt8Vector) vector, allocator);
            coercedVectors.add(converted);
            return converted;
        }
        if (vector instanceof FixedSizeListVector) {
            FixedSizeListVector fslVector = (FixedSizeListVector) vector;
            FieldVector dataVector = fslVector.getDataVector();
            if (dataVector instanceof Float2Vector) {
                Float4Vector widened = widenFloat2ToFloat4((Float2Vector) dataVector, allocator);
                coercedVectors.add(widened);
                // Build a new FixedSizeListVector backed by the widened Float4Vector
                FixedSizeListVector newFsl = buildFixedSizeListWithData(
                        fslVector.getName(), fslVector.getListSize(),
                        fslVector.getValueCount(), fslVector, widened);
                coercedVectors.add(newFsl);
                return newFsl;
            }
        }
        if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            FieldVector dataVector = listVector.getDataVector();
            if (dataVector instanceof Float2Vector) {
                Float4Vector widened = widenFloat2ToFloat4((Float2Vector) dataVector, allocator);
                coercedVectors.add(widened);
                // Build a new ListVector backed by the widened Float4Vector
                ListVector newList = buildListWithData(
                        listVector.getName(), listVector, widened);
                coercedVectors.add(newList);
                return newList;
            }
        }
        return vector;
    }

    static Float4Vector widenFloat2ToFloat4(Float2Vector f2v, BufferAllocator allocator)
    {
        int valueCount = f2v.getValueCount();
        Float4Vector f4v = new Float4Vector(f2v.getName(), allocator);
        f4v.allocateNew(valueCount);
        for (int i = 0; i < valueCount; i++) {
            if (f2v.isNull(i)) {
                f4v.setNull(i);
            }
            else {
                f4v.set(i, f2v.getValueAsFloat(i));
            }
        }
        f4v.setValueCount(valueCount);
        return f4v;
    }

    static BigIntVector convertUInt8ToBigInt(UInt8Vector uint8v, BufferAllocator allocator)
    {
        int valueCount = uint8v.getValueCount();
        BigIntVector bigIntVector = new BigIntVector(uint8v.getName(), allocator);
        bigIntVector.allocateNew(valueCount);
        for (int i = 0; i < valueCount; i++) {
            if (uint8v.isNull(i)) {
                bigIntVector.setNull(i);
            }
            else {
                bigIntVector.set(i, uint8v.get(i));
            }
        }
        bigIntVector.setValueCount(valueCount);
        return bigIntVector;
    }

    /**
     * Build a new FixedSizeListVector using the validity from the original
     * and the widened data vector as the inner data.
     */
    private FixedSizeListVector buildFixedSizeListWithData(
            String name, int listSize, int valueCount,
            FixedSizeListVector original, Float4Vector widenedData)
    {
        FixedSizeListVector newFsl = FixedSizeListVector.empty(name, listSize, allocator);
        newFsl.addOrGetVector(widenedData.getField().getFieldType());
        newFsl.setInitialCapacity(valueCount);
        newFsl.allocateNew();

        // Copy validity bits from original
        for (int i = 0; i < valueCount; i++) {
            if (original.isNull(i)) {
                newFsl.setNull(i);
            }
            else {
                newFsl.setNotNull(i);
            }
        }
        newFsl.setValueCount(valueCount);

        // Copy widened float data into the new inner data vector
        Float4Vector newData = (Float4Vector) newFsl.getDataVector();
        newData.allocateNew(widenedData.getValueCount());
        for (int i = 0; i < widenedData.getValueCount(); i++) {
            if (widenedData.isNull(i)) {
                newData.setNull(i);
            }
            else {
                newData.set(i, widenedData.get(i));
            }
        }
        newData.setValueCount(widenedData.getValueCount());
        return newFsl;
    }

    /**
     * Build a new ListVector using the offset buffer from the original
     * and the widened data vector as the inner data.
     */
    private ListVector buildListWithData(
            String name, ListVector original, Float4Vector widenedData)
    {
        ListVector newList = ListVector.empty(name, allocator);
        newList.addOrGetVector(widenedData.getField().getFieldType());
        int valueCount = original.getValueCount();
        newList.setInitialCapacity(valueCount);
        newList.allocateNew();

        // Copy offset buffer from original
        ArrowBuf originalOffsets = original.getOffsetBuffer();
        ArrowBuf newOffsets = newList.getOffsetBuffer();
        newOffsets.setBytes(0, originalOffsets, 0, (long) (valueCount + 1) * ListVector.OFFSET_WIDTH);

        // Copy validity bits
        for (int i = 0; i < valueCount; i++) {
            if (original.isNull(i)) {
                newList.setNull(i);
            }
            else {
                newList.setNotNull(i);
            }
        }
        newList.setValueCount(valueCount);
        newList.setLastSet(valueCount - 1);

        // Copy widened data
        Float4Vector newData = (Float4Vector) newList.getDataVector();
        newData.allocateNew(widenedData.getValueCount());
        for (int i = 0; i < widenedData.getValueCount(); i++) {
            if (widenedData.isNull(i)) {
                newData.setNull(i);
            }
            else {
                newData.set(i, widenedData.get(i));
            }
        }
        newData.setValueCount(widenedData.getValueCount());
        return newList;
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
