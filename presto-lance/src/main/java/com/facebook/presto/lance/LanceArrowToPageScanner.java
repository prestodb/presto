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

import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slices;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.lance.ipc.LanceScanner;

import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class LanceArrowToPageScanner
        implements AutoCloseable
{
    private final ScannerFactory scannerFactory;
    private final ArrowReader arrowReader;
    private final List<LanceColumnHandle> columns;
    private long lastBatchBytes;

    public LanceArrowToPageScanner(
            BufferAllocator allocator,
            List<LanceColumnHandle> columns,
            ScannerFactory scannerFactory)
    {
        this.columns = requireNonNull(columns, "columns is null");
        this.scannerFactory = requireNonNull(scannerFactory, "scannerFactory is null");
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

    public void convert(PageBuilder pageBuilder)
    {
        VectorSchemaRoot root;
        try {
            root = arrowReader.getVectorSchemaRoot();
        }
        catch (IOException e) {
            throw new PrestoException(LanceErrorCode.LANCE_ERROR, "Failed to get VectorSchemaRoot", e);
        }
        int rowCount = root.getRowCount();

        for (int col = 0; col < columns.size(); col++) {
            LanceColumnHandle column = columns.get(col);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(col);
            FieldVector vector = root.getVector(column.getColumnName());
            Type type = column.getColumnType();
            writeVectorToBlock(vector, blockBuilder, type, rowCount);
        }
        pageBuilder.declarePositions(rowCount);
    }

    private void writeVectorToBlock(FieldVector vector, BlockBuilder blockBuilder, Type type, int rowCount)
    {
        for (int i = 0; i < rowCount; i++) {
            if (vector.isNull(i)) {
                blockBuilder.appendNull();
            }
            else {
                writeValue(vector, blockBuilder, type, i);
            }
        }
    }

    private void writeValue(FieldVector vector, BlockBuilder blockBuilder, Type type, int index)
    {
        if (type instanceof BooleanType) {
            type.writeBoolean(blockBuilder, ((BitVector) vector).get(index) == 1);
        }
        else if (type instanceof TinyintType) {
            type.writeLong(blockBuilder, ((TinyIntVector) vector).get(index));
        }
        else if (type instanceof SmallintType) {
            type.writeLong(blockBuilder, ((SmallIntVector) vector).get(index));
        }
        else if (type instanceof IntegerType) {
            type.writeLong(blockBuilder, ((IntVector) vector).get(index));
        }
        else if (type instanceof BigintType) {
            type.writeLong(blockBuilder, ((BigIntVector) vector).get(index));
        }
        else if (type instanceof RealType) {
            type.writeLong(blockBuilder, floatToRawIntBits(((Float4Vector) vector).get(index)));
        }
        else if (type instanceof DoubleType) {
            type.writeDouble(blockBuilder, ((Float8Vector) vector).get(index));
        }
        else if (type instanceof VarcharType) {
            byte[] bytes = ((VarCharVector) vector).get(index);
            type.writeSlice(blockBuilder, Slices.wrappedBuffer(bytes));
        }
        else if (type instanceof VarbinaryType) {
            byte[] bytes = ((VarBinaryVector) vector).get(index);
            type.writeSlice(blockBuilder, Slices.wrappedBuffer(bytes));
        }
        else if (type instanceof DateType) {
            type.writeLong(blockBuilder, ((DateDayVector) vector).get(index));
        }
        else if (type instanceof TimestampType) {
            long micros;
            if (vector instanceof TimeStampMicroVector) {
                micros = ((TimeStampMicroVector) vector).get(index);
            }
            else if (vector instanceof TimeStampMicroTZVector) {
                micros = ((TimeStampMicroTZVector) vector).get(index);
            }
            else {
                throw new PrestoException(LanceErrorCode.LANCE_TYPE_NOT_SUPPORTED,
                        "Unsupported timestamp vector: " + vector.getClass());
            }
            type.writeLong(blockBuilder, micros);
        }
        else if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            BlockBuilder arrayBuilder = blockBuilder.beginBlockEntry();
            FieldVector dataVector;
            int start;
            int end;
            if (vector instanceof ListVector) {
                ListVector listVector = (ListVector) vector;
                dataVector = listVector.getDataVector();
                start = listVector.getElementStartIndex(index);
                end = listVector.getElementEndIndex(index);
            }
            else if (vector instanceof FixedSizeListVector) {
                FixedSizeListVector fslVector = (FixedSizeListVector) vector;
                dataVector = fslVector.getDataVector();
                int listSize = fslVector.getListSize();
                start = index * listSize;
                end = start + listSize;
            }
            else {
                throw new PrestoException(LanceErrorCode.LANCE_TYPE_NOT_SUPPORTED,
                        "Unsupported list vector: " + vector.getClass());
            }
            for (int j = start; j < end; j++) {
                if (dataVector.isNull(j)) {
                    arrayBuilder.appendNull();
                }
                else {
                    writeValue(dataVector, arrayBuilder, elementType, j);
                }
            }
            blockBuilder.closeEntry();
        }
        else {
            throw new PrestoException(LanceErrorCode.LANCE_TYPE_NOT_SUPPORTED,
                    "Unsupported type: " + type);
        }
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
