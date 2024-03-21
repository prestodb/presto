package com.facebook.presto.lance.scan;

import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.lance.metadata.LanceColumnType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;

import static io.airlift.slice.Slices.wrappedBuffer;

public class ArrowVectorPageBuilder
{
    private final Type columnType;
    private final BlockBuilder blockBuilder;
    private final FieldVector arrowVector;

    private final ColumnProcessor columnProcessor;

    interface ColumnProcessor {
        void write(int index);
    }

    private ArrowVectorPageBuilder(Type columnType, BlockBuilder blockBuilder, FieldVector arrowVector, ColumnProcessor columnProcessor)
    {
        this.columnType = columnType;
        this.blockBuilder = blockBuilder;
        this.arrowVector = arrowVector;
        this.columnProcessor = columnProcessor;
    }

    public static ArrowVectorPageBuilder create(Type columnType, BlockBuilder blockBuilder, FieldVector arrowVector) {
        ColumnProcessor columnProcessor = createColumnProcessor(columnType, blockBuilder, arrowVector);
        return new ArrowVectorPageBuilder(columnType, blockBuilder, arrowVector, columnProcessor);
    }

    private static ColumnProcessor createColumnProcessor(Type columnType, BlockBuilder blockBuilder, FieldVector arrowVector) {
        LanceColumnType lanceColumnType = LanceColumnType.fromPrestoType(columnType);
        switch (lanceColumnType){
            case BIGINT:
                return index -> columnType.writeLong(blockBuilder, ((BigIntVector) arrowVector).get(index));
            case INTEGER:
                return index -> columnType.writeLong(blockBuilder, ((IntVector) arrowVector).get(index));
            case DOUBLE:
            case FLOAT:
                return index -> columnType.writeDouble(blockBuilder, ((Float8Vector) arrowVector).get(index));
            case VARCHAR:
                return index -> columnType.writeSlice(blockBuilder, wrappedBuffer(((VarCharVector) arrowVector).get(index)));
            case BOOLEAN:
                return index -> columnType.writeBoolean(blockBuilder, ((BitVector) arrowVector).get(index) == 1);
            case TIMESTAMP:
            case OTHER:
            default:
                throw new RuntimeException("unsupported type: " + lanceColumnType);
        }
    }
    public void build() {
        int valueCount = arrowVector.getValueCount();
        for (int index = 0; index < valueCount; index++) {
            if (arrowVector.isNull(index)) {
                blockBuilder.appendNull();
            }
            else {
                columnProcessor.write(index);
            }
        }
    }
}
