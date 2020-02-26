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
package com.facebook.presto.operator.repartition;

import com.facebook.presto.spi.block.AbstractVariableWidthBlock;
import com.facebook.presto.spi.block.ArrayAllocator;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.array.Arrays.ExpansionFactor.LARGE;
import static com.facebook.presto.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.operator.MoreByteArrays.setBytes;
import static com.facebook.presto.operator.UncheckedByteArrays.setIntUnchecked;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class VariableWidthBlockEncodingBuffer
        extends AbstractBlockEncodingBuffer
{
    @VisibleForTesting
    static final int POSITION_SIZE = Integer.BYTES + Byte.BYTES;

    private static final String NAME = "VARIABLE_WIDTH";
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlockEncodingBuffer.class).instanceSize();

    // The buffer for the slice for all incoming blocks so far
    private byte[] sliceBuffer;

    // The address that the next slice will be written to.
    private int sliceBufferIndex;

    // The buffer for the offsets for all incoming blocks so far
    private byte[] offsetsBuffer;

    // The address that the next offset value will be written to.
    private int offsetsBufferIndex;

    // The last offset in the offsets buffer
    private int lastOffset;

    public VariableWidthBlockEncodingBuffer(ArrayAllocator bufferAllocator)
    {
        super(bufferAllocator);
    }

    @Override
    public void accumulateSerializedRowSizes(int[] serializedRowSizes)
    {
        int[] positions = getPositions();
        for (int i = 0; i < positionCount; i++) {
            serializedRowSizes[i] += POSITION_SIZE + decodedBlock.getSliceLength(positions[i]);
        }
    }

    @Override
    public void appendDataInBatch()
    {
        if (batchSize == 0) {
            return;
        }

        appendOffsetsAndSlices();
        appendNulls();
        bufferedPositionCount += batchSize;
    }

    @Override
    public void serializeTo(SliceOutput output)
    {
        writeLengthPrefixedString(output, NAME);

        output.writeInt(bufferedPositionCount);

        // offsets
        // note that VariableWidthBlock doesn't write the initial offset 0
        if (offsetsBufferIndex > 0) {
            output.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);
        }

        // nulls
        serializeNullsTo(output);

        // slice
        output.writeInt(sliceBufferIndex);  // totalLength
        if (sliceBufferIndex > 0) {
            output.appendBytes(sliceBuffer, 0, sliceBufferIndex);
        }
    }

    @Override
    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        sliceBufferIndex = 0;
        offsetsBufferIndex = 0;
        lastOffset = 0;
        resetNullsBuffer();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                getPositionsRetainedSizeInBytes() +
                sizeOf(offsetsBuffer) +
                getNullsBufferRetainedSizeInBytes() +
                sizeOf(sliceBuffer);
    }

    @Override
    public long getSerializedSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +    // NAME
                SIZE_OF_INT +                   // positionCount
                offsetsBufferIndex +            // offsets buffer.
                SIZE_OF_INT +                   // sliceBuffer size.
                sliceBufferIndex +               // sliceBuffer
                getNullsBufferSerializedSizeInBytes();  // nulls
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(getClass().getSimpleName()).append("{");
        sb.append("sliceBufferCapacity=").append(sliceBuffer == null ? 0 : sliceBuffer.length).append(",");
        sb.append("sliceBufferIndex=").append(sliceBufferIndex).append(",");
        sb.append("offsetsBufferCapacity=").append(offsetsBuffer == null ? 0 : offsetsBuffer.length).append(",");
        sb.append("offsetsBufferIndex=").append(offsetsBufferIndex).append("}");
        return sb.toString();
    }

    @Override
    protected void accumulateSerializedRowSizes(int[] positionOffsets, int positionCount, int[] serializedRowSizes)
    {
        // The nested level positionCount could be 0.
        if (this.positionCount == 0) {
            return;
        }

        int[] positions = getPositions();

        for (int i = 0; i < positionCount; i++) {
            for (int j = positionOffsets[i]; j < positionOffsets[i + 1]; j++) {
                serializedRowSizes[i] += POSITION_SIZE + decodedBlock.getSliceLength(positions[j]);
            }
        }
    }

    // This implementation uses variableWidthBlock.getRawSlice() and variableWidthBlock.getPositionOffset() to achieve high performance
    private void appendOffsetsAndSlices()
    {
        offsetsBuffer = ensureCapacity(offsetsBuffer, offsetsBufferIndex + batchSize * ARRAY_INT_INDEX_SCALE, LARGE, PRESERVE);

        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) decodedBlock;
        int[] positions = getPositions();

        // We need to use getRawSlice() to get the raw slice whose address is not advanced by getSlice(). It's incorrect to call getSlice()
        // because the returned slice's address may be advanced if it's based on a slice view.
        Slice rawSlice = variableWidthBlock.getRawSlice(0);
        byte[] sliceBase = (byte[]) rawSlice.getBase();

        // The slice's address starts from ARRAY_BYTE_BASE_OFFSET but when we read the bytes later in setBytes() the ARRAY_BYTE_BASE_OFFSET was added
        // inside, so we need to subtract it here. If sliceAddress < ARRAY_BYTE_BASE_OFFSET it's an empty slice and the sliceBuffer writing will be
        // guarded by the length check in the for loop, so the subtraction doesn't matter.
        int sliceAddress = (int) rawSlice.getAddress() - ARRAY_BYTE_BASE_OFFSET;

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
            int beginOffset = variableWidthBlock.getPositionOffset(position);
            int endOffset = variableWidthBlock.getPositionOffset(position + 1);
            int length = endOffset - beginOffset;

            lastOffset += length;
            offsetsBufferIndex = setIntUnchecked(offsetsBuffer, offsetsBufferIndex, lastOffset);

            if (length > 0) {
                sliceBuffer = ensureCapacity(sliceBuffer, sliceBufferIndex + length, LARGE, PRESERVE);

                // The slice address may be greater than 0. Since we are reading from the raw slice, we need to read from beginOffset + sliceAddress.
                sliceBufferIndex = setBytes(sliceBuffer, sliceBufferIndex, sliceBase, beginOffset + sliceAddress, length);
            }
        }
    }
}
