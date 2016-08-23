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
package com.facebook.presto.operator;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.array.BooleanArray;
import com.facebook.presto.spi.block.array.BooleanArraySlice;
import com.facebook.presto.spi.block.array.IntArray;
import com.facebook.presto.spi.block.array.IntArraySlice;
import com.facebook.presto.spi.block.array.LongArray;
import com.facebook.presto.spi.block.array.LongArrayList;
import com.facebook.presto.spi.block.array.LongArraySlice;
import com.facebook.presto.spi.block.resource.BlockResourceContext;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
public class SpillBlockResourceContext
        implements BlockResourceContext
{
    private final File dir;
    private final List<ByteBuffer> buffers = new ArrayList<>();

    public SpillBlockResourceContext(File dir)
    {
        this.dir = dir;
        this.dir.mkdirs();
    }

    @Override
    public BooleanArray newBooleanArray(int size)
    {
        Slice slice = createOffHeapSlice(size);
        return new BooleanArraySlice(slice);
    }

    @Override
    public BooleanArray copyOfRangeBooleanArray(BooleanArray booleanArray, int offset, int length)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "not impl");
    }

    @Override
    public IntArray newIntArray(int size)
    {
        Slice slice = createOffHeapSlice(size * 4);
        return new IntArraySlice(slice);
    }

    @Override
    public IntArray copyOfRangeIntArray(IntArray intArray, int offset, int length)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "not impl");
    }

    @Override
    public LongArray newLongArray(int size)
    {
        Slice slice = createOffHeapSlice(size * 8);
        return new LongArraySlice(slice);
    }

    @Override
    public LongArray copyOfRangeLongArray(LongArray values, int offset, int length)
    {
        throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "not impl");
    }

    @Override
    public Slice newSlice(int size)
    {
        return createOffHeapSlice(size);
    }

    @Override
    public Slice copyOfSlice(Slice slice, int offset, int length)
    {
        return createAndCopyOffHeapSlice(slice, offset, length);
    }

    @Override
    public Slice copyOfSlice(Slice slice)
    {
        return copyOfSlice(slice, 0, slice.length());
    }

    @Override
    public LongArrayList newLongArrayList(int expectedPositions)
    {
        return new LongArrayListSlice(expectedPositions, this);
    }

    private Slice createOffHeapSlice(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        ByteBuffer offHeapBuffer = getCreateOffHeapByteBuffer(length);
        return Slices.wrappedBuffer(offHeapBuffer);
    }

    private Slice createAndCopyOffHeapSlice(Slice slice, int offset, int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        ByteBuffer offHeapBuffer = getCreateOffHeapByteBuffer(length);
        // write slice data
        Slice offHeapSlice = Slices.wrappedBuffer(offHeapBuffer);
        offHeapSlice.setBytes(0, slice, offset, length);
        return offHeapSlice;
    }

    private ByteBuffer getCreateOffHeapByteBuffer(int length)
    {
        ByteBuffer duplicate = getDuplicatedRawBuffer(length);
        duplicate.limit(duplicate.position() + length);
        return duplicate.slice();
    }

    private synchronized ByteBuffer getDuplicatedRawBuffer(int length)
    {
        if (!buffers.isEmpty()) {
            for (ByteBuffer buffer : buffers) {
                if (hasSpace(buffer, length)) {
                    return allocateAndDuplicate(buffer, length);
                }
            }
        }
        return allocateAndDuplicate(createMappedByteBuffer(), length);
    }

    private ByteBuffer allocateAndDuplicate(ByteBuffer buffer, int length)
    {
        ByteBuffer duplicate = buffer.duplicate();
        buffer.position(buffer.position() + length);
        return duplicate;
    }

    private boolean hasSpace(ByteBuffer buffer, int length)
    {
        return buffer.remaining() >= length;
    }

    private ByteBuffer createMappedByteBuffer()
    {
        try {
            // @TODO use NIO 2 with SPARSE flag.
            try (RandomAccessFile file = new RandomAccessFile(new File(dir, UUID.randomUUID().toString()), "rw")) {
                file.setLength(Integer.MAX_VALUE);
                MappedByteBuffer byteBuffer = file.getChannel().map(MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
                buffers.add(byteBuffer);
                return byteBuffer;
            }
        }
        catch (IOException e) {
            throw new PrestoException(StandardErrorCode.GENERIC_EXTERNAL, "IOException", e);
        }
    }

    @Override
    public void cleanup()
    {
        rmr(dir);
    }

    private static void rmr(File file)
    {
        if (!file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rmr(f);
            }
        }
        file.delete();
    }
}
