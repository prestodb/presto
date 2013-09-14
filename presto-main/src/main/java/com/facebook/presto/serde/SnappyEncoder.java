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
package com.facebook.presto.serde;

import com.facebook.presto.block.snappy.SnappyBlock;
import com.facebook.presto.tuple.Tuple;
import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.iq80.snappy.Snappy;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class SnappyEncoder
        implements Encoder
{
    public static final int MAX_UNCOMPRESSED_BLOCK_SIZE = (int) new DataSize(1, MEGABYTE).toBytes();

    private final SliceOutput sliceOutput;
    private final DynamicSliceOutput buffer = new DynamicSliceOutput(MAX_UNCOMPRESSED_BLOCK_SIZE);

    private SnappyBlockEncoding encoding;
    private boolean finished;
    private int tupleCount;

    public SnappyEncoder(SliceOutput sliceOutput)
    {
        this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
    }

    @Override
    public Encoder append(Iterable<Tuple> tuples)
    {
        Preconditions.checkNotNull(tuples, "tuples is null");
        checkState(!finished, "already finished");

        for (Tuple tuple : tuples) {
            if (encoding == null) {
                encoding = new SnappyBlockEncoding(tuple.getTupleInfo());
            }
            tuple.writeTo(buffer);
            tupleCount++;

            if (buffer.size() >= MAX_UNCOMPRESSED_BLOCK_SIZE) {
                flushBlock();
            }
        }
        return this;
    }

    @Override
    public BlockEncoding finish()
    {
        checkState(encoding != null, "nothing appended");
        checkState(!finished, "already finished");

        finished = true;
        flushBlock();

        return encoding;
    }

    private void flushBlock()
    {
        if (buffer.size() > 0) {
            Slice slice = buffer.slice();
            byte[] compressedBytes = new byte[Snappy.maxCompressedLength(slice.length())];
            int actualLength = Snappy.compress(slice.getBytes(), 0, slice.length(), compressedBytes, 0);
            SnappyBlock block = new SnappyBlock(tupleCount, encoding.getTupleInfo(), Slices.wrappedBuffer(Arrays.copyOf(compressedBytes, actualLength)));
            encoding.writeBlock(sliceOutput, block);
            buffer.reset();
            tupleCount = 0;
        }
    }
}
