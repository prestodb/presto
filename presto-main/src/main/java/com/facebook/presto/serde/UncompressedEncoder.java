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

import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.tuple.Tuple;
import com.google.common.base.Preconditions;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class UncompressedEncoder
        implements Encoder
{
    private static final int MAX_BLOCK_SIZE = (int) new DataSize(64, KILOBYTE).toBytes();

    private final SliceOutput sliceOutput;
    private final DynamicSliceOutput buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);

    private UncompressedBlockEncoding encoding;
    private boolean finished;
    private int tupleCount;

    public UncompressedEncoder(SliceOutput sliceOutput)
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
                encoding = new UncompressedBlockEncoding(tuple.getTupleInfo());
            }
            tuple.writeTo(buffer);
            tupleCount++;

            if (buffer.size() >= MAX_BLOCK_SIZE) {
                writeBlock();
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

        if (buffer.size() > 0) {
            writeBlock();
        }
        return encoding;
    }

    private void writeBlock()
    {
        Slice slice = buffer.slice();
        UncompressedBlock block = new UncompressedBlock(tupleCount, encoding.getTupleInfo(), slice);
        encoding.writeBlock(sliceOutput, block);
        buffer.reset();
        tupleCount = 0;
    }
}
