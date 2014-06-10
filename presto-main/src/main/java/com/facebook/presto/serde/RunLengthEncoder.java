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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.block.rle.RunLengthBlockEncoding;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RunLengthEncoder
        implements Encoder
{
    private final SliceOutput sliceOutput;
    private boolean finished;

    private int positionCount;
    private Block lastValue;
    private RunLengthBlockEncoding encoding;

    public RunLengthEncoder(SliceOutput sliceOutput)
    {
        this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
    }

    @Override
    public Encoder append(Block block)
    {
        checkNotNull(block, "block is null");
        checkState(!finished, "already finished");

        if (encoding == null) {
            encoding = new RunLengthBlockEncoding(block.getEncoding());
        }

        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            if (lastValue == null) {
                lastValue = cursor.getSingleValueBlock();
            }
            else if (!lastValue.equalTo(0, cursor)) {
                writeBlock();
                lastValue = cursor.getSingleValueBlock();
            }
            positionCount++;
        }

        return this;
    }

    private void writeBlock()
    {
        RunLengthEncodedBlock block = new RunLengthEncodedBlock(lastValue, positionCount);

        encoding.writeBlock(sliceOutput, block);
        lastValue = null;
        positionCount = 0;
    }

    @Override
    public BlockEncoding finish()
    {
        checkState(encoding != null, "nothing appended");
        checkState(!finished, "already finished");
        finished = true;

        // Flush out final block if there exists one (null if they were all empty blocks)
        if (positionCount > 0) {
            writeBlock();
        }

        return encoding;
    }
}
