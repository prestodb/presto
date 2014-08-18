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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class UncompressedEncoder
        implements Encoder
{
    private final Type type;
    private final SliceOutput sliceOutput;
    private final BlockEncoding encoding;
    private BlockBuilder blockBuilder;

    private boolean finished;

    public UncompressedEncoder(Type type, SliceOutput sliceOutput)
    {
        this.type = checkNotNull(type, "type is null");
        this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");

        this.blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        this.encoding = blockBuilder.getEncoding();
    }

    @Override
    public Encoder append(Block block)
    {
        checkNotNull(block, "block is null");
        checkState(!finished, "already finished");

        for (int position = 0; position < block.getPositionCount(); position++) {
            type.appendTo(block, position, blockBuilder);
            if (blockBuilder.isFull()) {
                writeBlock();
            }
        }

        return this;
    }

    @Override
    public BlockEncoding finish()
    {
        checkState(!finished, "already finished");
        finished = true;

        if (!blockBuilder.isEmpty()) {
            writeBlock();
        }
        return encoding;
    }

    private void writeBlock()
    {
        Block block = blockBuilder.build();
        encoding.writeBlock(sliceOutput, block);
        blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
    }
}
