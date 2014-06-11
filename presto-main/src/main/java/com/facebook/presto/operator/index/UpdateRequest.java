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
package com.facebook.presto.operator.index;

import com.facebook.presto.spi.block.Block;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
class UpdateRequest
{
    private final int startPosition;
    private final Block[] blocks;
    private final AtomicBoolean finished = new AtomicBoolean();

    public UpdateRequest(int startPosition, Block... blocks)
    {
        this.startPosition = startPosition;
        this.blocks = checkNotNull(blocks, "blocks is null");
    }

    public int getStartPosition()
    {
        return startPosition;
    }

    public Block[] getBlocks()
    {
        return blocks;
    }

    public void finished()
    {
        finished.set(true);
    }

    public boolean isFinished()
    {
        return finished.get();
    }
}
