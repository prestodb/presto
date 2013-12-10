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

import com.facebook.presto.spi.block.BlockCursor;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
class UpdateRequest
{
    private final List<BlockCursor> cursors;
    private final AtomicBoolean finished = new AtomicBoolean();

    public UpdateRequest(BlockCursor... cursors)
    {
        this(ImmutableList.copyOf(checkNotNull(cursors, "cursors is null")));
    }

    public UpdateRequest(List<BlockCursor> cursors)
    {
        this.cursors = ImmutableList.copyOf(checkNotNull(cursors, "cursors is null"));
    }

    public BlockCursor[] duplicateCursors()
    {
        BlockCursor[] duplicates = new BlockCursor[cursors.size()];
        for (int i = 0; i < cursors.size(); i++) {
            BlockCursor cursor = cursors.get(i);
            duplicates[i] = cursor.duplicate();
        }
        return duplicates;
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
