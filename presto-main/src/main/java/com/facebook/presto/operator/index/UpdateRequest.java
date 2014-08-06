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

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ThreadSafe
class UpdateRequest
{
    private final Block[] blocks;
    private final AtomicReference<IndexSnapshot> indexSnapshotReference = new AtomicReference<>();

    public UpdateRequest(Block... blocks)
    {
        this.blocks = checkNotNull(blocks, "blocks is null");
    }

    public Block[] getBlocks()
    {
        return blocks;
    }

    public void finished(IndexSnapshot indexSnapshot)
    {
        checkNotNull(indexSnapshot, "indexSnapshot is null");
        checkState(indexSnapshotReference.compareAndSet(null, indexSnapshot), "Already finished!");
    }

    public boolean isFinished()
    {
        return indexSnapshotReference.get() != null;
    }

    public IndexSnapshot getFinishedIndexSnapshot()
    {
        IndexSnapshot indexSnapshot = indexSnapshotReference.get();
        checkState(indexSnapshot != null, "Update request is not finished");
        return indexSnapshot;
    }
}
