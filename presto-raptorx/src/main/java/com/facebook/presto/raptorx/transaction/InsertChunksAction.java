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
package com.facebook.presto.raptorx.transaction;

import com.facebook.presto.raptorx.storage.ChunkInfo;
import com.google.common.collect.ImmutableList;

import java.util.Collection;

public class InsertChunksAction
        implements Action
{
    private final long tableId;
    private final Collection<ChunkInfo> chunks;

    public InsertChunksAction(long tableId, Collection<ChunkInfo> chunks)
    {
        this.tableId = tableId;
        this.chunks = ImmutableList.copyOf(chunks);
    }

    public long getTableId()
    {
        return tableId;
    }

    public Collection<ChunkInfo> getChunks()
    {
        return chunks;
    }

    @Override
    public void accept(ActionVisitor visitor)
    {
        visitor.visitInsertChunks(this);
    }
}
