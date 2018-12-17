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
package com.facebook.presto.raptorx.metadata;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class InMemoryChunkRecorder
        implements ChunkRecorder
{
    private final List<RecordedChunk> chunks = new ArrayList<>();

    public List<RecordedChunk> getChunks()
    {
        return ImmutableList.copyOf(chunks);
    }

    @Override
    public void recordCreatedChunk(long transactionId, long tableId, long chunkId, long size)
    {
        chunks.add(new RecordedChunk(transactionId, chunkId));
    }

    public static class RecordedChunk
    {
        private final long transactionId;
        private final long chunkId;

        public RecordedChunk(long transactionId, long chunkId)
        {
            this.transactionId = transactionId;
            this.chunkId = requireNonNull(chunkId, "chunkId is null");
        }

        public long getTransactionId()
        {
            return transactionId;
        }

        public long getChunkId()
        {
            return chunkId;
        }
    }
}
