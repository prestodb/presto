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
package com.facebook.presto.raptorx.chunkstore;

import io.airlift.log.Logger;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.File;

import static java.util.Objects.requireNonNull;

public class ManagedChunkStore
        implements ChunkStore
{
    private final ChunkStore store;
    private final Logger log;

    private final ChunkStoreOperationStats putChunk = new ChunkStoreOperationStats();
    private final ChunkStoreOperationStats getChunk = new ChunkStoreOperationStats();
    private final ChunkStoreOperationStats deleteChunk = new ChunkStoreOperationStats();
    private final ChunkStoreOperationStats chunkExists = new ChunkStoreOperationStats();

    public ManagedChunkStore(ChunkStore store)
    {
        this.store = requireNonNull(store, "store is null");
        this.log = Logger.get(store.getClass());
    }

    @Override
    public void putChunk(long chunkId, File source)
    {
        log.debug("Chunk store put: %s", chunkId);
        putChunk.run(() -> store.putChunk(chunkId, source));
    }

    @Override
    public void getChunk(long chunkId, File target)
    {
        log.debug("Chunk store get: %s", chunkId);
        getChunk.run(() -> store.getChunk(chunkId, target));
    }

    @Override
    public boolean deleteChunk(long chunkId)
    {
        log.debug("Chunk store delete: %s", chunkId);
        return deleteChunk.run(() -> store.deleteChunk(chunkId));
    }

    @Override
    public boolean chunkExists(long chunkId)
    {
        return chunkExists.run(() -> store.chunkExists(chunkId));
    }

    @Managed
    @Nested
    public ChunkStoreOperationStats getPutChunk()
    {
        return putChunk;
    }

    @Managed
    @Nested
    public ChunkStoreOperationStats getGetChunk()
    {
        return getChunk;
    }

    @Managed
    @Nested
    public ChunkStoreOperationStats getDeleteChunk()
    {
        return deleteChunk;
    }

    @Managed
    @Nested
    public ChunkStoreOperationStats getChunkExists()
    {
        return chunkExists;
    }
}
