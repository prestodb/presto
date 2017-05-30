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

import java.io.File;

public interface ChunkStore
{
    /**
     * Write a chunk to the chunk store.
     *
     * @param chunkId chunk ID
     * @param source the source file
     */
    void putChunk(long chunkId, File source);

    /**
     * Read a chunk from the chunk store.
     *
     * @param chunkId chunk ID
     * @param target the destination file
     */
    void getChunk(long chunkId, File target);

    /**
     * Delete chunk from the chunk store if it exists.
     *
     * @param chunkId chunk ID
     * @return {@code true} if the chunk was deleted; {@code false} if it did not exist
     */
    boolean deleteChunk(long chunkId);

    /**
     * Check if a chunk exists in the chunk store.
     *
     * @param chunkId chunk ID
     * @return if the chunk exists
     */
    boolean chunkExists(long chunkId);
}
