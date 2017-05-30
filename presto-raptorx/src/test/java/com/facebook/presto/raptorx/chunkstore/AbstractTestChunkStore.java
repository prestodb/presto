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

import org.testng.annotations.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Files.write;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestChunkStore<T extends ChunkStore>
{
    private final AtomicLong nextChunkId = new AtomicLong(1);

    protected File temporary;
    protected T store;

    @Test
    public void testChunkStore()
            throws Exception
    {
        // write first file
        File file1 = new File(temporary, "file1");
        byte[] data1 = "hello world".getBytes(UTF_8);
        write(file1.toPath(), data1);
        long id1 = nextChunkId.getAndIncrement();

        assertFalse(store.chunkExists(id1));
        store.putChunk(id1, file1);
        assertTrue(store.chunkExists(id1));

        // write second file
        File file2 = new File(temporary, "file2");
        byte[] data2 = "bye bye".getBytes(UTF_8);
        write(file2.toPath(), data2);
        long id2 = nextChunkId.getAndIncrement();

        assertFalse(store.chunkExists(id2));
        store.putChunk(id2, file2);
        assertTrue(store.chunkExists(id2));

        // verify first file
        File restore1 = new File(temporary, "restore1");
        store.getChunk(id1, restore1);
        assertEquals(readAllBytes(restore1.toPath()), data1);

        // verify second file
        File restore2 = new File(temporary, "restore2");
        store.getChunk(id2, restore2);
        assertEquals(readAllBytes(restore2.toPath()), data2);

        // verify random chunk ID does not exist
        assertFalse(store.chunkExists(nextChunkId.getAndIncrement()));

        // delete first file
        assertTrue(store.chunkExists(id1));
        assertTrue(store.chunkExists(id2));

        assertTrue(store.deleteChunk(id1));
        assertFalse(store.deleteChunk(id1));

        assertFalse(store.chunkExists(id1));
        assertTrue(store.chunkExists(id2));

        // delete random chunk ID
        assertFalse(store.deleteChunk(nextChunkId.getAndIncrement()));
    }
}
