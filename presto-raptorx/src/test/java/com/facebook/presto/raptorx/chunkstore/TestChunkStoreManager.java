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

import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.io.Files.asCharSink;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.Futures.allAsList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static java.util.stream.LongStream.rangeClosed;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestChunkStoreManager
{
    private File temporary;
    private FileChunkStore store;
    private ChunkStoreManager chunkStoreManager;

    @BeforeClass
    public void setup()
    {
        temporary = createTempDir();
        store = new FileChunkStore(new File(temporary, "data"));
        store.start();
        chunkStoreManager = new ChunkStoreManager(store, 5);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
        chunkStoreManager.shutdown();
    }

    @Test
    public void testSimple()
            throws Exception
    {
        List<ListenableFuture<?>> futures = new ArrayList<>();
        List<Long> chunkIds = rangeClosed(1, 5).boxed().collect(toList());

        for (Long chunkId : chunkIds) {
            File file = new File(temporary, "file" + chunkId);
            asCharSink(file, UTF_8).write("hello world");
            futures.add(chunkStoreManager.submit(chunkId, file));
        }

        getFutureValue(allAsList(futures));

        for (long chunkId : chunkIds) {
            assertTrue(store.chunkExists(chunkId));
        }
    }
}
