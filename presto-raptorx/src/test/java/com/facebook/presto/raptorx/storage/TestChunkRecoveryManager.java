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
package com.facebook.presto.raptorx.storage;

import com.facebook.presto.raptorx.chunkstore.FileChunkStore;
import com.facebook.presto.raptorx.metadata.ChunkFile;
import com.facebook.presto.raptorx.metadata.ChunkManager;
import com.facebook.presto.spi.PrestoException;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.raptorx.RaptorErrorCode.RAPTOR_CHUNK_STORE_CORRUPTION;
import static com.facebook.presto.raptorx.util.StorageUtil.xxhash64;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.io.File.createTempFile;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestChunkRecoveryManager
{
    private static final long TABLE_ID = 42;

    private File temporary;
    private FileChunkStore chunkStore;
    private StorageService storageService;
    private TestingChunkManager chunkManager;
    private ChunkRecoveryManager recoveryManager;

    @BeforeMethod
    public void setup()
    {
        temporary = createTempDir();
        chunkStore = new FileChunkStore(new File(temporary, "chunkstore"));
        chunkStore.start();
        storageService = new FileStorageService(new File(temporary, "data"));
        storageService.start();
        chunkManager = new TestingChunkManager();
        recoveryManager = new ChunkRecoveryManager(
                storageService,
                chunkStore,
                chunkManager,
                "test",
                new Duration(5, MINUTES),
                2);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testChunkRecovery()
            throws Exception
    {
        long chunkId = 123;
        File file = storageService.getStorageFile(chunkId);
        File tempFile = createTempFile("tmp", null, temporary);

        byte[] data = "test data".getBytes(UTF_8);
        writeFile(tempFile, data);

        chunkManager.addChunk(new ChunkFile(chunkId, tempFile.length(), xxhash64(tempFile)));

        chunkStore.putChunk(chunkId, tempFile);
        assertTrue(chunkStore.chunkExists(chunkId));
        File chunkStoreFile = chunkStore.getChunkFile(chunkId);
        assertTrue(chunkStoreFile.exists());
        assertEquals(readFile(chunkStoreFile), data);

        assertFalse(file.exists());
        recoverChunk(chunkId);
        assertTrue(file.exists());
        assertEquals(readFile(tempFile), data);
    }

    @Test
    public void testExistingFileSizeMismatch()
            throws Exception
    {
        long chunkId = 123;

        // write data
        File tempFile = createTempFile("tmp", null, temporary);
        byte[] data = "test data".getBytes(UTF_8);
        writeFile(tempFile, data);

        chunkManager.addChunk(new ChunkFile(chunkId, tempFile.length(), xxhash64(tempFile)));

        chunkStore.putChunk(chunkId, tempFile);
        assertTrue(chunkStore.chunkExists(chunkId));

        File chunkStoreFile = chunkStore.getChunkFile(chunkId);
        assertEquals(readFile(chunkStoreFile), data);

        // write corrupt storage file with wrong length
        File storageFile = storageService.getStorageFile(chunkId);

        byte[] corrupt = "bad data".getBytes(UTF_8);
        writeFile(storageFile, corrupt);

        assertTrue(storageFile.exists());
        assertNotEquals(storageFile.length(), tempFile.length());
        assertNotEquals(readFile(storageFile), data);

        // recover and verify
        recoverChunk(chunkId);

        assertTrue(storageFile.exists());
        assertEquals(readFile(storageFile), data);

        // verify quarantine exists
        assertTrue(storageService.getQuarantineFile(chunkId).exists());
        assertEquals(readFile(storageService.getQuarantineFile(chunkId)), corrupt);
    }

    @Test
    public void testExistingFileChecksumMismatch()
            throws Exception
    {
        long chunkId = 123;

        // write data and to chunk store
        File tempFile = createTempFile("tmp", null, temporary);
        byte[] data = "test data".getBytes(UTF_8);
        writeFile(tempFile, data);

        chunkManager.addChunk(new ChunkFile(chunkId, tempFile.length(), xxhash64(tempFile)));

        chunkStore.putChunk(chunkId, tempFile);
        assertTrue(chunkStore.chunkExists(chunkId));

        File chunkStoreFile = chunkStore.getChunkFile(chunkId);
        assertEquals(readFile(chunkStoreFile), data);

        // write corrupt storage file with wrong data
        File storageFile = storageService.getStorageFile(chunkId);

        byte[] corrupt = "test xata".getBytes(UTF_8);
        writeFile(storageFile, corrupt);

        assertTrue(storageFile.exists());
        assertEquals(storageFile.length(), tempFile.length());
        assertNotEquals(readFile(storageFile), data);

        // recover and verify
        recoverChunk(chunkId);

        assertTrue(storageFile.exists());
        assertEquals(readFile(storageFile), data);

        // verify quarantine exists
        assertTrue(storageService.getQuarantineFile(chunkId).exists());
        assertEquals(readFile(storageService.getQuarantineFile(chunkId)), corrupt);
    }

    @Test
    public void testChunkStoreChecksumMismatch()
            throws Exception
    {
        long chunkId = 123;

        // write storage file
        File storageFile = storageService.getStorageFile(chunkId);

        byte[] data = "test data".getBytes(UTF_8);
        writeFile(storageFile, data);

        chunkManager.addChunk(new ChunkFile(chunkId, storageFile.length(), xxhash64(storageFile)));

        // write and verify
        chunkStore.putChunk(chunkId, storageFile);

        assertTrue(chunkStore.chunkExists(chunkId));
        File chunkStoreFile = chunkStore.getChunkFile(chunkId);
        assertEquals(readFile(chunkStoreFile), data);

        // corrupt chunk store file
        byte[] corrupt = "test xata".getBytes(UTF_8);
        writeFile(chunkStoreFile, corrupt);

        assertTrue(chunkStoreFile.exists());
        assertEquals(storageFile.length(), chunkStoreFile.length());
        assertNotEquals(readFile(chunkStoreFile), data);

        // delete local file to force recovery
        assertTrue(storageFile.delete());
        assertFalse(storageFile.exists());

        // recover should fail
        try {
            recoverChunk(chunkId);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), RAPTOR_CHUNK_STORE_CORRUPTION.toErrorCode());
            assertEquals(e.getMessage(), "Chunk is corrupt in chunk store: " + chunkId);
        }

        // verify quarantine exists
        assertTrue(storageService.getQuarantineFile(chunkId).exists());
        assertEquals(readFile(storageService.getQuarantineFile(chunkId)), corrupt);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Chunk not found in chunk store: 456")
    public void testNoChunkStoreChunkException()
            throws Exception
    {
        chunkManager.addChunk(new ChunkFile(456, 0, 0));
        recoverChunk(456);
    }

    @Test(timeOut = 5_000)
    public void testRecoverMissingChunks()
            throws Exception
    {
        long chunkId = 123;
        File storageFile = storageService.getStorageFile(chunkId);
        File tempFile = createTempFile("tmp", null, temporary);

        byte[] data = "test data".getBytes(UTF_8);
        writeFile(tempFile, data);

        chunkManager.addChunk(new ChunkFile(chunkId, tempFile.length(), xxhash64(tempFile)));

        chunkStore.putChunk(chunkId, tempFile);
        assertTrue(chunkStore.chunkExists(chunkId));
        File chunkStoreFile = chunkStore.getChunkFile(chunkId);
        assertTrue(chunkStoreFile.exists());
        assertEquals(readFile(chunkStoreFile), data);

        assertFalse(storageFile.exists());
        recoveryManager.recoverMissingChunks();
        while (!storageFile.exists()) {
            MILLISECONDS.sleep(10);
        }
        assertTrue(storageFile.exists());
        assertEquals(readFile(storageFile), data);
    }

    private void recoverChunk(long chunkId)
            throws TimeoutException, InterruptedException, ExecutionException
    {
        try {
            recoveryManager.recoverChunk(TABLE_ID, chunkId).get(5, SECONDS);
        }
        catch (ExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw e;
        }
    }

    private static class TestingChunkManager
            implements ChunkManager
    {
        private final Set<ChunkFile> chunks = new HashSet<>();

        public void addChunk(ChunkFile chunk)
        {
            chunks.add(chunk);
        }

        @Override
        public Set<ChunkFile> getNodeChunks(String nodeIdentifier)
        {
            return chunks;
        }

        @Override
        public ChunkFile getChunk(long tableId, long chunkId)
        {
            checkArgument(tableId == TABLE_ID);
            return chunks.stream()
                    .filter(chunk -> chunk.getChunkId() == chunkId)
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("no chunk: " + chunkId));
        }
    }

    private static byte[] readFile(File file)
            throws IOException
    {
        return Files.readAllBytes(file.toPath());
    }

    private static void writeFile(File file, byte[] data)
            throws IOException
    {
        Files.write(file.toPath(), data);
    }
}
