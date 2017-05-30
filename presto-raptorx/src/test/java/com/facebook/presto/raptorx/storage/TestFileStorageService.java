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

import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Set;

import static com.facebook.presto.raptorx.storage.FileStorageService.getFileSystemPath;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.assertDirectory;
import static org.testng.FileAssert.assertFile;

@Test(singleThreaded = true)
public class TestFileStorageService
{
    private File temporary;
    private FileStorageService store;

    @BeforeMethod
    public void setup()
    {
        temporary = createTempDir();
        store = new FileStorageService(temporary);
        store.start();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
    }

    @Test
    public void testGetFileSystemPath()
    {
        assertThat(getFileSystemPath(new File("/test"), 123456))
                .isEqualTo(new File("/test", "b4/dc/123456"));
    }

    @Test
    public void testFilePaths()
    {
        long chunkId = 123456;
        File staging = new File(temporary, "staging/123456");
        File storage = new File(temporary, "storage/b4/dc/123456");
        File quarantine = new File(temporary, "quarantine/123456");
        assertEquals(store.getStagingFile(chunkId), staging);
        assertEquals(store.getStorageFile(chunkId), storage);
        assertEquals(store.getQuarantineFile(chunkId), quarantine);
    }

    @Test
    public void testStop()
            throws Exception
    {
        File staging = new File(temporary, "staging");
        File storage = new File(temporary, "storage");
        File quarantine = new File(temporary, "quarantine");

        assertDirectory(staging);
        assertDirectory(storage);
        assertDirectory(quarantine);

        File file = store.getStagingFile(987654);
        assertFalse(file.exists());
        assertTrue(file.createNewFile());
        assertFile(file);

        store.stop();

        assertFalse(file.exists());
        assertFalse(staging.exists());
        assertDirectory(storage);
        assertDirectory(quarantine);
    }

    @Test
    public void testGetStorageChunks()
            throws Exception
    {
        Set<Long> chunkIds = ImmutableSet.of(1234L, 5678L);

        for (long chunkId : chunkIds) {
            assertTrue(store.getStorageFile(chunkId).createNewFile());
        }

        File storage = new File(temporary, "storage");
        assertTrue(new File(storage, "abc").mkdir());
        assertTrue(new File(storage, "ab/cd").mkdirs());
        assertTrue(new File(storage, "ab/cd/123.junk").createNewFile());
        assertTrue(new File(storage, "ab/cd/junk").createNewFile());

        assertEquals(store.getStorageChunks(), chunkIds);
    }
}
