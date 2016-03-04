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
package com.facebook.presto.raptor.storage;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.UUID;

import static com.facebook.presto.raptor.storage.FileStorageService.getFileSystemPath;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
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
            throws Exception
    {
        temporary = createTempDir();
        store = new FileStorageService(temporary);
        store.start();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary);
    }

    @Test
    public void testGetFileSystemPath()
            throws Exception
    {
        UUID uuid = UUID.fromString("701e1a79-74f7-4f56-b438-b41e8e7d019d");
        File expected = new File("/test", format("70/1e/%s.orc", uuid));
        assertEquals(getFileSystemPath(new File("/test"), uuid), expected);
    }

    @Test
    public void testFilePaths()
    {
        UUID uuid = UUID.fromString("701e1a79-74f7-4f56-b438-b41e8e7d019d");
        File staging = new File(temporary, format("staging/%s.orc", uuid));
        File storage = new File(temporary, format("storage/70/1e/%s.orc", uuid));
        assertEquals(store.getStagingFile(uuid), staging);
        assertEquals(store.getStorageFile(uuid), storage);
    }

    @Test
    public void testStop()
            throws Exception
    {
        File staging = new File(temporary, "staging");
        File storage = new File(temporary, "storage");

        assertTrue(staging.mkdir());
        assertTrue(storage.mkdir());

        File file = store.getStagingFile(randomUUID());
        store.createParents(file);
        assertFalse(file.exists());
        assertTrue(file.createNewFile());
        assertFile(file);

        store.stop();

        assertFalse(file.exists());
        assertFalse(staging.exists());
        assertDirectory(storage);
    }
}
