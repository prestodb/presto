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
package io.prestosql.plugin.raptor.legacy.storage;

import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Set;
import java.util.UUID;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.plugin.raptor.legacy.storage.FileStorageService.getFileSystemPath;
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
        File quarantine = new File(temporary, format("quarantine/%s.orc", uuid));
        assertEquals(store.getStagingFile(uuid), staging);
        assertEquals(store.getStorageFile(uuid), storage);
        assertEquals(store.getQuarantineFile(uuid), quarantine);
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

        File file = store.getStagingFile(randomUUID());
        store.createParents(file);
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
    public void testGetStorageShards()
            throws Exception
    {
        Set<UUID> shards = ImmutableSet.<UUID>builder()
                .add(UUID.fromString("9e7abb51-56b5-4180-9164-ad08ddfe7c63"))
                .add(UUID.fromString("bbfc3895-1c3d-4bf4-bca4-7b1198b1759e"))
                .build();

        for (UUID shard : shards) {
            File file = store.getStorageFile(shard);
            store.createParents(file);
            assertTrue(file.createNewFile());
        }

        File storage = new File(temporary, "storage");
        assertTrue(new File(storage, "abc").mkdir());
        assertTrue(new File(storage, "ab/cd").mkdirs());
        assertTrue(new File(storage, format("ab/cd/%s.junk", randomUUID())).createNewFile());
        assertTrue(new File(storage, "ab/cd/junk.orc").createNewFile());

        assertEquals(store.getStorageShards(), shards);
    }
}
