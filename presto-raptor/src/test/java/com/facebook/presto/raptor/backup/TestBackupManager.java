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
package com.facebook.presto.raptor.backup;

import com.google.common.io.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.google.common.io.Files.createTempDir;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestBackupManager
{
    private File temporary;
    private FileBackupStore store;
    private BackupManager backupManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        temporary = createTempDir();
        store = new FileBackupStore(new File(temporary, "backup"));
        store.start();
        backupManager = new BackupManager(Optional.of(store), 5);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary);
        backupManager.shutdown();
    }

    @Test
    public void testSimple()
            throws Exception
    {
        List<CompletableFuture<?>> futures = new ArrayList<>();
        List<UUID> uuids = new ArrayList<>(5);
        for (int i = 0; i < 5; i++) {
            File file = new File(temporary, "file" + i);
            Files.write("hello world", file, UTF_8);
            uuids.add(randomUUID());

            futures.add(backupManager.submit(uuids.get(i), file));
        }
        futures.forEach(CompletableFuture::join);
        for (UUID uuid : uuids) {
            assertTrue(store.shardExists(uuid));
        }
    }
}
