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
import org.testng.annotations.Test;

import java.io.File;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestBackupStore<T extends BackupStore>
{
    protected File temporary;
    protected T store;

    @Test
    public void testBackupStore()
            throws Exception
    {
        // backup first file
        File file1 = new File(temporary, "file1");
        Files.write("hello world", file1, UTF_8);
        UUID uuid1 = randomUUID();

        assertFalse(store.shardExists(uuid1));
        store.backupShard(uuid1, file1);
        assertTrue(store.shardExists(uuid1));

        // backup second file
        File file2 = new File(temporary, "file2");
        Files.write("bye bye", file2, UTF_8);
        UUID uuid2 = randomUUID();

        assertFalse(store.shardExists(uuid2));
        store.backupShard(uuid2, file2);
        assertTrue(store.shardExists(uuid2));

        // verify first file
        File restore1 = new File(temporary, "restore1");
        store.restoreShard(uuid1, restore1);
        assertEquals(readAllBytes(file1.toPath()), readAllBytes(restore1.toPath()));

        // verify second file
        File restore2 = new File(temporary, "restore2");
        store.restoreShard(uuid2, restore2);
        assertEquals(readAllBytes(file2.toPath()), readAllBytes(restore2.toPath()));

        // verify random UUID does not exist
        assertFalse(store.shardExists(randomUUID()));

        // delete first file
        assertTrue(store.shardExists(uuid1));
        assertTrue(store.shardExists(uuid2));

        store.deleteShard(uuid1);
        store.deleteShard(uuid1);

        assertFalse(store.shardExists(uuid1));
        assertTrue(store.shardExists(uuid2));

        // delete random UUID
        store.deleteShard(randomUUID());
    }
}
