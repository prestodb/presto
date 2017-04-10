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

import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.backup.FileBackupStore;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.io.Files;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static java.io.File.createTempFile;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestShardRecovery
{
    private StorageService storageService;
    private ShardRecoveryManager recoveryManager;
    private Handle dummyHandle;
    private File temporary;
    private FileBackupStore backupStore;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        temporary = createTempDir();
        File directory = new File(temporary, "data");
        File backupDirectory = new File(temporary, "backup");
        backupStore = new FileBackupStore(backupDirectory);
        backupStore.start();
        storageService = new FileStorageService(directory);
        storageService.start();

        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        createTablesWithRetry(dbi);
        ShardManager shardManager = createShardManager(dbi);
        recoveryManager = createShardRecoveryManager(storageService, Optional.of(backupStore), shardManager);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (dummyHandle != null) {
            dummyHandle.close();
        }
        deleteRecursively(temporary);
    }

    @SuppressWarnings("EmptyTryBlock")
    @Test
    public void testShardRecovery()
            throws Exception
    {
        UUID shardUuid = UUID.randomUUID();
        File file = storageService.getStorageFile(shardUuid);
        File tempFile = createTempFile("tmp", null, temporary);

        Files.write("test data", tempFile, UTF_8);

        backupStore.backupShard(shardUuid, tempFile);
        assertTrue(backupStore.shardExists(shardUuid));
        File backupFile = backupStore.getBackupFile(shardUuid);
        assertTrue(backupFile.exists());
        assertEquals(backupFile.length(), tempFile.length());

        assertFalse(file.exists());
        recoveryManager.restoreFromBackup(shardUuid, OptionalLong.empty());
        assertTrue(file.exists());
        assertEquals(file.length(), tempFile.length());
    }

    @SuppressWarnings("EmptyTryBlock")
    @Test
    public void testShardRecoveryExistingFileMismatch()
            throws Exception
    {
        UUID shardUuid = UUID.randomUUID();
        File file = storageService.getStorageFile(shardUuid);
        storageService.createParents(file);
        File tempFile = createTempFile("tmp", null, temporary);

        Files.write("test data", tempFile, UTF_8);
        Files.write("bad data", file, UTF_8);

        backupStore.backupShard(shardUuid, tempFile);

        long backupSize = tempFile.length();

        assertTrue(backupStore.shardExists(shardUuid));
        assertEquals(backupStore.getBackupFile(shardUuid).length(), backupSize);

        assertTrue(file.exists());
        assertNotEquals(file.length(), backupSize);

        recoveryManager.restoreFromBackup(shardUuid, OptionalLong.of(backupSize));

        assertTrue(file.exists());
        assertEquals(file.length(), backupSize);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No backup file found for shard: .*")
    public void testNoBackupException()
            throws Exception
    {
        recoveryManager.restoreFromBackup(UUID.randomUUID(), OptionalLong.empty());
    }

    public static ShardRecoveryManager createShardRecoveryManager(
            StorageService storageService,
            Optional<BackupStore> backupStore,
            ShardManager shardManager)
    {
        return new ShardRecoveryManager(
                storageService,
                backupStore,
                new TestingNodeManager(),
                shardManager,
                new Duration(5, MINUTES),
                10);
    }
}
