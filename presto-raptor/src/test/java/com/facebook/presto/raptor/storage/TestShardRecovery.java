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
import com.facebook.presto.raptor.filesystem.LocalFileStorageService;
import com.facebook.presto.raptor.filesystem.LocalOrcDataEnvironment;
import com.facebook.presto.raptor.filesystem.RaptorLocalFileSystem;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.TestingNodeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_BACKUP_CORRUPTION;
import static com.facebook.presto.raptor.filesystem.FileSystemUtil.xxhash64;
import static com.facebook.presto.raptor.metadata.SchemaDaoUtil.createTablesWithRetry;
import static com.facebook.presto.raptor.metadata.TestDatabaseShardManager.createShardManager;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.io.File.createTempFile;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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
    {
        temporary = createTempDir();
        File directory = new File(temporary, "data");
        File backupDirectory = new File(temporary, "backup");
        backupStore = new FileBackupStore(backupDirectory);
        backupStore.start();
        storageService = new LocalFileStorageService(new LocalOrcDataEnvironment(), directory.toURI());
        storageService.start();

        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt());
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
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
    }

    @SuppressWarnings("EmptyTryBlock")
    @Test
    public void testShardRecovery()
            throws Exception
    {
        UUID shardUuid = UUID.randomUUID();
        File file = new File(storageService.getStorageFile(shardUuid).toString());
        File tempFile = createTempFile("tmp", null, temporary);

        Files.write("test data", tempFile, UTF_8);

        backupStore.backupShard(shardUuid, tempFile);
        assertTrue(backupStore.shardExists(shardUuid));
        File backupFile = backupStore.getBackupFile(shardUuid);
        assertTrue(backupFile.exists());
        assertEquals(backupFile.length(), tempFile.length());

        assertFalse(file.exists());
        recoveryManager.restoreFromBackup(shardUuid, tempFile.length(), OptionalLong.empty());
        assertTrue(file.exists());
        assertEquals(file.length(), tempFile.length());
    }

    @Test
    public void testShardRecoveryExistingFileSizeMismatch()
            throws Exception
    {
        UUID shardUuid = UUID.randomUUID();

        // write data and backup
        File tempFile = createTempFile("tmp", null, temporary);
        Files.write("test data", tempFile, UTF_8);

        backupStore.backupShard(shardUuid, tempFile);
        assertTrue(backupStore.shardExists(shardUuid));

        File backupFile = backupStore.getBackupFile(shardUuid);
        assertTrue(Files.equal(tempFile, backupFile));

        // write corrupt storage file with wrong length
        File storageFile = new File(storageService.getStorageFile(shardUuid).toString());
        storageService.createParents(new Path(storageFile.toURI()));

        Files.write("bad data", storageFile, UTF_8);

        assertTrue(storageFile.exists());
        assertNotEquals(storageFile.length(), tempFile.length());
        assertFalse(Files.equal(storageFile, tempFile));

        // restore from backup and verify
        recoveryManager.restoreFromBackup(shardUuid, tempFile.length(), OptionalLong.empty());

        assertTrue(storageFile.exists());
        assertTrue(Files.equal(storageFile, tempFile));

        // verify quarantine exists
        List<String> quarantined = listFiles(new File(storageService.getQuarantineFile(shardUuid).getParent().toString()));
        assertEquals(quarantined.size(), 1);
        assertTrue(getOnlyElement(quarantined).startsWith(shardUuid + ".orc.corrupt"));
    }

    @Test
    public void testShardRecoveryExistingFileChecksumMismatch()
            throws Exception
    {
        UUID shardUuid = UUID.randomUUID();

        // write data and backup
        File tempFile = createTempFile("tmp", null, temporary);
        Files.write("test data", tempFile, UTF_8);

        backupStore.backupShard(shardUuid, tempFile);
        assertTrue(backupStore.shardExists(shardUuid));

        File backupFile = backupStore.getBackupFile(shardUuid);
        assertTrue(Files.equal(tempFile, backupFile));

        // write corrupt storage file with wrong data
        File storageFile = new File(storageService.getStorageFile(shardUuid).toString());
        storageService.createParents(new Path(storageFile.toURI()));

        Files.write("test xata", storageFile, UTF_8);

        assertTrue(storageFile.exists());
        assertEquals(storageFile.length(), tempFile.length());
        assertFalse(Files.equal(storageFile, tempFile));

        // restore from backup and verify
        recoveryManager.restoreFromBackup(shardUuid, tempFile.length(), OptionalLong.of(xxhash64(new RaptorLocalFileSystem(new Configuration()), new Path(tempFile.toURI()))));

        assertTrue(storageFile.exists());
        assertTrue(Files.equal(storageFile, tempFile));

        // verify quarantine exists
        List<String> quarantined = listFiles(new File(storageService.getQuarantineFile(shardUuid).getParent().toString()));
        assertEquals(quarantined.size(), 1);
        assertTrue(getOnlyElement(quarantined).startsWith(shardUuid + ".orc.corrupt"));
    }

    @Test
    public void testShardRecoveryBackupChecksumMismatch()
            throws Exception
    {
        UUID shardUuid = UUID.randomUUID();

        // write storage file
        File storageFile = new File(storageService.getStorageFile(shardUuid).toString());
        storageService.createParents(new Path(storageFile.toURI()));

        Files.write("test data", storageFile, UTF_8);

        long size = storageFile.length();
        long xxhash64 = xxhash64(new RaptorLocalFileSystem(new Configuration()), new Path(storageFile.toURI()));

        // backup and verify
        backupStore.backupShard(shardUuid, storageFile);

        assertTrue(backupStore.shardExists(shardUuid));
        File backupFile = backupStore.getBackupFile(shardUuid);
        assertTrue(Files.equal(storageFile, backupFile));

        // corrupt backup file
        Files.write("test xata", backupFile, UTF_8);

        assertTrue(backupFile.exists());
        assertEquals(storageFile.length(), backupFile.length());
        assertFalse(Files.equal(storageFile, backupFile));

        // delete local file to force restore
        assertTrue(storageFile.delete());
        assertFalse(storageFile.exists());

        // restore should fail
        try {
            recoveryManager.restoreFromBackup(shardUuid, size, OptionalLong.of(xxhash64));
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), RAPTOR_BACKUP_CORRUPTION.toErrorCode());
            assertEquals(e.getMessage(), "Backup is corrupt after read: " + shardUuid);
        }

        // verify quarantine exists
        List<String> quarantined = listFiles(new File(storageService.getQuarantineFile(shardUuid).getParent().toString()));
        assertEquals(quarantined.size(), 1);
        assertTrue(getOnlyElement(quarantined).startsWith(shardUuid + ".orc.corrupt"));
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No backup file found for shard: .*")
    public void testNoBackupException()
    {
        recoveryManager.restoreFromBackup(UUID.randomUUID(), 0, OptionalLong.empty());
    }

    public static ShardRecoveryManager createShardRecoveryManager(
            StorageService storageService,
            Optional<BackupStore> backupStore,
            ShardManager shardManager)
    {
        return new ShardRecoveryManager(
                storageService,
                backupStore,
                new LocalOrcDataEnvironment(),
                new TestingNodeManager(),
                shardManager,
                new Duration(5, MINUTES),
                10);
    }

    private static List<String> listFiles(File path)
    {
        String[] files = path.list();
        assertNotNull(files);
        return ImmutableList.copyOf(files);
    }
}
