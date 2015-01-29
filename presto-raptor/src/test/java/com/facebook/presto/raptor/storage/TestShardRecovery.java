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

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.raptor.metadata.DatabaseShardManager;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestShardRecovery
{
    private StorageService storageService;
    private ShardRecoveryManager recoveryManager;
    private Handle dummyHandle;
    private File temporary;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        temporary = createTempDir();
        File directory = new File(temporary, "data");
        File backupDirectory = new File(temporary, "backup");
        storageService = new FileStorageService(directory, Optional.of(backupDirectory));
        storageService.start();

        IDBI dbi = new DBI("jdbc:h2:mem:test" + System.nanoTime());
        dummyHandle = dbi.open();
        ShardManager shardManager = new DatabaseShardManager(dbi);
        recoveryManager = new ShardRecoveryManager(storageService, new InMemoryNodeManager(), shardManager, new Duration(5, TimeUnit.MINUTES), 10);
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
        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, VARCHAR);

        UUID shardUuid = UUID.randomUUID();
        File file = storageService.getStorageFile(shardUuid);
        File backupFile = storageService.getBackupFile(shardUuid);

        try (OrcFileWriter writer = new OrcFileWriter(columnIds, columnTypes, backupFile)) {
            // create file with zero rows
        }

        assertTrue(backupFile.exists());
        assertFalse(file.exists());
        recoveryManager.restoreFromBackup(shardUuid);
        assertTrue(backupFile.exists());
        assertTrue(file.exists());
        assertEquals(file.length(), backupFile.length());
    }

    @SuppressWarnings("EmptyTryBlock")
    @Test
    public void testShardRecoveryExistingFileMismatch()
            throws Exception
    {
        List<Long> columnIds = ImmutableList.of(3L, 7L);
        List<Type> columnTypes = ImmutableList.of(BIGINT, VARCHAR);

        UUID shardUuid = UUID.randomUUID();
        File file = storageService.getStorageFile(shardUuid);
        File backupFile = storageService.getBackupFile(shardUuid);

        try (OrcFileWriter writer = new OrcFileWriter(columnIds, columnTypes, backupFile)) {
            List<Page> pages = RowPagesBuilder.rowPagesBuilder(columnTypes)
                    .row(123, "hello")
                    .row(456, "bye")
                    .build();

            writer.appendPages(pages);
        }

        try (OrcFileWriter writer = new OrcFileWriter(columnIds, columnTypes, file)) {
            // create file with zero rows
        }

        assertTrue(backupFile.exists());
        assertTrue(file.exists());
        recoveryManager.restoreFromBackup(shardUuid);
        assertTrue(backupFile.exists());
        assertTrue(file.exists());
        assertTrue(file.length() == backupFile.length());
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "No backup file found for shard: .*")
    public void testNoBackupException()
            throws Exception
    {
        UUID shardUuid = UUID.randomUUID();
        File file = storageService.getStorageFile(shardUuid);
        File backupFile = storageService.getBackupFile(shardUuid);

        assertFalse(backupFile.exists());
        recoveryManager.restoreFromBackup(shardUuid);
    }
}
