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
package io.prestosql.plugin.raptor.legacy.backup;

import com.google.common.io.Files;
import io.prestosql.plugin.raptor.legacy.storage.BackupStats;
import io.prestosql.plugin.raptor.legacy.storage.FileStorageService;
import io.prestosql.spi.PrestoException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_BACKUP_CORRUPTION;
import static io.prestosql.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_BACKUP_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.FileAssert.assertFile;

@Test(singleThreaded = true)
public class TestBackupManager
{
    private static final UUID FAILURE_UUID = randomUUID();
    private static final UUID CORRUPTION_UUID = randomUUID();

    private File temporary;
    private BackupStore backupStore;
    private FileStorageService storageService;
    private BackupManager backupManager;

    @BeforeMethod
    public void setup()
    {
        temporary = createTempDir();

        FileBackupStore fileStore = new FileBackupStore(new File(temporary, "backup"));
        fileStore.start();
        backupStore = new TestingBackupStore(fileStore);

        storageService = new FileStorageService(new File(temporary, "data"));
        storageService.start();

        backupManager = new BackupManager(Optional.of(backupStore), storageService, 5);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary.toPath(), ALLOW_INSECURE);
        backupManager.shutdown();
    }

    @Test
    public void testSimple()
            throws Exception
    {
        assertEmptyStagingDirectory();
        assertBackupStats(0, 0, 0);

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
            assertTrue(backupStore.shardExists(uuid));
        }

        assertBackupStats(5, 0, 0);
        assertEmptyStagingDirectory();
    }

    @Test
    public void testFailure()
            throws Exception
    {
        assertEmptyStagingDirectory();
        assertBackupStats(0, 0, 0);

        File file = new File(temporary, "failure");
        Files.write("hello world", file, UTF_8);

        try {
            backupManager.submit(FAILURE_UUID, file).get(1, SECONDS);
            fail("expected exception");
        }
        catch (ExecutionException wrapper) {
            PrestoException e = (PrestoException) wrapper.getCause();
            assertEquals(e.getErrorCode(), RAPTOR_BACKUP_ERROR.toErrorCode());
            assertEquals(e.getMessage(), "Backup failed for testing");
        }

        assertBackupStats(0, 1, 0);
        assertEmptyStagingDirectory();
    }

    @Test
    public void testCorruption()
            throws Exception
    {
        assertEmptyStagingDirectory();
        assertBackupStats(0, 0, 0);

        File file = new File(temporary, "corrupt");
        Files.write("hello world", file, UTF_8);

        try {
            backupManager.submit(CORRUPTION_UUID, file).get(1, SECONDS);
            fail("expected exception");
        }
        catch (ExecutionException wrapper) {
            PrestoException e = (PrestoException) wrapper.getCause();
            assertEquals(e.getErrorCode(), RAPTOR_BACKUP_CORRUPTION.toErrorCode());
            assertEquals(e.getMessage(), "Backup is corrupt after write: " + CORRUPTION_UUID);
        }

        File quarantineBase = storageService.getQuarantineFile(CORRUPTION_UUID);
        assertFile(new File(quarantineBase.getPath() + ".original"));
        assertFile(new File(quarantineBase.getPath() + ".restored"));

        assertBackupStats(0, 1, 1);
        assertEmptyStagingDirectory();
    }

    private void assertEmptyStagingDirectory()
    {
        File staging = storageService.getStagingFile(randomUUID()).getParentFile();
        assertEquals(staging.list(), new String[] {});
    }

    private void assertBackupStats(int successCount, int failureCount, int corruptionCount)
    {
        BackupStats stats = backupManager.getStats();
        assertEquals(stats.getBackupSuccess().getTotalCount(), successCount);
        assertEquals(stats.getBackupFailure().getTotalCount(), failureCount);
        assertEquals(stats.getBackupCorruption().getTotalCount(), corruptionCount);
    }

    private static class TestingBackupStore
            implements BackupStore
    {
        private final BackupStore delegate;

        private TestingBackupStore(BackupStore delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void backupShard(UUID uuid, File source)
        {
            if (uuid.equals(FAILURE_UUID)) {
                throw new PrestoException(RAPTOR_BACKUP_ERROR, "Backup failed for testing");
            }
            delegate.backupShard(uuid, source);
        }

        @Override
        public void restoreShard(UUID uuid, File target)
        {
            delegate.restoreShard(uuid, target);
            if (uuid.equals(CORRUPTION_UUID)) {
                corruptFile(target);
            }
        }

        @Override
        public boolean deleteShard(UUID uuid)
        {
            return delegate.deleteShard(uuid);
        }

        @Override
        public boolean shardExists(UUID uuid)
        {
            return delegate.shardExists(uuid);
        }

        private static void corruptFile(File path)
        {
            // flip a bit at a random offset
            try (RandomAccessFile file = new RandomAccessFile(path, "rw")) {
                if (file.length() == 0) {
                    throw new RuntimeException("file is empty");
                }
                long offset = ThreadLocalRandom.current().nextLong(file.length());
                file.seek(offset);
                int value = file.read() ^ 0x01;
                file.seek(offset);
                file.write(value);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
