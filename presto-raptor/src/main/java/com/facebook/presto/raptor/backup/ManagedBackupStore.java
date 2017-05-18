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

import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.File;
import java.util.UUID;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;

public class ManagedBackupStore
        implements BackupStore
{
    private final BackupStore store;
    private final Logger log;

    private final BackupCopyStats backupShard = new BackupCopyStats();
    private final BackupCopyStats restoreShard = new BackupCopyStats();
    private final BackupOperationStats deleteShard = new BackupOperationStats();
    private final BackupOperationStats shardExists = new BackupOperationStats();

    public ManagedBackupStore(BackupStore store)
    {
        this.store = requireNonNull(store, "store is null");
        this.log = Logger.get(store.getClass());
    }

    @Override
    public void backupShard(UUID uuid, File source)
    {
        log.debug("Creating shard backup: %s", uuid);
        backupShard.run(() -> store.backupShard(uuid, source), () -> new DataSize(source.length(), BYTE));
    }

    @Override
    public void restoreShard(UUID uuid, File target)
    {
        log.debug("Restoring shard backup: %s", uuid);
        restoreShard.run(() -> store.restoreShard(uuid, target), () -> new DataSize(target.length(), BYTE));
    }

    @Override
    public boolean deleteShard(UUID uuid)
    {
        log.debug("Deleting shard backup: %s", uuid);
        return deleteShard.run(() -> store.deleteShard(uuid));
    }

    @Override
    public boolean shardExists(UUID uuid)
    {
        return shardExists.run(() -> store.shardExists(uuid));
    }

    @Managed
    @Nested
    public BackupCopyStats getBackupShard()
    {
        return backupShard;
    }

    @Managed
    @Nested
    public BackupCopyStats getRestoreShard()
    {
        return restoreShard;
    }

    @Managed
    @Nested
    public BackupOperationStats getDeleteShard()
    {
        return deleteShard;
    }

    @Managed
    @Nested
    public BackupOperationStats getShardExists()
    {
        return shardExists;
    }
}
