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

import java.io.File;
import java.util.OptionalLong;
import java.util.UUID;

public interface BackupStore
{
    /**
     * Backup a shard by copying it to the backup store.
     *
     * @param uuid shard UUID
     * @param source the source file
     */
    void backupShard(UUID uuid, File source);

    /**
     * Restore a shard by copying it from the backup store.
     *
     * @param uuid shard UUID
     * @param target the destination file
     */
    void restoreShard(UUID uuid, File target);

    /**
     * Get the size of a shard in the backup store, if it exists.
     * This method can be used to check for size and/or existence.
     *
     * @param uuid shard UUID
     * @return the length of the shard in bytes
     */
    OptionalLong shardSize(UUID uuid);
}
