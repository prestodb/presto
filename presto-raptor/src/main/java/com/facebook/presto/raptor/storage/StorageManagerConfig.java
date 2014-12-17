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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.io.File;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class StorageManagerConfig
{
    private File dataDirectory;
    private File backupDirectory;

    private DataSize orcMaxMergeDistance = new DataSize(1, MEGABYTE);

    @NotNull
    public File getDataDirectory()
    {
        return dataDirectory;
    }

    @Config("storage.data-directory")
    @ConfigDescription("Base directory to use for storing shard data")
    public StorageManagerConfig setDataDirectory(File dataDirectory)
    {
        this.dataDirectory = dataDirectory;
        return this;
    }

    @Nullable
    public File getBackupDirectory()
    {
        return backupDirectory;
    }

    @Config("storage.backup-directory")
    @ConfigDescription("Base directory to use for the backup copy of shard data")
    public StorageManagerConfig setBackupDirectory(File backupDirectory)
    {
        this.backupDirectory = backupDirectory;
        return this;
    }

    @NotNull
    public DataSize getOrcMaxMergeDistance()
    {
        return orcMaxMergeDistance;
    }

    @Config("storage.orc.max-merge-distance")
    public StorageManagerConfig setOrcMaxMergeDistance(DataSize orcMaxMergeDistance)
    {
        this.orcMaxMergeDistance = orcMaxMergeDistance;
        return this;
    }
}
