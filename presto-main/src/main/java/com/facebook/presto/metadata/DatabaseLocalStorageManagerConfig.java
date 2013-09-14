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
package com.facebook.presto.metadata;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.io.File;

public class DatabaseLocalStorageManagerConfig
{
    private File dataDirectory = new File("var/data");
    private int tasksPerNode = 32;

    @NotNull
    public File getDataDirectory()
    {
        return dataDirectory;
    }

    @Config("storage-manager.data-directory")
    @ConfigDescription("Base directory to use for storing shard data")
    public DatabaseLocalStorageManagerConfig setDataDirectory(File dataDirectory)
    {
        this.dataDirectory = dataDirectory;
        return this;
    }

    @Min(1)
    public int getTasksPerNode()
    {
        return tasksPerNode;
    }

    @Config("storage-manager.tasks-per-node")
    @ConfigDescription("Number of background tasks")
    public DatabaseLocalStorageManagerConfig setTasksPerNode(int tasksPerNode)
    {
        this.tasksPerNode = tasksPerNode;
        return this;
    }
}
