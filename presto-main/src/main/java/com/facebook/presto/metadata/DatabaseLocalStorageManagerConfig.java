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
