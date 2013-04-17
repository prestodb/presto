package com.facebook.presto.metadata;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;
import java.io.File;

public class DatabaseLocalStorageManagerConfig
{
    private File dataDirectory = new File("var/data");

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
}
