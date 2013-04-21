/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.metadata;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;
import java.io.File;

public class CatalogManagerConfig
{
    private File catalogConfigurationDir = new File("etc/catalog/");

    @NotNull
    public File getCatalogConfigurationDir()
    {
        return catalogConfigurationDir;
    }

    @Config("plugin.config-dir")
    public CatalogManagerConfig setCatalogConfigurationDir(File pluginConfigurationDir)
    {
        this.catalogConfigurationDir = pluginConfigurationDir;
        return this;
    }
}
