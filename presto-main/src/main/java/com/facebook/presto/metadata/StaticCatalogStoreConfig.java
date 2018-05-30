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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.List;

public class StaticCatalogStoreConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private File catalogConfigurationDir = new File("etc/catalog/");
    private List<String> disabledCatalogs;

    @NotNull
    public File getCatalogConfigurationDir()
    {
        return catalogConfigurationDir;
    }

    @LegacyConfig("plugin.config-dir")
    @Config("catalog.config-dir")
    public StaticCatalogStoreConfig setCatalogConfigurationDir(File dir)
    {
        this.catalogConfigurationDir = dir;
        return this;
    }

    public List<String> getDisabledCatalogs()
    {
        return disabledCatalogs;
    }

    @Config("catalog.disabled-catalogs")
    public StaticCatalogStoreConfig setDisabledCatalogs(String catalogs)
    {
        this.disabledCatalogs = (catalogs == null) ? null : SPLITTER.splitToList(catalogs);
        return this;
    }

    public StaticCatalogStoreConfig setDisabledCatalogs(List<String> catalogs)
    {
        this.disabledCatalogs = (catalogs == null) ? null : ImmutableList.copyOf(catalogs);
        return this;
    }
}
