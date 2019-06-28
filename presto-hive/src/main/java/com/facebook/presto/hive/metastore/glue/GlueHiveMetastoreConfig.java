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
package com.facebook.presto.hive.metastore.glue;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Min;

import java.util.Optional;

public class GlueHiveMetastoreConfig
{
    private Optional<String> glueRegion = Optional.empty();
    private boolean pinGlueClientToCurrentRegion;
    private int maxGlueConnections = 5;
    private Optional<String> defaultWarehouseDir = Optional.empty();
    private Optional<String> catalogId = Optional.empty();

    public Optional<String> getGlueRegion()
    {
        return glueRegion;
    }

    @Config("hive.metastore.glue.region")
    @ConfigDescription("AWS Region for Glue Data Catalog")
    public GlueHiveMetastoreConfig setGlueRegion(String region)
    {
        this.glueRegion = Optional.ofNullable(region);
        return this;
    }

    public boolean getPinGlueClientToCurrentRegion()
    {
        return pinGlueClientToCurrentRegion;
    }

    @Config("hive.metastore.glue.pin-client-to-current-region")
    @ConfigDescription("Should the Glue client be pinned to the current EC2 region")
    public GlueHiveMetastoreConfig setPinGlueClientToCurrentRegion(boolean pinGlueClientToCurrentRegion)
    {
        this.pinGlueClientToCurrentRegion = pinGlueClientToCurrentRegion;
        return this;
    }

    @Min(1)
    public int getMaxGlueConnections()
    {
        return maxGlueConnections;
    }

    @Config("hive.metastore.glue.max-connections")
    @ConfigDescription("Max number of concurrent connections to Glue")
    public GlueHiveMetastoreConfig setMaxGlueConnections(int maxGlueConnections)
    {
        this.maxGlueConnections = maxGlueConnections;
        return this;
    }

    public Optional<String> getDefaultWarehouseDir()
    {
        return defaultWarehouseDir;
    }

    @Config("hive.metastore.glue.default-warehouse-dir")
    @ConfigDescription("Hive Glue metastore default warehouse directory")
    public GlueHiveMetastoreConfig setDefaultWarehouseDir(String defaultWarehouseDir)
    {
        this.defaultWarehouseDir = Optional.ofNullable(defaultWarehouseDir);
        return this;
    }

    public Optional<String> getCatalogId()
    {
        return catalogId;
    }

    @Config("hive.metastore.glue.catalogid")
    @ConfigDescription("Hive Glue metastore catalog id")
    public GlueHiveMetastoreConfig setCatalogId(String catalogId)
    {
        this.catalogId = Optional.ofNullable(catalogId);
        return this;
    }
}
