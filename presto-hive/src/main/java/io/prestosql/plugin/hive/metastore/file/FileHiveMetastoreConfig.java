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
package io.prestosql.plugin.hive.metastore.file;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class FileHiveMetastoreConfig
{
    private String catalogDirectory;
    private String metastoreUser = "presto";

    @NotNull
    public String getCatalogDirectory()
    {
        return catalogDirectory;
    }

    @Config("hive.metastore.catalog.dir")
    @ConfigDescription("Hive file-based metastore catalog directory")
    public void setCatalogDirectory(String catalogDirectory)
    {
        this.catalogDirectory = catalogDirectory;
    }

    @NotNull
    public String getMetastoreUser()
    {
        return metastoreUser;
    }

    @Config("hive.metastore.user")
    @ConfigDescription("Hive file-based metastore username for file access")
    public void setMetastoreUser(String metastoreUser)
    {
        this.metastoreUser = metastoreUser;
    }
}
