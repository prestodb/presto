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
package com.facebook.presto.ducklake;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;

public class DuckLakeCatalogConfig
{
    private String connectionUrl;
    private String connectionUser;
    private String connectionPassword;
    private String schema = "public";

    @NotNull
    public String getConnectionUrl()
    {
        return connectionUrl;
    }

    @Config("ducklake.catalog.connection-url")
    @ConfigDescription("JDBC connection URL for the database that stores the DuckLake catalog")
    public DuckLakeCatalogConfig setConnectionUrl(String connectionUrl)
    {
        this.connectionUrl = connectionUrl;
        return this;
    }

    @Nullable
    public String getConnectionUser()
    {
        return connectionUser;
    }

    @Config("ducklake.catalog.connection-user")
    @ConfigDescription("Username for the DuckLake catalog database connection")
    public DuckLakeCatalogConfig setConnectionUser(String connectionUser)
    {
        this.connectionUser = connectionUser;
        return this;
    }

    @Nullable
    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    @Config("ducklake.catalog.connection-password")
    @ConfigDescription("Password for the DuckLake catalog database connection")
    @ConfigSecuritySensitive
    public DuckLakeCatalogConfig setConnectionPassword(String connectionPassword)
    {
        this.connectionPassword = connectionPassword;
        return this;
    }

    @NotNull
    public String getSchema()
    {
        return schema;
    }

    @Config("ducklake.catalog.schema")
    @ConfigDescription("Schema in the catalog database that stores the DuckLake metadata tables")
    public DuckLakeCatalogConfig setSchema(String schema)
    {
        this.schema = schema;
        return this;
    }
}
