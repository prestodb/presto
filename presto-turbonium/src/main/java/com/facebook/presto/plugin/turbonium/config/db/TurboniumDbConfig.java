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
package com.facebook.presto.plugin.turbonium.config.db;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class TurboniumDbConfig
{
    private String configUrl = "jdbc:h2:mem:memory_config_db;DB_CLOSE_DELAY=-1";
    private String dbType = "h2";

    @NotNull
    public String getConfigDbUrl()
    {
        return configUrl;
    }

    @Config("config-db-url")
    public TurboniumDbConfig setConfigDbUrl(String configUrl)
    {
        this.configUrl = configUrl;
        return this;
    }

    @NotNull
    public String getConfigDbType()
    {
        return dbType;
    }

    @Config("config-db-type")
    public TurboniumDbConfig setConfigDbType(String dbType)
    {
        this.dbType = dbType;
        return this;
    }
}
