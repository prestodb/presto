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
package com.facebook.presto.server;

import io.airlift.configuration.Config;

public class ServerConfig
{
    private boolean coordinator = true;
    private String prestoVersion;
    private String dataSources;

    public boolean isCoordinator()
    {
        return coordinator;
    }

    @Config("coordinator")
    public ServerConfig setCoordinator(boolean coordinator)
    {
        this.coordinator = coordinator;
        return this;
    }

    public String getPrestoVersion()
    {
        return prestoVersion;
    }

    @Config("presto.version")
    public ServerConfig setPrestoVersion(String prestoVersion)
    {
        this.prestoVersion = prestoVersion;
        return this;
    }

    public String getDataSources()
    {
        return dataSources;
    }

    @Config("datasources")
    public ServerConfig setDataSources(String dataSources)
    {
        this.dataSources = dataSources;
        return this;
    }
}
