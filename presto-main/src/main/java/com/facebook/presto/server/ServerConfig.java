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

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ServerConfig
{
    private boolean resourceManager;
    private boolean resourceManagerEnabled;
    private boolean coordinator = true;
    private String prestoVersion = getClass().getPackage().getImplementationVersion();
    private String dataSources;
    private boolean includeExceptionInResponse = true;
    private Duration gracePeriod = new Duration(2, MINUTES);
    private boolean enhancedErrorReporting = true;
    private boolean queryResultsCompressionEnabled = true;

    public boolean isResourceManager()
    {
        return resourceManager;
    }

    @Config("resource-manager")
    public ServerConfig setResourceManager(boolean resourceManager)
    {
        this.resourceManager = resourceManager;
        return this;
    }

    public boolean isResourceManagerEnabled()
    {
        return resourceManagerEnabled;
    }

    @Config("resource-manager-enabled")
    public ServerConfig setResourceManagerEnabled(boolean resourceManagerEnabled)
    {
        this.resourceManagerEnabled = resourceManagerEnabled;
        return this;
    }

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

    @NotNull(message = "presto.version must be provided when it cannot be automatically determined")
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

    @Deprecated
    public String getDataSources()
    {
        return dataSources;
    }

    @Deprecated
    @Config("datasources")
    public ServerConfig setDataSources(String dataSources)
    {
        this.dataSources = dataSources;
        return this;
    }

    public boolean isIncludeExceptionInResponse()
    {
        return includeExceptionInResponse;
    }

    @Config("http.include-exception-in-response")
    public ServerConfig setIncludeExceptionInResponse(boolean includeExceptionInResponse)
    {
        this.includeExceptionInResponse = includeExceptionInResponse;
        return this;
    }

    public Duration getGracePeriod()
    {
        return gracePeriod;
    }

    @Config("shutdown.grace-period")
    public ServerConfig setGracePeriod(Duration gracePeriod)
    {
        this.gracePeriod = gracePeriod;
        return this;
    }

    public boolean isEnhancedErrorReporting()
    {
        return enhancedErrorReporting;
    }

    // TODO: temporary kill switch until we're confident the new error handling logic is
    // solid. Placed here for convenience and to avoid creating a new set of throwaway config objects
    // and because the parser is instantiated in the module that wires up the server (ServerMainModule)
    @Config("sql.parser.enhanced-error-reporting")
    public ServerConfig setEnhancedErrorReporting(boolean value)
    {
        this.enhancedErrorReporting = value;
        return this;
    }

    public boolean isQueryResultsCompressionEnabled()
    {
        return queryResultsCompressionEnabled;
    }

    @Config("query-results.compression-enabled")
    public ServerConfig setQueryResultsCompressionEnabled(boolean queryResultsCompressionEnabled)
    {
        this.queryResultsCompressionEnabled = queryResultsCompressionEnabled;
        return this;
    }
}
