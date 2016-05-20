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

package com.facebook.presto.spi.eventlistener;

public class QueryContext
{
    private final String user;
    private final String principal;
    private final String remoteClientAddress;
    private final String userAgent;
    private final String source;

    private final String catalog;
    private final String schema;

    private final String sessionPropertiesJson;

    private final String serverVersion;
    private final String environment;

    public QueryContext(String user, String principal, String remoteClientAddress, String userAgent, String source, String catalog, String schema, String sessionPropertiesJson, String serverVersion, String environment)
    {
        this.user = user;
        this.principal = principal;
        this.remoteClientAddress = remoteClientAddress;
        this.userAgent = userAgent;
        this.source = source;
        this.catalog = catalog;
        this.schema = schema;
        this.sessionPropertiesJson = sessionPropertiesJson;
        this.serverVersion = serverVersion;
        this.environment = environment;
    }

    public String getUser()
    {
        return user;
    }

    public String getPrincipal()
    {
        return principal;
    }

    public String getRemoteClientAddress()
    {
        return remoteClientAddress;
    }

    public String getUserAgent()
    {
        return userAgent;
    }

    public String getSource()
    {
        return source;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getSessionPropertiesJson()
    {
        return sessionPropertiesJson;
    }

    public String getServerVersion()
    {
        return serverVersion;
    }

    public String getEnvironment()
    {
        return environment;
    }
}
