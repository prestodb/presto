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
package com.facebook.presto.mcp;

import com.facebook.airlift.configuration.Config;

import java.net.URI;

public class McpServerConfig
{
    private URI prestoUri;
    private int defaultLimit = 10;
    private String prestoUser = "mcp-user";
    private String prestoPassword;
    private String prestoToken;
    private boolean sslVerificationDisabled;

    @Config("presto.server.uri")
    public McpServerConfig setPrestoUri(URI uri)
    {
        this.prestoUri = uri;
        return this;
    }

    public URI getPrestoUri()
    {
        return prestoUri;
    }

    @Config("mcp.default-limit")
    public McpServerConfig setDefaultLimit(int limit)
    {
        this.defaultLimit = limit;
        return this;
    }

    public int getDefaultLimit()
    {
        return defaultLimit;
    }

    @Config("presto.server.user")
    public McpServerConfig setPrestoUser(String user)
    {
        this.prestoUser = user;
        return this;
    }

    public String getPrestoUser()
    {
        return prestoUser;
    }

    @Config("presto.server.password")
    public McpServerConfig setPrestoPassword(String password)
    {
        this.prestoPassword = password;
        return this;
    }

    public String getPrestoPassword()
    {
        return prestoPassword;
    }

    @Config("presto.server.token")
    public McpServerConfig setPrestoToken(String token)
    {
        this.prestoToken = token;
        return this;
    }

    public String getPrestoToken()
    {
        return prestoToken;
    }

    @Config("presto.server.ssl.verification-disabled")
    public McpServerConfig setSslVerificationDisabled(boolean disabled)
    {
        this.sslVerificationDisabled = disabled;
        return this;
    }

    public boolean isSslVerificationDisabled()
    {
        return sslVerificationDisabled;
    }
}
