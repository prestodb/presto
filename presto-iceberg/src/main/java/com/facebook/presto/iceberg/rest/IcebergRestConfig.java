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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import java.util.Optional;

public class IcebergRestConfig
{
    private String serverUri;
    private SessionType sessionType;
    private AuthenticationType authenticationType;
    private String authenticationServerUri;
    private String credential;
    private String token;

    @NotNull
    public Optional<String> getServerUri()
    {
        return Optional.ofNullable(serverUri);
    }

    @Config("iceberg.rest.uri")
    @ConfigDescription("The URI to connect to the REST server")
    public IcebergRestConfig setServerUri(String serverUri)
    {
        this.serverUri = serverUri;
        return this;
    }

    public Optional<SessionType> getSessionType()
    {
        return Optional.ofNullable(sessionType);
    }

    @Config("iceberg.rest.session.type")
    @ConfigDescription("The session type to use for communicating with REST catalog server (NONE | USER)")
    public IcebergRestConfig setSessionType(SessionType sessionType)
    {
        this.sessionType = sessionType;
        return this;
    }

    public Optional<AuthenticationType> getAuthenticationType()
    {
        return Optional.ofNullable(authenticationType);
    }

    @Config("iceberg.rest.auth.type")
    @ConfigDescription("The authentication type to use for communicating with REST catalog server (NONE | OAUTH2)")
    public IcebergRestConfig setAuthenticationType(AuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }

    public Optional<String> getAuthenticationServerUri()
    {
        return Optional.ofNullable(authenticationServerUri);
    }

    @Config("iceberg.rest.auth.oauth2.uri")
    @ConfigDescription("The URI to connect to the OAUTH2 server")
    public IcebergRestConfig setAuthenticationServerUri(String authServerUri)
    {
        this.authenticationServerUri = authServerUri;
        return this;
    }

    public Optional<String> getCredential()
    {
        return Optional.ofNullable(credential);
    }

    @Config("iceberg.rest.auth.oauth2.credential")
    @ConfigDescription("The credential to use for OAUTH2 authentication")
    public IcebergRestConfig setCredential(String credential)
    {
        this.credential = credential;
        return this;
    }

    public Optional<String> getToken()
    {
        return Optional.ofNullable(token);
    }

    @Config("iceberg.rest.auth.oauth2.token")
    @ConfigDescription("The Bearer token to use for OAUTH2 authentication")
    public IcebergRestConfig setToken(String token)
    {
        this.token = token;
        return this;
    }

    public boolean credentialOrTokenExists()
    {
        return credential != null || token != null;
    }
}
