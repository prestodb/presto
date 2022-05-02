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
package com.facebook.presto.iceberg.nessie;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotEmpty;

import java.util.Optional;

public class NessieConfig
{
    private String defaultReferenceName = "main";
    private String serverUri;
    private AuthenticationType authenticationType;
    private String username;
    private String password;
    private String bearerToken;
    private Integer readTimeoutMillis;
    private Integer connectTimeoutMillis;
    private String clientBuilderImpl;
    private boolean compressionEnabled = true;

    @NotEmpty(message = "must not be null or empty")
    public String getDefaultReferenceName()
    {
        return defaultReferenceName;
    }

    @Config("iceberg.nessie.ref")
    @ConfigDescription("The default Nessie reference to work on")
    public NessieConfig setDefaultReferenceName(String defaultReferenceName)
    {
        this.defaultReferenceName = defaultReferenceName;
        return this;
    }

    public Optional<String> getServerUri()
    {
        return Optional.ofNullable(serverUri);
    }

    @Config("iceberg.nessie.uri")
    @ConfigDescription("The URI to connect to the Nessie server")
    public NessieConfig setServerUri(String serverUri)
    {
        this.serverUri = serverUri;
        return this;
    }

    @Config("iceberg.nessie.auth.type")
    @ConfigDescription("The authentication type to use. Available values are BASIC | BEARER")
    public NessieConfig setAuthenticationType(AuthenticationType authenticationType)
    {
        this.authenticationType = authenticationType;
        return this;
    }

    public Optional<AuthenticationType> getAuthenticationType()
    {
        return Optional.ofNullable(authenticationType);
    }

    @Config("iceberg.nessie.auth.basic.username")
    @ConfigDescription("The username to use with BASIC authentication")
    public NessieConfig setUsername(String username)
    {
        this.username = username;
        return this;
    }

    public Optional<String> getUsername()
    {
        return Optional.ofNullable(username);
    }

    @Config("iceberg.nessie.auth.basic.password")
    @ConfigDescription("The password to use with BASIC authentication")
    public NessieConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    public Optional<String> getPassword()
    {
        return Optional.ofNullable(password);
    }

    @Config("iceberg.nessie.auth.bearer.token")
    @ConfigDescription("The token to use with BEARER authentication")
    public NessieConfig setBearerToken(String bearerToken)
    {
        this.bearerToken = bearerToken;
        return this;
    }

    public Optional<String> getBearerToken()
    {
        return Optional.ofNullable(bearerToken);
    }

    @Config("iceberg.nessie.read-timeout-ms")
    @ConfigDescription("The read timeout in milliseconds for the client")
    public NessieConfig setReadTimeoutMillis(Integer readTimeoutMillis)
    {
        this.readTimeoutMillis = readTimeoutMillis;
        return this;
    }

    public Optional<Integer> getReadTimeoutMillis()
    {
        return Optional.ofNullable(readTimeoutMillis);
    }

    @Config("iceberg.nessie.connect-timeout-ms")
    @ConfigDescription("The connection timeout in milliseconds for the client")
    public NessieConfig setConnectTimeoutMillis(Integer connectTimeoutMillis)
    {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    public Optional<Integer> getConnectTimeoutMillis()
    {
        return Optional.ofNullable(connectTimeoutMillis);
    }

    @Config("iceberg.nessie.compression-enabled")
    @ConfigDescription("Configure whether compression should be enabled or not. Default: true")
    public NessieConfig setCompressionEnabled(boolean compressionEnabled)
    {
        this.compressionEnabled = compressionEnabled;
        return this;
    }

    public boolean isCompressionEnabled()
    {
        return compressionEnabled;
    }

    public boolean isCompressionDisabled()
    {
        return !compressionEnabled;
    }

    @Config("iceberg.nessie.client-builder-impl")
    @ConfigDescription("Configure the custom ClientBuilder implementation class to be used")
    public NessieConfig setClientBuilderImpl(String clientBuilderImpl)
    {
        this.clientBuilderImpl = clientBuilderImpl;
        return this;
    }

    public Optional<String> getClientBuilderImpl()
    {
        return Optional.ofNullable(clientBuilderImpl);
    }
}
