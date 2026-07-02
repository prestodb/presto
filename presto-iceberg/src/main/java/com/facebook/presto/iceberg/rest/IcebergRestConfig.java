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
import com.facebook.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class IcebergRestConfig
{
    private String serverUri;
    private SessionType sessionType;
    private AuthenticationType authenticationType;
    private String authenticationServerUri;
    private String credential;
    private String token;
    private String scope;
    private boolean nestedNamespaceEnabled = true;
    private String basicAuthUsername;
    private String basicAuthPassword;
    private boolean tlsEnabled;
    private String keystorePath;
    private String keystorePassword;
    private String truststorePath;
    private String truststorePassword;

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
    @ConfigDescription("The authentication type to use for communicating with REST catalog server (NONE | OAUTH2 | BASIC)")
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

    public Optional<String> getScope()
    {
        return Optional.ofNullable(scope);
    }

    @Config("iceberg.rest.auth.oauth2.scope")
    @ConfigDescription("The scope to use for OAUTH2 authentication")
    public IcebergRestConfig setScope(String scope)
    {
        this.scope = scope;
        return this;
    }

    public boolean isNestedNamespaceEnabled()
    {
        return nestedNamespaceEnabled;
    }

    @Config("iceberg.rest.nested.namespace.enabled")
    @ConfigDescription("Allows querying nested namespaces. Default: true")
    public IcebergRestConfig setNestedNamespaceEnabled(boolean nestedNamespaceEnabled)
    {
        this.nestedNamespaceEnabled = nestedNamespaceEnabled;
        return this;
    }

    public Optional<String> getBasicAuthUsername()
    {
        return Optional.ofNullable(basicAuthUsername);
    }

    @Config("iceberg.rest.auth.basic.username")
    @ConfigDescription("Username for Basic Auth against the REST catalog server; requires iceberg.rest.auth.basic.password")
    public IcebergRestConfig setBasicAuthUsername(String basicAuthUsername)
    {
        this.basicAuthUsername = basicAuthUsername;
        return this;
    }

    public Optional<String> getBasicAuthPassword()
    {
        return Optional.ofNullable(basicAuthPassword);
    }

    @Config("iceberg.rest.auth.basic.password")
    @ConfigDescription("Password for Basic Auth against the REST catalog server; requires iceberg.rest.auth.basic.username")
    @ConfigSecuritySensitive
    public IcebergRestConfig setBasicAuthPassword(String basicAuthPassword)
    {
        this.basicAuthPassword = basicAuthPassword;
        return this;
    }

    public Optional<String> getKeystorePath()
    {
        return Optional.ofNullable(keystorePath);
    }

    @Config("iceberg.rest.tls.keystore-path")
    @ConfigDescription("The path to the keystore file for REST catalog TLS communication (mutual TLS)")
    public IcebergRestConfig setKeystorePath(String keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public Optional<String> getKeystorePassword()
    {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("iceberg.rest.tls.keystore-password")
    @ConfigDescription("The password for the keystore file for REST catalog TLS communication")
    @ConfigSecuritySensitive
    public IcebergRestConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public boolean isTlsEnabled()
    {
        return tlsEnabled;
    }

    @Config("iceberg.rest.tls.enabled")
    @ConfigDescription("Whether to enable TLS for REST catalog communication")
    public IcebergRestConfig setTlsEnabled(boolean tlsEnabled)
    {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    public Optional<String> getTruststorePath()
    {
        return Optional.ofNullable(truststorePath);
    }

    @Config("iceberg.rest.tls.truststore-path")
    @ConfigDescription("The path to the truststore file for REST catalog TLS communication")
    public IcebergRestConfig setTruststorePath(String truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("iceberg.rest.tls.truststore-password")
    @ConfigDescription("The password for the truststore file for REST catalog TLS communication")
    @ConfigSecuritySensitive
    public IcebergRestConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

    @AssertTrue(message = "iceberg.rest.tls.keystore-path and iceberg.rest.tls.keystore-password must be set together; " +
            "iceberg.rest.tls.truststore-path and iceberg.rest.tls.truststore-password must be set together; " +
            "at least one of keystore or truststore must be configured when TLS is enabled")
    public boolean isValidTlsConfig()
    {
        if (!tlsEnabled) {
            return true;
        }
        boolean validKeystore = (keystorePath == null && keystorePassword == null) ||
                (keystorePath != null && keystorePassword != null);
        boolean validTruststore = (truststorePath == null && truststorePassword == null) ||
                (truststorePath != null && truststorePassword != null);
        boolean atLeastOneStoreConfigured = keystorePath != null || truststorePath != null;
        return validKeystore && validTruststore && atLeastOneStoreConfigured;
    }

    public boolean credentialOrTokenExists()
    {
        return credential != null || token != null;
    }
}
