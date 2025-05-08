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
package com.facebook.presto.server.security.oauth2;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.NotNull;

import java.util.Optional;

import static com.facebook.presto.server.security.oauth2.StaticOAuth2ServerConfiguration.ACCESS_TOKEN_ISSUER;
import static com.facebook.presto.server.security.oauth2.StaticOAuth2ServerConfiguration.AUTH_URL;
import static com.facebook.presto.server.security.oauth2.StaticOAuth2ServerConfiguration.JWKS_URL;
import static com.facebook.presto.server.security.oauth2.StaticOAuth2ServerConfiguration.TOKEN_URL;
import static com.facebook.presto.server.security.oauth2.StaticOAuth2ServerConfiguration.USERINFO_URL;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OidcDiscoveryConfig
{
    private Duration discoveryTimeout = new Duration(30, SECONDS);
    private boolean userinfoEndpointEnabled = true;

    //TODO Left for backward compatibility, remove after the next release/a couple of releases
    private Optional<String> accessTokenIssuer = Optional.empty();
    private Optional<String> authUrl = Optional.empty();
    private Optional<String> tokenUrl = Optional.empty();
    private Optional<String> jwksUrl = Optional.empty();
    private Optional<String> userinfoUrl = Optional.empty();

    @NotNull
    public Duration getDiscoveryTimeout()
    {
        return discoveryTimeout;
    }

    @Config("http-server.authentication.oauth2.oidc.discovery.timeout")
    @ConfigDescription("OpenID Connect discovery timeout")
    public OidcDiscoveryConfig setDiscoveryTimeout(Duration discoveryTimeout)
    {
        this.discoveryTimeout = discoveryTimeout;
        return this;
    }

    public boolean isUserinfoEndpointEnabled()
    {
        return userinfoEndpointEnabled;
    }

    @Config("http-server.authentication.oauth2.oidc.use-userinfo-endpoint")
    @ConfigDescription("Use userinfo endpoint from OpenID connect metadata document")
    public OidcDiscoveryConfig setUserinfoEndpointEnabled(boolean userinfoEndpointEnabled)
    {
        this.userinfoEndpointEnabled = userinfoEndpointEnabled;
        return this;
    }

    @NotNull
    @Deprecated
    public Optional<String> getAccessTokenIssuer()
    {
        return accessTokenIssuer;
    }

    @Config(ACCESS_TOKEN_ISSUER)
    @ConfigDescription("The required issuer for access tokens")
    @Deprecated
    public OidcDiscoveryConfig setAccessTokenIssuer(String accessTokenIssuer)
    {
        this.accessTokenIssuer = Optional.ofNullable(accessTokenIssuer);
        return this;
    }

    @NotNull
    @Deprecated
    public Optional<String> getAuthUrl()
    {
        return authUrl;
    }

    @Config(AUTH_URL)
    @ConfigDescription("URL of the authorization server's authorization endpoint")
    @Deprecated
    public OidcDiscoveryConfig setAuthUrl(String authUrl)
    {
        this.authUrl = Optional.ofNullable(authUrl);
        return this;
    }

    @NotNull
    @Deprecated
    public Optional<String> getTokenUrl()
    {
        return tokenUrl;
    }

    @Config(TOKEN_URL)
    @ConfigDescription("URL of the authorization server's token endpoint")
    @Deprecated
    public OidcDiscoveryConfig setTokenUrl(String tokenUrl)
    {
        this.tokenUrl = Optional.ofNullable(tokenUrl);
        return this;
    }

    @NotNull
    @Deprecated
    public Optional<String> getJwksUrl()
    {
        return jwksUrl;
    }

    @Config(JWKS_URL)
    @ConfigDescription("URL of the authorization server's JWKS (JSON Web Key Set) endpoint")
    @Deprecated
    public OidcDiscoveryConfig setJwksUrl(String jwksUrl)
    {
        this.jwksUrl = Optional.ofNullable(jwksUrl);
        return this;
    }

    @NotNull
    @Deprecated
    public Optional<String> getUserinfoUrl()
    {
        return userinfoUrl;
    }

    @Config(USERINFO_URL)
    @ConfigDescription("URL of the userinfo endpoint")
    @Deprecated
    public OidcDiscoveryConfig setUserinfoUrl(String userinfoUrl)
    {
        this.userinfoUrl = Optional.ofNullable(userinfoUrl);
        return this;
    }
}
