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
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class StaticOAuth2ServerConfiguration
{
    public static final String ACCESS_TOKEN_ISSUER = "http-server.authentication.oauth2.access-token-issuer";
    public static final String AUTH_URL = "http-server.authentication.oauth2.auth-url";
    public static final String TOKEN_URL = "http-server.authentication.oauth2.token-url";
    public static final String JWKS_URL = "http-server.authentication.oauth2.jwks-url";
    public static final String USERINFO_URL = "http-server.authentication.oauth2.userinfo-url";

    private Optional<String> accessTokenIssuer = Optional.empty();
    private String authUrl;
    private String tokenUrl;
    private String jwksUrl;
    private Optional<String> userinfoUrl = Optional.empty();

    @NotNull
    public Optional<String> getAccessTokenIssuer()
    {
        return accessTokenIssuer;
    }

    @Config(ACCESS_TOKEN_ISSUER)
    @ConfigDescription("The required issuer for access tokens")
    public StaticOAuth2ServerConfiguration setAccessTokenIssuer(String accessTokenIssuer)
    {
        this.accessTokenIssuer = Optional.ofNullable(accessTokenIssuer);
        return this;
    }

    @NotNull
    public String getAuthUrl()
    {
        return authUrl;
    }

    @Config(AUTH_URL)
    @ConfigDescription("URL of the authorization server's authorization endpoint")
    public StaticOAuth2ServerConfiguration setAuthUrl(String authUrl)
    {
        this.authUrl = authUrl;
        return this;
    }

    @NotNull
    public String getTokenUrl()
    {
        return tokenUrl;
    }

    @Config(TOKEN_URL)
    @ConfigDescription("URL of the authorization server's token endpoint")
    public StaticOAuth2ServerConfiguration setTokenUrl(String tokenUrl)
    {
        this.tokenUrl = tokenUrl;
        return this;
    }

    @NotNull
    public String getJwksUrl()
    {
        return jwksUrl;
    }

    @Config(JWKS_URL)
    @ConfigDescription("URL of the authorization server's JWKS (JSON Web Key Set) endpoint")
    public StaticOAuth2ServerConfiguration setJwksUrl(String jwksUrl)
    {
        this.jwksUrl = jwksUrl;
        return this;
    }

    public Optional<String> getUserinfoUrl()
    {
        return userinfoUrl;
    }

    @Config(USERINFO_URL)
    @ConfigDescription("URL of the userinfo endpoint")
    public StaticOAuth2ServerConfiguration setUserinfoUrl(String userinfoUrl)
    {
        this.userinfoUrl = Optional.ofNullable(userinfoUrl);
        return this;
    }
}
