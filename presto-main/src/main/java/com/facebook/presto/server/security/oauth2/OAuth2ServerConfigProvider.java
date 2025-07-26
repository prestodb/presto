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

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface OAuth2ServerConfigProvider
{
    OAuth2ServerConfig get();

    class OAuth2ServerConfig
    {
        private final Optional<String> accessTokenIssuer;
        private final URI authUrl;
        private final URI tokenUrl;
        private final URI jwksUrl;
        private final Optional<URI> userinfoUrl;

        public OAuth2ServerConfig(Optional<String> accessTokenIssuer, URI authUrl, URI tokenUrl, URI jwksUrl, Optional<URI> userinfoUrl)
        {
            this.accessTokenIssuer = requireNonNull(accessTokenIssuer, "accessTokenIssuer is null");
            this.authUrl = requireNonNull(authUrl, "authUrl is null");
            this.tokenUrl = requireNonNull(tokenUrl, "tokenUrl is null");
            this.jwksUrl = requireNonNull(jwksUrl, "jwksUrl is null");
            this.userinfoUrl = requireNonNull(userinfoUrl, "userinfoUrl is null");
        }

        public Optional<String> getAccessTokenIssuer()
        {
            return accessTokenIssuer;
        }

        public URI getAuthUrl()
        {
            return authUrl;
        }

        public URI getTokenUrl()
        {
            return tokenUrl;
        }

        public URI getJwksUrl()
        {
            return jwksUrl;
        }

        public Optional<URI> getUserinfoUrl()
        {
            return userinfoUrl;
        }
    }
}
