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

import javax.inject.Inject;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class StaticConfigurationProvider
        implements OAuth2ServerConfigProvider
{
    private final OAuth2ServerConfig config;

    @Inject
    StaticConfigurationProvider(StaticOAuth2ServerConfiguration config)
    {
        requireNonNull(config, "config is null");
        this.config = new OAuth2ServerConfig(
                config.getAccessTokenIssuer(),
                URI.create(config.getAuthUrl()),
                URI.create(config.getTokenUrl()),
                URI.create(config.getJwksUrl()),
                config.getUserinfoUrl().map(URI::create));
    }

    @Override
    public OAuth2ServerConfig get()
    {
        return config;
    }
}
