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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.client.NodeVersion;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.nimbusds.jose.KeyLengthException;

import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;

public class JweTokenSerializerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(RefreshTokensConfig.class);
        RefreshTokensConfig config = buildConfigObject(RefreshTokensConfig.class);
        newOptionalBinder(binder, Key.get(Duration.class, ForRefreshTokens.class)).setBinding().toInstance(Duration.ofMillis(config.getTokenExpiration().toMillis()));
    }

    @Provides
    @Singleton
    @Inject
    public TokenPairSerializer getTokenPairSerializer(
            OAuth2Client client,
            NodeVersion nodeVersion,
            RefreshTokensConfig config,
            OAuth2Config oAuth2Config)
            throws KeyLengthException, NoSuchAlgorithmException
    {
        return new JweTokenSerializer(
                config,
                client,
                config.getIssuer() + "_" + nodeVersion.getVersion(),
                config.getAudience(),
                oAuth2Config.getPrincipalField(),
                Clock.systemUTC(),
                config.getTokenExpiration());
    }
}
