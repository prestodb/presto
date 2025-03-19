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
package com.facebook.presto.server.security;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.security.PrestoAuthenticator;
import com.facebook.presto.spi.security.PrestoAuthenticatorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class PrestoAuthenticatorManager
{
    private static final Logger log = Logger.get(PrestoAuthenticatorManager.class);

    private static final File CONFIG_FILE = new File("etc/presto-authenticator.properties");
    private static final String NAME_PROPERTY = "presto-authenticator.name";

    private final Map<String, PrestoAuthenticatorFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<PrestoAuthenticator> authenticator = new AtomicReference<>();
    private final boolean customAuthenticatorRequested;

    @Inject
    public PrestoAuthenticatorManager(SecurityConfig securityConfig)
    {
        this.customAuthenticatorRequested = securityConfig.getAuthenticationTypes().contains(SecurityConfig.AuthenticationType.CUSTOM);
    }

    public void addPrestoAuthenticatorFactory(PrestoAuthenticatorFactory factory)
    {
        checkArgument(factories.putIfAbsent(factory.getName(), factory) == null,
                "Presto authenticator '%s' is already registered", factory.getName());
    }

    @VisibleForTesting
    public void loadAuthenticator(String authenticatorName)
    {
        PrestoAuthenticatorFactory factory = factories.get(authenticatorName);

        PrestoAuthenticator authenticator = factory.create(ImmutableMap.of());
        this.authenticator.set(requireNonNull(authenticator, "authenticator is null"));
    }

    public void loadPrestoAuthenticator()
            throws Exception
    {
        if (!customAuthenticatorRequested) {
            return;
        }

        File configFileLocation = CONFIG_FILE.getAbsoluteFile();
        Map<String, String> properties = new HashMap<>(loadProperties(configFileLocation));

        String name = properties.remove(NAME_PROPERTY);
        checkArgument(!isNullOrEmpty(name),
                "Presto authenticator configuration %s does not contain %s", configFileLocation, NAME_PROPERTY);

        log.info("-- Loading Presto authenticator --");

        PrestoAuthenticatorFactory factory = factories.get(name);
        checkState(factory != null, "Presto authenticator %s is not registered", name);

        PrestoAuthenticator authenticator = factory.create(ImmutableMap.copyOf(properties));
        this.authenticator.set(requireNonNull(authenticator, "authenticator is null"));

        log.info("-- Loaded Presto authenticator %s --", name);
    }

    public PrestoAuthenticator getAuthenticator()
    {
        checkState(authenticator.get() != null, "authenticator was not loaded");
        return authenticator.get();
    }
}
