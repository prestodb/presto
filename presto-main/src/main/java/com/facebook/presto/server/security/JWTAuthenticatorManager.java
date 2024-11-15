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
import com.facebook.presto.spi.security.JWTAuthenticator;
import com.facebook.presto.spi.security.JWTAuthenticatorFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class JWTAuthenticatorManager
{
    private static final Logger log = Logger.get(JWTAuthenticatorManager.class);

    private static final File CONFIG_FILE = new File("etc/jwt-authenticator.properties");
    private static final String NAME_PROPERTY = "jwt-authenticator.name";

    private final AtomicBoolean required = new AtomicBoolean();
    private final Map<String, JWTAuthenticatorFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<JWTAuthenticator> authenticator = new AtomicReference<>();

    @VisibleForTesting
    public void setAuthenticator(JWTAuthenticator authenticator)
    {
        this.authenticator.set(requireNonNull(authenticator, "authenticator is null"));
    }

    public void setRequired()
    {
        required.set(true);
    }

    public void addJWTAuthenticatorFactory(JWTAuthenticatorFactory factory)
    {
        checkArgument(factories.putIfAbsent(factory.getName(), factory) == null,
                "JWT authenticator '%s' is already registered", factory.getName());
    }

    public void loadJWTAuthenticator()
            throws Exception
    {
        if (!required.get()) {
            return;
        }

        File configFileLocation = CONFIG_FILE.getAbsoluteFile();
        Map<String, String> properties = new HashMap<>(loadProperties(configFileLocation));

        String name = properties.remove(NAME_PROPERTY);
        checkArgument(!isNullOrEmpty(name),
                "JWT authenticator configuration %s does not contain %s", configFileLocation, NAME_PROPERTY);

        log.info("-- Loading JWT authenticator --");

        JWTAuthenticatorFactory factory = factories.get(name);
        checkState(factory != null, "JWT authenticator %s is not registered", name);

        JWTAuthenticator authenticator = factory.create(ImmutableMap.copyOf(properties));
        this.authenticator.set(requireNonNull(authenticator, "authenticator is null"));

        log.info("-- Loaded JWT authenticator %s --", name);
    }

    public JWTAuthenticator getAuthenticator()
    {
        checkState(authenticator.get() != null, "authenticator was not loaded");
        return authenticator.get();
    }
}
