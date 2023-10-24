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
package com.facebook.presto.secretsManager;

import com.facebook.presto.spi.secretsManager.SecretsManagerFactory;
import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SecretsManagerHandler
{
    private final Map<String, SecretsManagerFactory> secretsManagerFactories = new ConcurrentHashMap<>();

    private static final File SECRETS_MANAGER_CONFIGURATION = new File("etc/secrets-manager.properties");

    private static final String SECRETS_MANAGER_PROPERTY_NAME = "secrets-manager.name";

    private static final Map<String, String> secretsManagerProperties = new ConcurrentHashMap<>();

    /**
     * Add the secretsmanagerfactory object from plugin to the map
     * @param secretsManagerFactory
     */
    public void addSecretsManagerFactory(SecretsManagerFactory secretsManagerFactory)
    {
        requireNonNull(secretsManagerFactory, "secretsManagerFactory is null");

        if (secretsManagerFactories.putIfAbsent(secretsManagerFactory.getName(), secretsManagerFactory) != null) {
            throw new IllegalArgumentException(format("SecretsManager implementation '%s' is already registered", secretsManagerFactory.getName()));
        }
    }

    /**
     * Creates an instance of configured secrets manager implementation and invokes the fetchSecrets()
     * @param secretsManagerName
     * @return Map of secretsId & secrets
     * @throws IOException
     */

    public Map<String, Map<String, String>> loadSecrets(String secretsManagerName) throws IOException
    {
        requireNonNull(secretsManagerName, "secretsManagerName is null");
        SecretsManagerFactory secretsManagerFactory = secretsManagerFactories.get(secretsManagerName);
        checkArgument(secretsManagerFactory != null, "No factory for SecretsManager %s", secretsManagerName);
        return secretsManagerFactory.create().fetchSecrets();
    }

    /**
     * Loads the configuration for secrets manager
     * @throws IOException
     */
    public void loadConfiguredSecretsManager() throws IOException
    {
        if (SECRETS_MANAGER_CONFIGURATION.exists()) {
            secretsManagerProperties.putAll(loadProperties(SECRETS_MANAGER_CONFIGURATION));
            checkArgument(
                    !isNullOrEmpty(secretsManagerProperties.get(SECRETS_MANAGER_PROPERTY_NAME)),
                    "Secrets Manager configuration %s does not contain %s",
                    SECRETS_MANAGER_CONFIGURATION.getAbsoluteFile(),
                    SECRETS_MANAGER_PROPERTY_NAME);
        }
    }

    /**
     * Return the loaded secrets manager implementation name
     * @return
     */
    public String getSecretsManagerName()
    {
        return secretsManagerProperties.get(SECRETS_MANAGER_PROPERTY_NAME);
    }

    @VisibleForTesting
    public void loadSecretsManagerConfiguration(Map<String, String> properties) throws IOException
    {
        secretsManagerProperties.putAll(properties);
    }
}
