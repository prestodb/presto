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
package com.facebook.presto.metadata;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.secretsManager.SecretsManagerHandler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;

import javax.inject.Inject;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;

public class StaticCatalogStore
{
    private static final Logger log = Logger.get(StaticCatalogStore.class);
    public static final String SECRETS_MANAGER_ENABLED = "secretsManager.enabled";
    public static final String CONNECTOR_NAME = "connector.name";
    private static final Map<String, Map<String, String>> secrets = new ConcurrentHashMap<>();

    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private final SecretsManagerHandler secretsManagerHandler;

    @Inject
    public StaticCatalogStore(ConnectorManager connectorManager, StaticCatalogStoreConfig config, SecretsManagerHandler secretsManagerHandler)
    {
        this(connectorManager,
                config.getCatalogConfigurationDir(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()), secretsManagerHandler);
    }

    public StaticCatalogStore(ConnectorManager connectorManager, File catalogConfigurationDir, List<String> disabledCatalogs, SecretsManagerHandler secretsManagerHandler)
    {
        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
        this.secretsManagerHandler = secretsManagerHandler;
    }

    public boolean areCatalogsLoaded()
    {
        return catalogsLoaded.get();
    }

    public void loadCatalogs()
            throws Exception
    {
        loadCatalogs(ImmutableMap.of());
    }

    public void loadCatalogs(Map<String, Map<String, String>> additionalCatalogs)
            throws Exception
    {
        final String secretsManagerName = secretsManagerHandler.getSecretsManagerName();
        if (secretsManagerName != null && !"none".equalsIgnoreCase(secretsManagerName)) {
            log.info("-- Fetching secrets from  %s --", secretsManagerName);
            secrets.putAll(secretsManagerHandler.loadSecrets(secretsManagerName));
        }
        if (!catalogsLoading.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                loadCatalog(file);
            }
        }

        additionalCatalogs.forEach(this::loadCatalog);

        catalogsLoaded.set(true);
    }

    private void loadCatalog(File file)
            throws Exception
    {
        String catalogName = Files.getNameWithoutExtension(file.getName());

        log.info("-- Loading catalog properties %s --", file);
        Map<String, String> properties = loadProperties(file);
        checkState(properties.containsKey(CONNECTOR_NAME), "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());

        loadCatalog(catalogName, properties);
    }

    private void loadCatalog(String catalogName, Map<String, String> properties)
    {
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return;
        }

        log.info("-- Loading catalog %s --", catalogName);

        Map<String, String> catalogSecrets = getSecretsForCatalog(properties, catalogName);
        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals(CONNECTOR_NAME)) {
                connectorName = entry.getValue();
            }
            else if (!entry.getKey().equals(SECRETS_MANAGER_ENABLED) && !catalogSecrets.containsKey(entry.getKey())) {
                // Ignore the secrets manager properties & keys present in secrets to avoid unnecessary/duplicate binding later
                connectorProperties.put(entry.getKey(), entry.getValue());
            }
        }
        // Remove connector name from secrets if present
        catalogSecrets.remove(CONNECTOR_NAME);
        connectorProperties.putAll(catalogSecrets);
        checkState(connectorName != null, "Configuration for catalog %s does not contain connector.name", catalogName);

        connectorManager.createConnection(catalogName, connectorName, connectorProperties.build());
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
    }

    private Map<String, String> getSecretsForCatalog(Map<String, String> properties, String catalogName)
    {
        if (properties.containsKey(SECRETS_MANAGER_ENABLED) && "true".equalsIgnoreCase(properties.get(SECRETS_MANAGER_ENABLED))) {
            if (secrets.containsKey(catalogName)) {
                return secrets.get(catalogName);
            }
            log.warn("No secrets fetched for catalog %s. Check the Secrets Manager Configuration", catalogName);
        }
        return Collections.emptyMap();
    }

    private static List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
