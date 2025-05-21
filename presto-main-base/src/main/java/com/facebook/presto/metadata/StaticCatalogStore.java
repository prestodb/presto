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
import com.facebook.presto.server.security.crypto.CryptoUtils;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import jakarta.inject.Inject;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;

public class StaticCatalogStore
{
    private static final Logger log = Logger.get(StaticCatalogStore.class);
    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final Set<String> disabledCatalogs;
    private final AtomicBoolean catalogsLoading = new AtomicBoolean();
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();
    private final Map<String, String> allEnvProperties;

    @Inject
    public StaticCatalogStore(ConnectorManager connectorManager, StaticCatalogStoreConfig config, FeaturesConfig featuresConfig)
    {
        this(connectorManager,
                config.getCatalogConfigurationDir(),
                firstNonNull(config.getDisabledCatalogs(), ImmutableList.of()),
                // Load the secrets file in the environment variable map, in case we didn't restart cleanly with preset env variables
                CryptoUtils.loadDecryptedProperties(featuresConfig.getIbmLhSecretPropsFile()));
    }

    public StaticCatalogStore(ConnectorManager connectorManager, File catalogConfigurationDir, List<String> disabledCatalogs, Map<String, String> decryptedEnvProperties)
    {
        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.disabledCatalogs = ImmutableSet.copyOf(disabledCatalogs);
        HashMap<String, String> mergedMap = new HashMap<>(System.getenv()); // Env variables are set by the crypto library + startup scripts with decrypted secrets
        mergedMap.putAll(decryptedEnvProperties); // Add those from the explicit secret file. Duplicate keys overridden
        this.allEnvProperties = ImmutableMap.copyOf(mergedMap);
    }

    @VisibleForTesting
    static String substitutePlaceHolder(String propertyValue, Map<String, String> environmentMap)
    {
        if (propertyValue.startsWith("${") && propertyValue.endsWith("}")) {
            String envVariable = propertyValue.substring(2, propertyValue.length() - 1);
            log.info("Substituting [%s] property using the value of the [%s] environment variable", propertyValue, envVariable);

            if (environmentMap.containsKey(envVariable)) {
                propertyValue = environmentMap.get(envVariable);
            }
            else {
                String errorMessage = String.format("Unable to find env variable [%s] corresponding to property value [%s]", envVariable, propertyValue);
                log.error(errorMessage);
                throw new RuntimeException(errorMessage);
            }
        }
        return propertyValue;
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

    public ConnectorId loadCatalog(String catalogName, Map<String, String> properties)
    {
        ConnectorId connectorId = null;
        if (disabledCatalogs.contains(catalogName)) {
            log.info("Skipping disabled catalog %s", catalogName);
            return connectorId;
        }

        log.info("-- Loading catalog %s --", catalogName);

        String connectorName = null;
        ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        for (Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals("connector.name")) {
                connectorName = entry.getValue();
            }
            else {
                String propertyValue = entry.getValue();
                propertyValue = substitutePlaceHolder(propertyValue, allEnvProperties);
                connectorProperties.put(entry.getKey(), propertyValue);
            }
        }

        checkState(connectorName != null, "Configuration for catalog %s does not contain connector.name", catalogName);

        connectorId = connectorManager.createConnection(catalogName, connectorName, connectorProperties.build());
        log.info("-- Added catalog %s using connector %s --", catalogName, connectorName);
        return connectorId;
    }

    public void dropConnection(String catalogName)
    {
        connectorManager.dropConnection(catalogName);
    }

    private void loadCatalog(File file)
            throws Exception
    {
        String catalogName = Files.getNameWithoutExtension(file.getName());

        log.info("-- Loading catalog properties %s --", file);
        Map<String, String> properties = loadProperties(file);
        checkState(properties.containsKey("connector.name"), "Catalog configuration %s does not contain connector.name", file.getAbsoluteFile());

        loadCatalog(catalogName, properties);
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
