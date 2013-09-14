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

import com.facebook.presto.connector.ConnectorManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.fromProperties;

public class CatalogManager
{
    private static final Logger log = Logger.get(CatalogManager.class);
    private final ConnectorManager connectorManager;
    private final File catalogConfigurationDir;
    private final AtomicBoolean catalogsLoaded = new AtomicBoolean();

    @Inject
    public CatalogManager(ConnectorManager connectorManager, CatalogManagerConfig config)
    {
        this(connectorManager, config.getCatalogConfigurationDir());
    }

    public CatalogManager(ConnectorManager connectorManager, File catalogConfigurationDir)
    {
        this.connectorManager = connectorManager;
        this.catalogConfigurationDir = catalogConfigurationDir;
    }

    public void loadCatalogs()
            throws Exception
    {
        if (!catalogsLoaded.compareAndSet(false, true)) {
            return;
        }

        for (File file : listFiles(catalogConfigurationDir)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                loadCatalog(file);
            }
        }
    }

    private void loadCatalog(File file)
            throws Exception
    {
        Map<String, String> properties = new HashMap<>(loadProperties(file));

        String connectorName = properties.remove("connector.name");
        checkState(connectorName != null, "Catalog configuration %s does not contain conector.name", file.getAbsoluteFile());

        String catalogName = Files.getNameWithoutExtension(file.getName());

        connectorManager.createConnection(catalogName, connectorName, ImmutableMap.copyOf(properties));
        log.info("Added catalog %s using connector %s", catalogName, connectorName);
    }

    private List<File> listFiles(File installedPluginsDir)
    {
        if (installedPluginsDir != null && installedPluginsDir.isDirectory()) {
            File[] files = installedPluginsDir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static Map<String, String> loadProperties(File file)
            throws Exception
    {
        checkNotNull(file, "file is null");

        Properties properties = new Properties();
        try (FileInputStream in = new FileInputStream(file)) {
            properties.load(in);
        }
        return fromProperties(properties);
    }
}
