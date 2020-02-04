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
import com.facebook.presto.server.PrestoServer;
import com.google.common.io.Files;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.server.PrestoServer.updateDatasourcesWithSpecifiedAction;

public class DynamicCatalogStore
        extends StaticCatalogStore
{
    private static final Logger log = Logger.get(DynamicCatalogStore.class);
    private final Duration watchTimeout;
    private final boolean dynamicUpdateEanbled;

    @Inject
    public DynamicCatalogStore(ConnectorManager connectorManager, DynamicCatalogStoreConfig config)
    {
        super(connectorManager, config);
        watchTimeout = config.getWatchTimeout();
        dynamicUpdateEanbled = config.isDynamicUpdateEanbled();
    }

    public void loadCatalogs()
            throws Exception
    {
        super.loadCatalogs();

        if (dynamicUpdateEanbled) {
            // update catalogs dynamically
            new Thread(() -> {
                try {
                    log.info("-- Catalog watcher thread start --");
                    startCatalogConfigWatcher(getCatalogConfigurationDir());
                }
                catch (Exception e) {
                    log.error(e);
                }
            }).start();
        }
    }

    private void startCatalogConfigWatcher(File catalogConfigurationDir) throws IOException, InterruptedException
    {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        Paths.get(catalogConfigurationDir.getAbsolutePath()).register(
                watchService,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE);

        while (true) {
            WatchKey key = watchService.take();
            for (WatchEvent<?> event : key.pollEvents()) {
                if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                    log.info("New file in catalog directory : " + event.context());
                    Path newCatalog = (Path) event.context();
                    addCatalog(newCatalog);
                }
                else if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
                    log.info("Delete file from catalog directory : " + event.context());
                    Path deletedCatalog = (Path) event.context();
                    deleteCatalog(deletedCatalog);
                }
                else if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                    log.info("Modify file from catalog directory : " + event.context());
                    Path modifiedCatalog = (Path) event.context();
                    modifyCatalog(modifiedCatalog);
                }
            }
            boolean valid = key.reset();
            if (!valid) {
                break;
            }
        }
    }

    private void addCatalog(Path catalogPath)
    {
        File file = new File(getCatalogConfigurationDir(), catalogPath.getFileName().toString());
        if (file.isFile() && file.getName().endsWith(".properties")) {
            try {
                TimeUnit.SECONDS.sleep((long) watchTimeout.getValue(TimeUnit.SECONDS));
                loadCatalog(file);
                updateDatasourcesWithSpecifiedAction(Files.getNameWithoutExtension(catalogPath.getFileName().toString()), PrestoServer.DatasourceAction.ADD);
            }
            catch (Exception e) {
                log.error(e);
            }
        }
    }

    private void deleteCatalog(Path catalogPath)
    {
        if (catalogPath.getFileName().toString().endsWith(".properties")) {
            String catalogName = Files.getNameWithoutExtension(catalogPath.getFileName().toString());
            log.info("-- Removing catalog %s", catalogName);
            getConnectorManager().dropConnection(catalogName);
            updateDatasourcesWithSpecifiedAction(catalogName, PrestoServer.DatasourceAction.DELETE);
            log.info("-- Removed catalog %s", catalogName);
        }
    }

    private void modifyCatalog(Path catalogPath)
    {
        deleteCatalog(catalogPath);
        addCatalog(catalogPath);
    }
}
