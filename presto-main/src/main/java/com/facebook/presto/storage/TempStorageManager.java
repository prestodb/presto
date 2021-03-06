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
package com.facebook.presto.storage;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.connector.ConnectorAwareNodeManager;
import com.facebook.presto.connector.system.GlobalSystemConnector;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageContext;
import com.facebook.presto.spi.storage.TempStorageFactory;
import com.facebook.presto.spiller.LocalTempStorage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spiller.LocalTempStorage.TEMP_STORAGE_PATH;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Files.getNameWithoutExtension;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TempStorageManager
{
    private static final Logger log = Logger.get(TempStorageManager.class);
    // TODO: Make this configurable
    private static final File TEMP_STORAGE_CONFIGURATION_DIR = new File("etc/temp-storage/");

    private final Map<String, TempStorageFactory> tempStorageFactories = new ConcurrentHashMap<>();
    private final Map<String, TempStorage> loadedTempStorages = new ConcurrentHashMap<>();
    private final AtomicBoolean tempStorageLoading = new AtomicBoolean();

    private final NodeManager nodeManager;

    @Inject
    public TempStorageManager(InternalNodeManager internalNodeManager, NodeInfo nodeInfo)
    {
        this(new ConnectorAwareNodeManager(
                requireNonNull(internalNodeManager, "internalNodeManager is null"),
                requireNonNull(nodeInfo, "nodeInfo is null").getEnvironment(),
                new ConnectorId(GlobalSystemConnector.NAME)));
    }

    @VisibleForTesting
    public TempStorageManager(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        addTempStorageFactory(new LocalTempStorage.Factory());
    }

    public void addTempStorageFactory(TempStorageFactory tempStorageFactory)
    {
        requireNonNull(tempStorageFactory, "tempStorageFactory is null");

        if (tempStorageFactories.putIfAbsent(tempStorageFactory.getName(), tempStorageFactory) != null) {
            throw new IllegalArgumentException(format("Temp Storage '%s' is already registered", tempStorageFactory.getName()));
        }
    }

    public void loadTempStorages()
            throws IOException
    {
        if (!tempStorageLoading.compareAndSet(false, true)) {
            return;
        }
        // Always load local temp storage
        loadTempStorage(
                LocalTempStorage.NAME,
                // TODO: Local temp storage should be configurable
                ImmutableMap.of(
                        TEMP_STORAGE_PATH,
                        Paths.get(System.getProperty("java.io.tmpdir"), "presto", "temp_storage").toAbsolutePath().toString()));

        for (File file : listFiles(TEMP_STORAGE_CONFIGURATION_DIR)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                String name = getNameWithoutExtension(file.getName());
                Map<String, String> properties = new HashMap<>(loadProperties(file));
                loadTempStorage(name, properties);
            }
        }
    }

    public TempStorage getTempStorage(String name)
    {
        TempStorage tempStorage = loadedTempStorages.get(name);
        checkState(tempStorage != null, "tempStorage %s was not loaded", name);

        return tempStorage;
    }

    protected void loadTempStorage(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading temp storage --");

        TempStorageFactory factory = tempStorageFactories.get(name);
        checkState(factory != null, "Temp Storage %s is not registered", name);

        TempStorage tempStorage = factory.create(properties, new TempStorageContext(nodeManager));
        if (loadedTempStorages.putIfAbsent(name, tempStorage) != null) {
            throw new IllegalArgumentException(format("Temp Storage '%s' is already loaded", name));
        }

        log.info("-- Loaded temp storage %s --", name);
    }

    private static List<File> listFiles(File dir)
    {
        if (dir != null && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
}
