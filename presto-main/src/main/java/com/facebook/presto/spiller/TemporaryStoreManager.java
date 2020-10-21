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
package com.facebook.presto.spiller;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.storage.TemporaryStore;
import com.facebook.presto.spi.storage.TemporaryStoreFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.spiller.LocalTemporaryStore.TEMPORARY_STORE_PATH;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Files.getNameWithoutExtension;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TemporaryStoreManager
{
    private static final Logger log = Logger.get(TemporaryStoreManager.class);
    // TODO: Make this configurable
    private static final File TEMP_STORAGE_CONFIGURATION_DIR = new File("etc/temp-storage/");

    private final Map<String, TemporaryStoreFactory> temporaryStoreFactories = new ConcurrentHashMap<>();
    private final Map<String, TemporaryStore> loadedTemporaryStores = new ConcurrentHashMap<>();
    private final AtomicBoolean temporaryStoreLoading = new AtomicBoolean();

    public TemporaryStoreManager()
    {
        addTemporaryStoreFactory(new LocalTemporaryStore.Factory());
    }

    public void addTemporaryStoreFactory(TemporaryStoreFactory temporaryStoreFactory)
    {
        requireNonNull(temporaryStoreFactory, "temporaryStoreFactory is null");

        if (temporaryStoreFactories.putIfAbsent(temporaryStoreFactory.getName(), temporaryStoreFactory) != null) {
            throw new IllegalArgumentException(format("Temporary store '%s' is already registered", temporaryStoreFactory.getName()));
        }
    }

    public void loadTemporaryStores()
            throws IOException
    {
        if (!temporaryStoreLoading.compareAndSet(false, true)) {
            return;
        }
        // Always load local temp storage
        loadTemporaryStore(
                LocalTemporaryStore.NAME,
                // TODO: Local temp storage should be configurable
                ImmutableMap.of(
                        TEMPORARY_STORE_PATH,
                        Paths.get(System.getProperty("java.io.tmpdir"), "presto", "temp_storage").toAbsolutePath().toString()));

        for (File file : listFiles(TEMP_STORAGE_CONFIGURATION_DIR)) {
            if (file.isFile() && file.getName().endsWith(".properties")) {
                String name = getNameWithoutExtension(file.getName());
                Map<String, String> properties = new HashMap<>(loadProperties(file));
                loadTemporaryStore(name, properties);
            }
        }
    }

    public TemporaryStore getTemporaryStore(String name)
    {
        TemporaryStore temporaryStore = loadedTemporaryStores.get(name);
        checkState(temporaryStore != null, "temporaryStore %s was not loaded", name);

        return temporaryStore;
    }

    protected void loadTemporaryStore(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading temporary store --");

        TemporaryStoreFactory factory = temporaryStoreFactories.get(name);
        checkState(factory != null, "Temporary store %s is not registered", name);

        TemporaryStore temporaryStore = factory.create(properties);
        if (loadedTemporaryStores.putIfAbsent(name, temporaryStore) != null) {
            throw new IllegalArgumentException(format("Temporary store '%s' is already loaded", name));
        }

        log.info("-- Loaded temporary store %s --", name);
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
