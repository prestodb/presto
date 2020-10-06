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
import com.facebook.presto.spi.spiller.SpillStorageService;
import com.facebook.presto.spi.spiller.SpillStorageServiceProvider;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.google.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SpillStorageServiceManager
{
    private static final Logger log = Logger.get(SpillStorageServiceManager.class);

    private final String spillStorageServiceName;
    private final Map<String, SpillStorageServiceProvider> spillStorageServiceProviders = new ConcurrentHashMap<>();
    private final AtomicReference<SpillStorageService> storageService = new AtomicReference<>();

    @Inject
    public SpillStorageServiceManager(FeaturesConfig featuresConfig)
    {
        this.spillStorageServiceName = requireNonNull(featuresConfig, "featuresConfig is null").getSpillStorageService();
        addSpillStorageServiceProvider(new SpillLocalStorageService.Provider());
    }

    public void addSpillStorageServiceProvider(SpillStorageServiceProvider spillStorageServiceProvider)
    {
        requireNonNull(spillStorageServiceProvider, "spillStorageServiceProvider is null");

        if (spillStorageServiceProviders.putIfAbsent(spillStorageServiceProvider.getName(), spillStorageServiceProvider) != null) {
            throw new IllegalArgumentException(format("Storage service provider '%s' is already registered", spillStorageServiceProvider.getName()));
        }
    }

    public void loadSpillStorageServiceProvider()
    {
        log.info("-- Loading spill storage service --");

        SpillStorageServiceProvider provider = spillStorageServiceProviders.get(spillStorageServiceName);
        checkState(provider != null, "Spill storage service %s is not registered", spillStorageServiceName);

        SpillStorageService storageService = provider.get();
        this.storageService.set(requireNonNull(storageService, "storageService is null"));

        log.info("-- Loaded spill storage service %s --", spillStorageServiceName);
    }

    public SpillStorageService getSpillStorageService()
    {
        checkState(storageService.get() != null, "storageService was not loaded");
        return storageService.get();
    }
}
