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
package com.facebook.presto.spark;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spark.classloader_interface.SparkProcessType;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageFactory;
import com.facebook.presto.spiller.LocalTempStorage;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.storage.TempStorageManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import jakarta.inject.Inject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.spark.classloader_interface.SparkProcessType.EXECUTOR;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * On executors that run native execution, the JVM does not run Presto operators, so it normally never uses
 * a temp storage, yet creating one can be expensive (for example, a file system client). With
 * spark.defer-native-executor-temp-storage, such executors create each temp storage other than the local
 * one on its first getTempStorage() call, so a Java task scheduled on them still gets the temp storages it
 * needs. The configuration is still validated at startup; only the creation is deferred.
 */
public class PrestoSparkTempStorageManager
        extends TempStorageManager
{
    private static final Logger log = Logger.get(PrestoSparkTempStorageManager.class);

    private final boolean deferTempStorages;
    private final Set<String> tempStorageFactoryNames = ConcurrentHashMap.newKeySet();
    private final Map<String, Map<String, String>> deferredTempStorages = new ConcurrentHashMap<>();

    @Inject
    public PrestoSparkTempStorageManager(
            InternalNodeManager internalNodeManager,
            NodeInfo nodeInfo,
            SparkProcessType sparkProcessType,
            FeaturesConfig featuresConfig,
            PrestoSparkConfig prestoSparkConfig)
    {
        super(internalNodeManager, nodeInfo);
        this.deferTempStorages = shouldDeferTempStorages(
                requireNonNull(sparkProcessType, "sparkProcessType is null"),
                requireNonNull(featuresConfig, "featuresConfig is null").isNativeExecutionEnabled(),
                requireNonNull(prestoSparkConfig, "prestoSparkConfig is null").isDeferNativeExecutorTempStorage());
    }

    @VisibleForTesting
    PrestoSparkTempStorageManager(NodeManager nodeManager, boolean deferTempStorages)
    {
        super(nodeManager);
        this.deferTempStorages = deferTempStorages;
    }

    public static boolean shouldDeferTempStorages(
            SparkProcessType sparkProcessType,
            boolean nativeExecutionEnabled,
            boolean deferNativeExecutorTempStorage)
    {
        return sparkProcessType.equals(EXECUTOR) && nativeExecutionEnabled && deferNativeExecutorTempStorage;
    }

    /**
     * Whether the temp storage with this name is created on first use instead of at startup. The local temp
     * storage is never deferred: it is cheap, and creating it prepares its directories.
     */
    public boolean isTempStorageDeferred(String name)
    {
        return deferTempStorages && !LocalTempStorage.NAME.equals(name);
    }

    @Override
    public void addTempStorageFactory(TempStorageFactory tempStorageFactory)
    {
        super.addTempStorageFactory(tempStorageFactory);
        tempStorageFactoryNames.add(tempStorageFactory.getName());
    }

    @Override
    public TempStorage getTempStorage(String name)
    {
        // Creates a deferred temp storage once. Concurrent callers for the same name wait for it without
        // blocking other names; if creation fails, the entry stays and the next call retries.
        deferredTempStorages.computeIfPresent(name, (key, properties) -> {
            super.loadTempStorage(key, properties);
            return null;
        });
        return super.getTempStorage(name);
    }

    @Override
    protected void loadTempStorage(String name, Map<String, String> properties)
    {
        if (!isTempStorageDeferred(name)) {
            super.loadTempStorage(name, properties);
            return;
        }
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        // Same checks as TempStorageManager.loadTempStorage, so a bad configuration still fails at startup
        String tempStorageFactoryName = properties.get(TEMP_STORAGE_FACTORY_NAME);
        checkState(tempStorageFactoryName != null, "Configuration for tempStorage %s does not contain %s", name, TEMP_STORAGE_FACTORY_NAME);
        checkState(tempStorageFactoryNames.contains(tempStorageFactoryName), "Temp Storage Factory %s is not registered", tempStorageFactoryName);

        if (deferredTempStorages.putIfAbsent(name, ImmutableMap.copyOf(properties)) != null) {
            throw new IllegalArgumentException(format("Temp Storage '%s' is already loaded", name));
        }
        log.info("-- Deferring temp storage %s until first use --", name);
    }
}
