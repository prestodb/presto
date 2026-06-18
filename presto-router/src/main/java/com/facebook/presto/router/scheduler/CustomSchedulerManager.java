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
package com.facebook.presto.router.scheduler;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.router.RouterPluginManager;
import com.facebook.presto.spi.router.Scheduler;
import com.facebook.presto.spi.router.SchedulerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;

public class CustomSchedulerManager
{
    private static final Logger log = Logger.get(CustomSchedulerManager.class);
    public static final File ROUTER_SCHEDULER_PROPERTIES = new File("etc/router-config/router-scheduler.properties");

    private final File configFile;
    private static final String NAME_PROPERTY = "router-scheduler.name";

    private final Map<String, SchedulerFactory> factories = new ConcurrentHashMap<>();
    private Scheduler scheduler;

    @Inject
    public CustomSchedulerManager(RouterPluginManager routerPluginManager)
    {
        try {
            routerPluginManager.loadPlugins();
            List<SchedulerFactory> factoryList = routerPluginManager.getRegisteredSchedulerFactoryList();
            for (SchedulerFactory factory : factoryList) {
                addSchedulerFactory(factory);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.configFile = ROUTER_SCHEDULER_PROPERTIES;
    }

    @VisibleForTesting
    public CustomSchedulerManager(List<SchedulerFactory> schedulerFactoryList, File configFile)
    {
        for (SchedulerFactory factory : schedulerFactoryList) {
            addSchedulerFactory(factory);
        }
        this.configFile = configFile;
    }

    public void addSchedulerFactory(SchedulerFactory factory)
    {
        checkArgument(factories.putIfAbsent(factory.getName(), factory) == null,
                "Presto Router Scheduler '%s' is already registered", factory.getName());
    }

    public void loadScheduler()
    {
        File configFileLocation = configFile.getAbsoluteFile();
        Map<String, String> properties;
        try {
            properties = new HashMap<>(loadProperties(configFileLocation));
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to load properties from config file", e);
        }

        String name = properties.remove(NAME_PROPERTY);
        checkArgument(!isNullOrEmpty(name),
                "Router scheduler configuration %s does not contain %s", configFileLocation, NAME_PROPERTY);

        log.info("-- Loading Presto Router Scheduler %s --", name);
        SchedulerFactory factory = factories.get(name);
        checkState(factory != null, "Presto Router Scheduler %s is not registered", name);
        this.scheduler = factory.create(ImmutableMap.copyOf(properties));
        log.info("-- Loaded Presto Router Scheduler %s --", name);
    }

    public Scheduler getScheduler()
    {
        checkState(scheduler != null, "scheduler was not loaded");
        return scheduler;
    }
}
