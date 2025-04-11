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
import com.facebook.presto.spi.router.Scheduler;
import com.facebook.presto.spi.router.SchedulerFactory;
import com.google.common.collect.ImmutableMap;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class SchedulerManager
{
    private static final Logger log = Logger.get(SchedulerManager.class);

    private static final File CONFIG_FILE = new File("etc/router-config/router-scheduler.properties");
    private static final String NAME_PROPERTY = "router-scheduler.name";

    private final AtomicBoolean required = new AtomicBoolean();
    private final Map<String, SchedulerFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<Scheduler> scheduler = new AtomicReference<>();
    private final AtomicReference<String> schedulerName = new AtomicReference<>("");

    public void setRequired()
    {
        required.set(true);
    }

    public void addSchedulerFactory(SchedulerFactory factory)
    {
        checkArgument(factories.putIfAbsent(factory.getName(), factory) == null,
                "Presto Router Scheduler '%s' is already registered", factory.getName());
    }

    public void loadScheduler()
            throws Exception
    {
        if (!required.get()) {
            return;
        }

        File configFileLocation = CONFIG_FILE.getAbsoluteFile();
        Map<String, String> properties = new HashMap<>(loadProperties(configFileLocation));

        String name = properties.remove(NAME_PROPERTY);
        checkArgument(!isNullOrEmpty(name),
                "Router scheduler configuration %s does not contain %s", configFileLocation, NAME_PROPERTY);

        if (!schedulerName.get().equalsIgnoreCase(name)) {
            log.info("-- Loading Presto Router Scheduler --");
            SchedulerFactory factory = factories.get(name);
            checkState(factory != null, "Presto Router Scheduler %s is not registered", name);
            Scheduler scheduler = factory.create(ImmutableMap.copyOf(properties));
            this.scheduler.set(requireNonNull(scheduler, "scheduler is null"));
            this.schedulerName.set(name);
            log.info("-- Loaded Presto Router Scheduler %s --", name);
        }
    }

    public Scheduler getScheduler()
    {
        checkState(scheduler.get() != null, "scheduler was not loaded");
        return scheduler.get();
    }
}
