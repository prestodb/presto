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
package com.facebook.presto.eventlistener;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryProgressEvent;
import com.facebook.presto.spi.eventlistener.QueryUpdatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class EventListenerManager
{
    private static final Logger log = Logger.get(EventListenerManager.class);
    private static final File EVENT_LISTENER_CONFIGURATION = new File("etc/event-listener.properties");
    private static final String EVENT_LISTENER_PROPERTY_NAME = "event-listener.name";
    private final List<File> configFiles;
    private final Map<String, EventListenerFactory> eventListenerFactories = new ConcurrentHashMap<>();
    private final AtomicReference<List<EventListener>> configuredEventListeners =
            new AtomicReference<>(ImmutableList.of());
    private final AtomicBoolean loading = new AtomicBoolean(false);

    @Inject
    public EventListenerManager(EventListenerConfig config)
    {
        this.configFiles = ImmutableList.copyOf(config.getEventListenerFiles());
    }

    public void addEventListenerFactory(EventListenerFactory eventListenerFactory)
    {
        requireNonNull(eventListenerFactory, "eventListenerFactory is null");

        if (eventListenerFactories.putIfAbsent(eventListenerFactory.getName(), eventListenerFactory) != null) {
            throw new IllegalArgumentException(format("Event listener '%s' is already registered", eventListenerFactory.getName()));
        }
    }

    public void loadConfiguredEventListeners()
    {
        checkState(loading.compareAndSet(false, true), "Event listeners already loaded");
        List<File> configFiles = this.configFiles;
        if (configFiles.isEmpty()) {
            if (!EVENT_LISTENER_CONFIGURATION.exists()) {
                return;
            }
            configFiles = ImmutableList.of(EVENT_LISTENER_CONFIGURATION);
        }
        configFiles.forEach(this::createEventListener);
    }

    private void createEventListener(File configFile)
    {
        log.info("-- Loading event listener configuration file %s --", configFile);
        if (configFile.exists()) {
            configFile = configFile.getAbsoluteFile();
            log.info("-- Loading event listener configuration file : %s --", configFile);
            try {
                Map<String, String> properties = new HashMap<>(loadProperties(configFile));
                loadConfiguredEventListener(properties);
                log.info("-- Loaded event listener configuration file %s --", configFile);
            }
            catch (IOException e) {
                log.error(e, "IOException while loading configuration file: " + configFile);
                throw new UncheckedIOException("Failed to read configuration file: " + configFile, e);
            }
        }
        else {
            log.info("Unable to locate configuration file %s --", configFile);
        }
    }

    private void setConfiguredEventListener(String name, Map<String, String> properties)
    {
        requireNonNull(name, "name is null");
        requireNonNull(properties, "properties is null");

        log.info("-- Loading event listener --");

        EventListenerFactory eventListenerFactory = eventListenerFactories.get(name);
        checkState(eventListenerFactory != null, "Event listener %s is not registered", name);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(eventListenerFactory.getClass().getClassLoader())) {
            EventListener eventListener = eventListenerFactory.create(ImmutableMap.copyOf(properties));
            ImmutableList<EventListener> eventListeners = ImmutableList.<EventListener>builder()
                    .addAll(this.configuredEventListeners.get())
                    .add(eventListener)
                    .build();
            this.configuredEventListeners.set(eventListeners);
        }

        log.info("-- Loaded event listener %s --", name);
    }

    public void loadConfiguredEventListener(Map<String, String> properties)
    {
        properties = new HashMap<>(properties);
        String eventListenerName = properties.remove(EVENT_LISTENER_PROPERTY_NAME);
        checkArgument(!isNullOrEmpty(eventListenerName), "event-listener.name property must be present");
        setConfiguredEventListener(eventListenerName, properties);
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        configuredEventListeners.get()
                .forEach(eventListener -> eventListener.queryCompleted(queryCompletedEvent));
    }

    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        configuredEventListeners.get()
                .forEach(eventListener -> eventListener.queryCreated(queryCreatedEvent));
    }

    public void queryUpdated(QueryUpdatedEvent queryUpdatedEvent)
    {
        configuredEventListeners.get()
                .forEach(eventListener -> eventListener.queryUpdated(queryUpdatedEvent));
    }

    public void publishQueryProgress(QueryProgressEvent queryProgressEvent)
    {
        configuredEventListeners.get()
                .forEach(eventListener -> eventListener.publishQueryProgress(queryProgressEvent));
    }

    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        configuredEventListeners.get()
                .forEach(eventListener -> eventListener.splitCompleted(splitCompletedEvent));
    }
}
