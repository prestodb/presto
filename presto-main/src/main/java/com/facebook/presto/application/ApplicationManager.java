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

package com.facebook.presto.application;

import com.facebook.presto.spi.application.Application;
import com.facebook.presto.spi.application.ApplicationFactory;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.google.inject.Injector;
import io.airlift.log.Logger;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ApplicationManager
{
    private static final Logger log = Logger.get(ApplicationManager.class);

    @GuardedBy("this")
    private final Map<String, ApplicationFactory> applicationFactories = new ConcurrentHashMap<>();
    @GuardedBy("this")
    private final Map<String, Application> applications = new ConcurrentHashMap<>();

    private final AtomicBoolean stopped = new AtomicBoolean();

    public synchronized void addApplicationFactory(ApplicationFactory applicationFactory)
    {
        requireNonNull(applicationFactory, "applicationFactory is null");

        if (applicationFactories.putIfAbsent(applicationFactory.getName(), applicationFactory) != null) {
            throw new IllegalArgumentException(format("Application '%s' is already registered", applicationFactory.getName()));
        }
    }

    @PreDestroy
    public void stop()
    {
        if (stopped.getAndSet(true)) {
            return;
        }

        for (Map.Entry<String, Application> entry : applications.entrySet()) {
            Application application = entry.getValue();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(application.getClass().getClassLoader())) {
                application.shutdown();
            }
            catch (Throwable t) {
                log.error(t, "Error shutting down application: %s", entry.getKey());
            }
        }
    }

    protected synchronized void setup(String applicationName, ApplicationFactory applicationFactory, Map<String, String> properties)
    {
        checkState(!stopped.get(), "ApplicationManager is stopped");
        checkState(!applications.containsKey(applicationName), "Application %s already exists", applicationName);
        try {
            applications.put(applicationName, applicationFactory.create(properties));
        }
        catch (Throwable t) {
            log.error(t, "Error initializing application: %s", applicationFactory.getName());
        }
    }

    public void setup(String applicationName, Map<String, String> properties)
    {
        checkState(!stopped.get(), "ApplicationManager is stopped");
        ApplicationFactory applicationFactory = applicationFactories.get(applicationName);
        checkArgument(applicationFactory != null, "No factory for application %s", applicationName);
        setup(applicationName, applicationFactory, properties);
    }

    protected void start(Injector injector)
    {
        for (Map.Entry<String, Application> entry : applications.entrySet()) {
            Application application = entry.getValue();
            try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(application.getClass().getClassLoader())) {
                application.run(injector);
            }
            catch (Throwable t) {
                log.error(t, "Error starting application: %s", entry.getKey());
            }
        }
    }

    public void startApplications(Injector injector)
    {
        requireNonNull(injector, "injector is null");
        checkState(!stopped.get(), "ApplicationManager is stopped");
        start(injector);
    }
}
