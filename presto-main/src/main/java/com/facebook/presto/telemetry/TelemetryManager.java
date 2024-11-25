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
package com.facebook.presto.telemetry;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.TelemetryConfig;
import com.facebook.presto.opentelemetry.OpenTelemetryImpl;
import com.facebook.presto.spi.telemetry.TelemetryFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * OpenTelemetryManager class creates and manages OpenTelemetry and Tracer instances.
 */
public class TelemetryManager
{
    private static final Logger log = Logger.get(TelemetryManager.class);
    private static final File OPENTELEMETRY_CONFIGURATION = new File("etc/telemetry-tracing.properties");
    private static final String TRACING_FACTORY_NAME = "tracing-factory.name";

    private final Map<String, TelemetryFactory> openTelemetryFactories = new ConcurrentHashMap<>();
    private OpenTelemetry configuredOpenTelemetry;
    private static Tracer tracer = OpenTelemetry.noop().getTracer("no-op");   //default tracer

    public TelemetryManager()
    {
        addOpenTelemetryFactory(new OpenTelemetryImpl());
    }

    /**
     * adds and registers all the OpenTelemetryFactory implementations to support different configurations
     * @param openTelemetryFactory
     */
    public void addOpenTelemetryFactory(TelemetryFactory openTelemetryFactory)
    {
        requireNonNull(openTelemetryFactory, "openTelemetryFactory is null");
        log.debug("Adding telemetry factory");
        if (openTelemetryFactories.putIfAbsent(openTelemetryFactory.getName(), openTelemetryFactory) != null) {
            throw new IllegalArgumentException(format("openTelemetry factory '%s' is already registered", openTelemetryFactory.getName()));
        }
    }

    public void clearFactories()
    {
        openTelemetryFactories.clear();
    }

    /**
     * called from PrestoServer for loading the properties after OpenTelemetryManager is bound and injected
     * @throws Exception
     */
    public void loadConfiguredOpenTelemetry()
            throws Exception
    {
        if (OPENTELEMETRY_CONFIGURATION.exists()) {
            Map<String, String> properties = loadProperties(OPENTELEMETRY_CONFIGURATION);
            checkArgument(
                    !isNullOrEmpty(properties.get(TRACING_FACTORY_NAME)),
                    "Opentelemetry configuration %s does not contain %s",
                    OPENTELEMETRY_CONFIGURATION.getAbsoluteFile(),
                    TRACING_FACTORY_NAME);

            if (properties.isEmpty()) {
                log.debug("telemetry properties not loaded");
            }

            properties = new HashMap<>(properties);
            String openTelemetryFactoryName = properties.remove(TRACING_FACTORY_NAME);

            checkArgument(!isNullOrEmpty(openTelemetryFactoryName), "otel-factory.name property must be present");

            TelemetryFactory<OpenTelemetry> openTelemetryFactory = openTelemetryFactories.get(openTelemetryFactoryName);
            checkState(openTelemetryFactory != null, "Opentelemetry factory %s is not registered", openTelemetryFactoryName);

            log.debug("setting telemetry properties");
            TelemetryConfig.getTelemetryConfig().setTelemetryProperties(properties);

            log.debug("creating opentelemetry instance");
            this.configuredOpenTelemetry = openTelemetryFactory.create();

            log.debug("creating telemetry tracer");
            createTracer();
        }
    }

    /**
     * creates and updates sdk tracer instance if tracing is enabled. Else uses a default no-op instance.
     */
    public void createTracer()
    {
        if (TelemetryConfig.getTracingEnabled()) {
            tracer = configuredOpenTelemetry.getTracer("Presto");
        }
    }

    public static Tracer getTracer()
    {
        return tracer;
    }

    public static void setTracer(Tracer tracer)
    {
        TelemetryManager.tracer = tracer;
    }

    public OpenTelemetry getOpenTelemetry()
    {
        return this.configuredOpenTelemetry;
    }
}
