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
package com.facebook.presto.plugin.openlineage;

import com.facebook.presto.Session;
import com.facebook.presto.plugin.memory.MemoryPlugin;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpcds.TpcdsPlugin;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.Transport;

import java.net.URI;
import java.util.Map;

import static com.facebook.airlift.testing.Closeables.closeAllSuppress;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

/**
 * Builds a single-node {@link DistributedQueryRunner} with the OpenLineage event listener installed,
 * so tests can run real queries and inspect the events the plugin emits for them.
 * Modeled on Trino's {@code OpenLineageListenerQueryRunner}.
 */
public final class OpenLineageListenerQueryRunner
{
    // Catalog used for output data
    public static final String CATALOG = "memory";
    public static final String SCHEMA = "default";
    public static final String PRESTO_URI = "http://presto-integration-test:1337";
    // The listener derives job and dataset namespaces from presto.uri by swapping the scheme
    public static final String OPENLINEAGE_NAMESPACE = "presto://presto-integration-test:1337";

    private static final URI PRODUCER_URI = URI.create("https://github.com/prestodb/presto/plugin/presto-openlineage-event-listener");

    private OpenLineageListenerQueryRunner() {}

    public static DistributedQueryRunner createQueryRunner(EventListener eventListener)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .build();
        try {
            queryRunner.installPlugin(new TestingEventListenerPlugin(eventListener));

            queryRunner.installPlugin(new MemoryPlugin());
            queryRunner.createCatalog(CATALOG, "memory");

            // Catalogs used for input data
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            queryRunner.installPlugin(new TpcdsPlugin());
            queryRunner.createCatalog("tpcds", "tpcds");

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    /**
     * Creates the listener the way {@link OpenLineageEventListenerFactory} does, but sending events to the given transport.
     */
    public static OpenLineageEventListener createEventListener(Transport transport)
    {
        return new OpenLineageEventListener(
                new OpenLineage(PRODUCER_URI),
                OpenLineageClient.builder().transport(transport).build(),
                new OpenLineageEventListenerConfig().setPrestoURI(URI.create(PRESTO_URI)));
    }

    /**
     * Registers a pre-built listener. The testing event listener manager instantiates every registered
     * factory with an empty configuration, so the configuration is applied when the listener is built.
     */
    private static class TestingEventListenerPlugin
            implements Plugin
    {
        private final EventListener eventListener;

        public TestingEventListenerPlugin(EventListener eventListener)
        {
            this.eventListener = requireNonNull(eventListener, "eventListener is null");
        }

        @Override
        public Iterable<EventListenerFactory> getEventListenerFactories()
        {
            return ImmutableList.of(new EventListenerFactory()
            {
                @Override
                public String getName()
                {
                    return "openlineage-event-listener-testing";
                }

                @Override
                public EventListener create(Map<String, String> config)
                {
                    return eventListener;
                }
            });
        }
    }
}
