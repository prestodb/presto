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
package com.facebook.presto.server;

import com.facebook.presto.discovery.EmbeddedDiscoveryModule;
import com.facebook.presto.metadata.CatalogManager;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.event.client.HttpEventModule;
import io.airlift.event.client.JsonEventModule;
import io.airlift.floatingdecimal.FloatingDecimal;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxHttpModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import org.weakref.jmx.guice.MBeanModule;

import static com.facebook.presto.server.CodeCacheGcTrigger.installCodeCacheGcTrigger;

import static com.facebook.presto.server.PrestoJvmRequirements.verifyJvmRequirements;

public class PrestoServer
        implements Runnable
{
    public static void main(String[] args)
    {
        new PrestoServer().run();
    }

    @Override
    public void run()
    {
        verifyJvmRequirements();

        Logger log = Logger.get(PrestoServer.class);

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new NodeModule(),
                new DiscoveryModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new JmxModule(),
                new JmxHttpModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new JsonEventModule(),
                new HttpEventModule(),
                new EmbeddedDiscoveryModule(),
                new ServerMainModule());

        modules.addAll(getAdditionalModules());

        Bootstrap app = new Bootstrap(modules.build());

        try {
            Injector injector = app.strictConfig().initialize();

            if (!FloatingDecimal.isPatchInstalled()) {
                log.warn("FloatingDecimal patch not installed. Parallelism will be diminished when parsing/formatting doubles");
            }

            injector.getInstance(PluginManager.class).loadPlugins();

            injector.getInstance(CatalogManager.class).loadCatalogs();

            injector.getInstance(Announcer.class).start();

            log.info("======== SERVER STARTED ========");

            installCodeCacheGcTrigger();
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }

    protected Iterable<? extends Module> getAdditionalModules()
    {
        return ImmutableList.of();
    }
}
