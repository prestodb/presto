package com.facebook.presto.server;

import com.facebook.presto.failureDetector.FailureDetectorModule;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.NodeManager;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceSelectorManager;
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

public class Main implements Runnable
{
    public static void main(String[] args)
    {
        new Main().run();
    }

    @Override
    public void run()
    {
        Logger log = Logger.get(Main.class);

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
                new FailureDetectorModule(),
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

            injector.getInstance(ServiceSelectorManager.class).attemptRefresh();
            injector.getInstance(NodeManager.class).refreshNodes(true);

            log.info("======== SERVER STARTED ========");
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
