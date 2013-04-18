package com.facebook.presto.server;

import com.facebook.presto.failureDetector.FailureDetectorModule;
import com.facebook.presto.guice.TestingJmxModule;
import com.facebook.presto.metadata.ConnectorMetadata;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.split.ConnectorSplitManager;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchSplitManager;
import com.facebook.presto.util.TestingTpchBlocksProvider;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.event.client.InMemoryEventModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.testing.FileUtils;
import io.airlift.tracetoken.TraceTokenModule;
import org.weakref.jmx.guice.MBeanModule;

import java.io.Closeable;
import java.io.File;
import java.net.URI;
import java.util.Map;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class TestingPrestoServer
        implements Closeable
{
    private final File baseDataDir;
    private final LifeCycleManager lifeCycleManager;
    private final TestingHttpServer server;

    public TestingPrestoServer()
            throws Exception
    {
        baseDataDir = Files.createTempDir();

        Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .put("storage-manager.data-directory", baseDataDir.getPath())
                .put("presto-metastore.db.type", "h2")
                .put("presto-metastore.db.filename", new File(baseDataDir, "db/MetaStore").getPath())
                .build();

        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingDiscoveryModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new TestingJmxModule(),
                new InMemoryEventModule(),
                new TraceTokenModule(),
                new FailureDetectorModule(),
                new ServerMainModule(),
                new Module() {
                    @Override
                    public void configure(Binder binder)
                    {
                        newMapBinder(binder, String.class, ConnectorMetadata.class).addBinding("tpch").to(TpchMetadata.class);
                        newSetBinder(binder, ConnectorSplitManager.class).addBinding().to(TpchSplitManager.class).in(Scopes.SINGLETON);
                        binder.bind(TpchBlocksProvider.class).to(TestingTpchBlocksProvider.class).in(Scopes.SINGLETON);
                        newSetBinder(binder, ConnectorDataStreamProvider.class).addBinding().to(TpchDataStreamProvider.class).in(Scopes.SINGLETON);
                    }
                });

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(serverProperties)
                .initialize();

        injector.getInstance(Announcer.class).start();

        injector.getInstance(NodeManager.class).refreshNodes(true);

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        server = injector.getInstance(TestingHttpServer.class);
    }

    @Override
    public void close()
    {
        try {
            if (lifeCycleManager != null) {
                lifeCycleManager.stop();
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
        finally {
            FileUtils.deleteRecursively(baseDataDir);
        }
    }

    public URI getBaseUrl()
    {
        return server.getBaseUrl();
    }

    public URI resolve(String path)
    {
        return server.getBaseUrl().resolve(path);
    }
}
