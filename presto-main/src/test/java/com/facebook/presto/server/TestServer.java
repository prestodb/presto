package com.facebook.presto.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import io.airlift.configuration.ConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.event.client.InMemoryEventModule;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxHttpModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.testing.FileUtils;
import io.airlift.tracetoken.TraceTokenModule;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import java.io.File;
import java.net.URI;
import java.util.Map;

import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static javax.ws.rs.core.Response.Status.OK;
import static org.testng.Assert.assertEquals;

public class TestServer
{
    private File baseDataDir;
    private TestingHttpServer server;
    private HttpClient client;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        baseDataDir = Files.createTempDir();

        Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .put("storage-manager.data-directory", baseDataDir.getPath())
                .put("presto-metastore.db.type", "h2")
                .put("presto-metastore.db.filename", new File(baseDataDir, "db/MetaStore").getPath())
                .build();

        // TODO: wrap all this stuff in a TestBootstrap class
        Injector serverInjector = createTestInjector(serverProperties,
                new TestingNodeModule(),
                new TestingDiscoveryModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new MBeanModule(),
                new JmxModule(),
                new JmxHttpModule(),
                new LogJmxModule(),
                new InMemoryEventModule(),
                new TraceTokenModule(),
                new ServerMainModule());

        server = serverInjector.getInstance(TestingHttpServer.class);
        server.start();

        client = new ApacheHttpClient();
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        if (server != null) {
            server.stop();
        }
        FileUtils.deleteRecursively(baseDataDir);
    }

    @Test
    public void testServerStarts()
            throws Exception
    {
        StatusResponseHandler.StatusResponse response = client.execute(
                prepareGet().setUri(uriFor("/v1/jmx/mbean")).build(),
                createStatusResponseHandler());

        assertEquals(response.getStatusCode(), OK.getStatusCode());
    }

    private URI uriFor(String path)
    {
        return server.getBaseUrl().resolve(path);
    }

    private static Injector createTestInjector(Map<String, String> properties, Module... modules)
    {
        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        for (Module module : modules) {
            if (module instanceof ConfigurationAwareModule) {
                ((ConfigurationAwareModule) module).setConfigurationFactory(configurationFactory);
            }
        }
        ImmutableList.Builder<Module> moduleList = ImmutableList.builder();
        moduleList.add(new ConfigurationModule(configurationFactory));
        moduleList.add(modules);
        return Guice.createInjector(Stage.DEVELOPMENT, moduleList.build());
    }
}
