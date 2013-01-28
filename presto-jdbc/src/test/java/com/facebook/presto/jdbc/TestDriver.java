package com.facebook.presto.jdbc;

import com.facebook.presto.hive.HiveClientModule;
import com.facebook.presto.server.ServerMainModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.net.HostAndPort;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import io.airlift.configuration.ConfigurationAwareModule;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.event.client.InMemoryEventModule;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDriver
{
    private File baseDataDir;
    private TestingHttpServer server;
    private HostAndPort address;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        // TODO: extract all this into a TestingServer class and unify with TestServer
        baseDataDir = Files.createTempDir();

        Map<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .put("storage-manager.data-directory", baseDataDir.getPath())
                .put("presto-metastore.db.type", "h2")
                .put("presto-metastore.db.filename", new File(baseDataDir, "db/MetaStore").getPath())
                .build();

        // TODO: wrap all this stuff in a TestBootstrap class
        Injector injector = createTestInjector(serverProperties,
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
                new HiveClientModule(),
                new ServerMainModule());

        injector.getInstance(Announcer.class).start();

        server = injector.getInstance(TestingHttpServer.class);
        server.start();

        URI uri = server.getBaseUrl();
        address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
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
    public void testDriverManager()
            throws Exception
    {
        String url = format("jdbc:presto://%s/", address);
        try (Connection connection = DriverManager.getConnection(url)) {
            try (ResultSet tableTypes = connection.getMetaData().getTableTypes()) {
                assertRowCount(tableTypes, 1);
            }

            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("select 123 x, 'foo' y from dual")) {
                    ResultSetMetaData metadata = rs.getMetaData();

                    assertEquals(metadata.getColumnCount(), 2);

                    assertEquals(metadata.getColumnLabel(1), "x");
                    assertEquals(metadata.getColumnType(1), Types.BIGINT);

                    assertEquals(metadata.getColumnLabel(2), "y");
                    assertEquals(metadata.getColumnType(2), Types.LONGNVARCHAR);

                    assertTrue(rs.next());
                    assertEquals(rs.getLong(1), 123);
                    assertEquals(rs.getLong("x"), 123);
                    assertEquals(rs.getString(2), "foo");
                    assertEquals(rs.getString("y"), "foo");

                    assertFalse(rs.next());
                }
            }
        }
    }

    private static void assertRowCount(ResultSet rs, int expected)
            throws SQLException
    {
        int actual = 0;
        while (rs.next()) {
            actual++;
        }
        assertEquals(actual, expected);
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
