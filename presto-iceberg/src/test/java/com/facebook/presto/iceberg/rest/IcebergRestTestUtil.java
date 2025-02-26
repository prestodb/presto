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
package com.facebook.presto.iceberg.rest;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.server.TheServlet;
import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.IcebergRestCatalogServlet;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTSessionCatalog;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.iceberg.IcebergDistributedTestBase.getHdfsEnvironment;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;

public class IcebergRestTestUtil
{
    public static final ConnectorSession SESSION = new TestingConnectorSession(ImmutableList.of());

    private IcebergRestTestUtil()
    {
    }

    public static Map<String, String> restConnectorProperties(String serverUri)
    {
        return ImmutableMap.of("iceberg.rest.uri", serverUri);
    }

    public static TestingHttpServer getRestServer(String location)
    {
        JdbcCatalog backingCatalog = new JdbcCatalog();
        HdfsEnvironment hdfsEnvironment = getHdfsEnvironment(new HiveClientConfig(), new MetastoreClientConfig(), new HiveS3Config());
        backingCatalog.setConf(hdfsEnvironment.getConfiguration(new HdfsContext(SESSION), new Path(location)));

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(URI, "jdbc:h2:mem:test_" + System.nanoTime() + "_" + ThreadLocalRandom.current().nextInt())
                .put(WAREHOUSE_LOCATION, location)
                .put("jdbc.username", "user")
                .put("jdbc.password", "password")
                .put("jdbc.schema-version", "V1")
                .build();
        backingCatalog.initialize("rest_jdbc_backend", properties);

        DelegateRestSessionCatalog delegate = new DelegateRestSessionCatalog(new RESTCatalogAdapter(backingCatalog), backingCatalog);
        return delegate.getServerInstance();
    }

    public static class DelegateRestSessionCatalog
            extends RESTSessionCatalog
    {
        public RESTCatalogAdapter adapter;
        private final Catalog delegate;

        public DelegateRestSessionCatalog(RESTCatalogAdapter adapter, Catalog delegate)
        {
            super(properties -> adapter, null);
            this.adapter = requireNonNull(adapter, "adapter is null");
            this.delegate = requireNonNull(delegate, "delegate catalog is null");
        }

        @Override
        public void close()
                throws IOException
        {
            super.close();
            adapter.close();

            if (delegate instanceof Closeable) {
                ((Closeable) delegate).close();
            }
        }

        public TestingHttpServer getServerInstance()
        {
            Bootstrap app = new Bootstrap(
                    new TestingHttpServerModule(),
                    new RestHttpServerModule());

            Injector injector = app
                    .doNotInitializeLogging()
                    .initialize();

            return injector.getInstance(TestingHttpServer.class);
        }

        private class RestHttpServerModule
                implements Module
        {
            @Override
            public void configure(Binder binder)
            {
                binder.bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(TheServlet.class).toInstance(ImmutableMap.of());
                binder.bind(javax.servlet.Servlet.class).annotatedWith(TheServlet.class).toInstance(new IcebergRestCatalogServlet(adapter));
                binder.bind(NodeInfo.class).toInstance(new NodeInfo("test"));
            }
        }
    }
}
