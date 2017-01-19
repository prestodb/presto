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

import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.transaction.TransactionManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.AnnouncementHttpServerInfo;
import io.airlift.discovery.server.DynamicAnnouncementResource;
import io.airlift.discovery.server.ServiceResource;
import io.airlift.event.client.EventClient;
import io.airlift.http.server.HttpRequestEvent;
import io.airlift.http.server.HttpServer;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.HttpServerProvider;
import io.airlift.http.server.LocalAnnouncementHttpServerInfo;
import io.airlift.http.server.RequestStats;
import io.airlift.http.server.TheAdminServlet;
import io.airlift.http.server.TheServlet;
import io.airlift.json.JsonCodecFactory;
import io.airlift.node.NodeConfig;
import io.airlift.node.NodeInfo;

import javax.servlet.Filter;

import java.util.UUID;

import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.event.client.EventBinder.eventBinder;
import static io.airlift.http.server.HttpServerBinder.HttpResourceBinding;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CoordinatorUIHttpServerModule
        extends AbstractConfigurationAwareModule
{
    private final Injector injector;

    public CoordinatorUIHttpServerModule(Injector injector)
    {
        this.injector = requireNonNull(injector, "injector is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.disableCircularProxies();

        ServerConfig serverConfig = injector.getInstance(ServerConfig.class);
        HttpServerConfig httpServerConfig = injector.getInstance(HttpServerConfig.class)
                                          .setHttpPort(serverConfig.getUIHttpPort());
        binder.bind(HttpServerConfig.class).toInstance(httpServerConfig);

        binder.bind(HttpServerInfo.class).in(Scopes.SINGLETON);
        binder.bind(EventClient.class).toInstance(injector.getInstance(EventClient.class));
        binder.bind(HttpServer.class).toProvider(HttpServerProvider.class).in(Scopes.SINGLETON);

        Multibinder.newSetBinder(binder, Filter.class, TheServlet.class);
        Multibinder.newSetBinder(binder, Filter.class, TheAdminServlet.class);
        Multibinder.newSetBinder(binder, HttpResourceBinding.class, TheServlet.class);
        newExporter(binder).export(HttpServer.class).withGeneratedName();
        binder.bind(RequestStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RequestStats.class).withGeneratedName();
        eventBinder(binder).bindEventClient(HttpRequestEvent.class);

        NodeConfig nodeConfig = injector.getInstance(NodeConfig.class);
        nodeConfig.setNodeId(UUID.randomUUID().toString());
        binder.bind(NodeInfo.class).toInstance(new NodeInfo(nodeConfig));
        binder.bind(AnnouncementHttpServerInfo.class).to(LocalAnnouncementHttpServerInfo.class).in(Scopes.SINGLETON);
        binder.bind(BlockEncodingSerde.class).toInstance(injector.getInstance(BlockEncodingSerde.class));
        binder.bind(TypeManager.class).toInstance(injector.getInstance(TypeManager.class));
        binder.bind(SqlParser.class).toInstance(injector.getInstance(SqlParser.class));
        binder.bind(TransactionManager.class).toInstance(injector.getInstance(TransactionManager.class));

        binder.bind(ObjectMapper.class).toInstance(injector.getInstance(ObjectMapper.class));
        binder.bind(JsonCodecFactory.class).toInstance(injector.getInstance(JsonCodecFactory.class));

        resourceBinding(binder);
    }

    protected void resourceBinding(Binder binder)
    {
        // bind webapp
        httpServerBinder(binder).bindResource("/", "webapp").withWelcomeFile("index.html");
        // presto coordinator ui announcement
        discoveryBinder(binder).bindHttpAnnouncement("presto-coordinator-ui");
        // accept presto worker's announcement
        jaxrsBinder(binder).bindInstance(injector.getInstance(DynamicAnnouncementResource.class));
        // service info
        jaxrsBinder(binder).bindInstance(injector.getInstance(ServiceResource.class));
        // query execution visualizer
        jaxrsBinder(binder).bindInstance(injector.getInstance(QueryExecutionResource.class));
        // query manager
        jaxrsBinder(binder).bindInstance(injector.getInstance(QueryResource.class));
        // cluster statistics
        jaxrsBinder(binder).bindInstance(injector.getInstance(ClusterStatsResource.class));
        // server info resource
        jaxrsBinder(binder).bindInstance(injector.getInstance(ServerInfoResource.class));
        // server node resource
        jaxrsBinder(binder).bindInstance(injector.getInstance(NodeResource.class));
    }
}
