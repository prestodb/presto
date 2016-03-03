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
import com.facebook.presto.execution.scheduler.FlatNetworkTopology;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NetworkTopology;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AccessControlModule;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.event.client.HttpEventModule;
import io.airlift.event.client.JsonEventModule;
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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.LEGACY_NETWORK_TOPOLOGY;
import static com.facebook.presto.server.CodeCacheGcTrigger.installCodeCacheGcTrigger;
import static com.facebook.presto.server.ConditionalModule.installModuleIf;
import static com.facebook.presto.server.PrestoSystemRequirements.verifyJvmRequirements;
import static com.facebook.presto.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;
import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.discovery.client.ServiceAnnouncement.ServiceAnnouncementBuilder;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static java.util.Objects.requireNonNull;

public class PrestoServer
        implements Runnable
{
    public static void main(String[] args)
    {
        new PrestoServer().run();
    }

    private final SqlParserOptions sqlParserOptions;

    public PrestoServer()
    {
        this(new SqlParserOptions());
    }

    public PrestoServer(SqlParserOptions sqlParserOptions)
    {
        this.sqlParserOptions = requireNonNull(sqlParserOptions, "sqlParserOptions is null");
    }

    @Override
    public void run()
    {
        verifyJvmRequirements();
        verifySystemTimeIsReasonable();

        Logger log = Logger.get(PrestoServer.class);

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new NodeModule(),
                new DiscoveryModule(),
                new HttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                new MBeanModule(),
                new JmxModule(),
                new JmxHttpModule(),
                new LogJmxModule(),
                new TraceTokenModule(),
                new JsonEventModule(),
                new HttpEventModule(),
                new EmbeddedDiscoveryModule(),
                new ServerSecurityModule(),
                new AccessControlModule(),
                new ServerMainModule(sqlParserOptions),
                new GracefulShutdownModule(),
                installModuleIf(
                        NodeSchedulerConfig.class,
                        config -> LEGACY_NETWORK_TOPOLOGY.equalsIgnoreCase(config.getNetworkTopology()),
                        binder -> binder.bind(NetworkTopology.class).to(LegacyNetworkTopology.class).in(Scopes.SINGLETON)),
                installModuleIf(
                        NodeSchedulerConfig.class,
                        config -> "flat".equalsIgnoreCase(config.getNetworkTopology()),
                        binder -> binder.bind(NetworkTopology.class).to(FlatNetworkTopology.class).in(Scopes.SINGLETON))

        );

        modules.addAll(getAdditionalModules());

        Bootstrap app = new Bootstrap(modules.build());

        try {
            Injector injector = app.strictConfig().initialize();

            injector.getInstance(PluginManager.class).loadPlugins();

            injector.getInstance(CatalogManager.class).loadCatalogs();

            // TODO: remove this huge hack
            updateDatasources(
                    injector.getInstance(Announcer.class),
                    injector.getInstance(Metadata.class),
                    injector.getInstance(ServerConfig.class),
                    injector.getInstance(NodeSchedulerConfig.class));

            injector.getInstance(AccessControlManager.class).loadSystemAccessControl();

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

    private static void updateDatasources(Announcer announcer, Metadata metadata, ServerConfig serverConfig, NodeSchedulerConfig schedulerConfig)
    {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());

        // get existing sources
        String property = nullToEmpty(announcement.getProperties().get("datasources"));
        List<String> values = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property);
        Set<String> datasources = new LinkedHashSet<>(values);

        // automatically build sources if not configured
        if (datasources.isEmpty()) {
            Set<String> catalogs = metadata.getCatalogNames().keySet();
            // if this is a dedicated coordinator, only add jmx
            if (serverConfig.isCoordinator() && !schedulerConfig.isIncludeCoordinator()) {
                if (catalogs.contains("jmx")) {
                    datasources.add("jmx");
                }
            }
            else {
                datasources.addAll(catalogs);
            }
        }

        // build announcement with updated sources
        ServiceAnnouncementBuilder builder = serviceAnnouncement(announcement.getType());
        for (Map.Entry<String, String> entry : announcement.getProperties().entrySet()) {
            if (!entry.getKey().equals("datasources")) {
                builder.addProperty(entry.getKey(), entry.getValue());
            }
        }
        builder.addProperty("datasources", Joiner.on(',').join(datasources));

        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(builder.build());
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }
}
