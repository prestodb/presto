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

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.discovery.server.EmbeddedDiscoveryModule;
import com.facebook.presto.dispatcher.NoopQueryManager;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.execution.resourceGroups.NoOpResourceGroupManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.failureDetector.FailureDetectorModule;
import com.facebook.presto.transaction.NoOpTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.http.server.HttpServerBinder.httpServerBinder;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.server.smile.SmileCodecBinder.smileCodecBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ResourceManagerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        httpServerBinder(binder).bindResource("/ui", "webapp").withWelcomeFile("index.html");
        httpServerBinder(binder).bindResource("/tableau", "webapp/tableau");

        // discovery server
        install(installModuleIf(EmbeddedDiscoveryConfig.class, EmbeddedDiscoveryConfig::isEnabled, new EmbeddedDiscoveryModule()));

        // presto coordinator announcement
        discoveryBinder(binder).bindHttpAnnouncement("presto-resource-manager");

        // statement resource
        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);

        // resource for serving static content
        jaxrsBinder(binder).bind(WebUiResource.class);

        // failure detector
        binder.install(new FailureDetectorModule());
        jaxrsBinder(binder).bind(NodeResource.class);
        jaxrsBinder(binder).bind(WorkerResource.class);
        httpClientBinder(binder).bindHttpClient("workerInfo", ForWorkerInfo.class);

        // TODO: remove query manager from this module
        binder.bind(QueryManager.class).to(NoopQueryManager.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(ResourceGroupStateInfoResource.class);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryManager.class).withGeneratedName();
        binder.bind(QueryPreparer.class).in(Scopes.SINGLETON);
        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);

        binder.bind(ResourceGroupManager.class).to(NoOpResourceGroupManager.class);

        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        smileCodecBinder(binder).bindSmileCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(BasicQueryInfo.class);
        smileCodecBinder(binder).bindSmileCodec(BasicQueryInfo.class);

        binder.bind(TransactionManager.class).to(NoOpTransactionManager.class);
    }

    @Provides
    @Singleton
    public static ResourceGroupManager<?> getResourceGroupManager(@SuppressWarnings("rawtypes") ResourceGroupManager manager)
    {
        return manager;
    }
}
