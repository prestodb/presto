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
package com.facebook.presto.router;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.ClientRequestFilterManager;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.router.cluster.ClusterManager;
import com.facebook.presto.router.cluster.ClusterManager.ClusterStatusTracker;
import com.facebook.presto.router.cluster.ClusterStatusResource;
import com.facebook.presto.router.cluster.ForClusterInfoTracker;
import com.facebook.presto.router.cluster.ForClusterManager;
import com.facebook.presto.router.cluster.ForQueryInfoTracker;
import com.facebook.presto.router.cluster.RemoteInfoFactory;
import com.facebook.presto.router.cluster.RemoteStateConfig;
import com.facebook.presto.router.predictor.ForQueryCpuPredictor;
import com.facebook.presto.router.predictor.ForQueryMemoryPredictor;
import com.facebook.presto.router.predictor.PredictorManager;
import com.facebook.presto.router.predictor.RemoteQueryFactory;
import com.facebook.presto.router.scheduler.CustomSchedulerManager;
import com.facebook.presto.server.PluginManagerConfig;
import com.facebook.presto.server.ServerConfig;
import com.facebook.presto.server.WebUiResource;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import java.lang.annotation.Annotation;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.facebook.presto.router.cluster.ClusterManager.ClusterStatusTracker;
import static com.facebook.presto.server.CoordinatorModule.webUIBinder;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RouterModule
        extends AbstractConfigurationAwareModule
{
    private static final int IDLE_TIMEOUT_SECOND = 30;
    private static final int REQUEST_TIMEOUT_SECOND = 10;
    private static final int PREDICTOR_REQUEST_TIMEOUT_SECOND = 2;

    private static final String QUERY_TRACKER = "query-tracker";
    private static final String QUERY_PREDICTOR = "query-predictor";
    private static final String INDEX_HTML = "index.html";
    private final Optional<CustomSchedulerManager> customSchedulerManager;

    public RouterModule(Optional<CustomSchedulerManager> customSchedulerManager)
    {
        this.customSchedulerManager = customSchedulerManager;
    }

    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);

        binder.bind(ClientRequestFilterManager.class).in(Scopes.SINGLETON);
        binder.bind(RouterPluginManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(RouterConfig.class);

        webUIBinder(binder, "/ui", "webapp-router").withWelcomeFile(INDEX_HTML);
        webUIBinder(binder, "/ui/vendor", "webapp/vendor");
        webUIBinder(binder, "/ui/assets", "webapp/assets");

        if (customSchedulerManager.isPresent()) {
            binder.bind(CustomSchedulerManager.class).toInstance(customSchedulerManager.get());
        }
        else {
            binder.bind(CustomSchedulerManager.class).in(Scopes.SINGLETON);
        }

        configBinder(binder).bindConfig(RemoteStateConfig.class);
        binder.bind(ScheduledExecutorService.class).annotatedWith(ForClusterManager.class).toInstance(newSingleThreadScheduledExecutor(threadsNamed("cluster-config")));

        // resource for serving static content
        jaxrsBinder(binder).bind(WebUiResource.class);

        binder.bind(ClusterManager.class).in(Scopes.SINGLETON);
        binder.bind(RemoteInfoFactory.class).in(Scopes.SINGLETON);

        bindHttpClient(binder, QUERY_TRACKER, ForQueryInfoTracker.class, IDLE_TIMEOUT_SECOND, REQUEST_TIMEOUT_SECOND);
        bindHttpClient(binder, QUERY_TRACKER, ForClusterInfoTracker.class, IDLE_TIMEOUT_SECOND, REQUEST_TIMEOUT_SECOND);

        //Determine the NodeVersion
        NodeVersion nodeVersion = new NodeVersion(serverConfig.getPrestoVersion());
        binder.bind(NodeVersion.class).toInstance(nodeVersion);

        binder.bind(ClusterStatusTracker.class).in(Scopes.SINGLETON);

        binder.bind(PredictorManager.class).in(Scopes.SINGLETON);
        binder.bind(RemoteQueryFactory.class).in(Scopes.SINGLETON);

        binder.bind(RouterPluginManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PluginManagerConfig.class);

        bindHttpClient(binder, QUERY_PREDICTOR, ForQueryCpuPredictor.class, IDLE_TIMEOUT_SECOND, PREDICTOR_REQUEST_TIMEOUT_SECOND);
        bindHttpClient(binder, QUERY_PREDICTOR, ForQueryMemoryPredictor.class, IDLE_TIMEOUT_SECOND, PREDICTOR_REQUEST_TIMEOUT_SECOND);

        jaxrsBinder(binder).bind(RouterResource.class);
        jaxrsBinder(binder).bind(ClusterStatusResource.class);
    }

    private void bindHttpClient(Binder binder, String name, Class<? extends Annotation> annotation, int idleTimeout, int requestTimeout)
    {
        httpClientBinder(binder).bindHttpClient(name, annotation).withConfigDefaults(config -> {
            config.setIdleTimeout(new Duration(idleTimeout, SECONDS));
            config.setRequestTimeout(new Duration(requestTimeout, SECONDS));
        });
    }
}
