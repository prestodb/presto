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
import com.facebook.presto.router.cluster.ClusterManager;
import com.facebook.presto.router.cluster.ClusterStatusResource;
import com.facebook.presto.router.cluster.ClusterStatusTracker;
import com.facebook.presto.router.cluster.ForClusterInfoTracker;
import com.facebook.presto.router.cluster.ForQueryInfoTracker;
import com.facebook.presto.router.cluster.RemoteInfoFactory;
import com.facebook.presto.router.predictor.ForQueryCpuPredictor;
import com.facebook.presto.router.predictor.ForQueryMemoryPredictor;
import com.facebook.presto.router.predictor.PredictorManager;
import com.facebook.presto.router.predictor.RemoteQueryFactory;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.units.Duration;

import java.lang.annotation.Annotation;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.http.server.HttpServerBinder.httpServerBinder;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RouterModule
        extends AbstractConfigurationAwareModule
{
    private static final int IDLE_TIMEOUT_SECOND = 30;
    private static final int REQUEST_TIMEOUT_SECOND = 10;
    private static final int PREDICTOR_REQUEST_TIMEOUT_SECOND = 2;

    private static final String QUERY_TRACKER = "query-tracker";
    private static final String QUERY_PREDICTOR = "query-predictor";
    private static final String UI_PATH = "/ui";
    private static final String ROUTER_UI = "router_ui";
    private static final String INDEX_HTML = "index.html";

    @Override
    protected void setup(Binder binder)
    {
        httpServerBinder(binder).bindResource(UI_PATH, ROUTER_UI).withWelcomeFile(INDEX_HTML);
        configBinder(binder).bindConfig(RouterConfig.class);

        binder.bind(ClusterManager.class).in(Scopes.SINGLETON);
        binder.bind(RemoteInfoFactory.class).in(Scopes.SINGLETON);

        bindHttpClient(binder, QUERY_TRACKER, ForQueryInfoTracker.class, IDLE_TIMEOUT_SECOND, REQUEST_TIMEOUT_SECOND);
        bindHttpClient(binder, QUERY_TRACKER, ForClusterInfoTracker.class, IDLE_TIMEOUT_SECOND, REQUEST_TIMEOUT_SECOND);

        binder.bind(ClusterStatusTracker.class).in(Scopes.SINGLETON);

        binder.bind(PredictorManager.class).in(Scopes.SINGLETON);
        binder.bind(RemoteQueryFactory.class).in(Scopes.SINGLETON);

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
