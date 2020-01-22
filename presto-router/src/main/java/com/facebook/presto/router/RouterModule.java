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

import com.facebook.presto.router.cluster.ClusterManager;
import com.facebook.presto.router.cluster.ClusterStatusResource;
import com.facebook.presto.router.cluster.ClusterStatusTracker;
import com.facebook.presto.router.cluster.ForClusterInfoTracker;
import com.facebook.presto.router.cluster.ForQueryInfoTracker;
import com.facebook.presto.router.cluster.RemoteInfoFactory;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RouterModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        httpServerBinder(binder).bindResource("/ui", "router_ui").withWelcomeFile("index.html");
        configBinder(binder).bindConfig(RouterConfig.class);

        binder.bind(ClusterManager.class).in(Scopes.SINGLETON);
        binder.bind(RemoteInfoFactory.class).in(Scopes.SINGLETON);

        httpClientBinder(binder).bindHttpClient("query-tracker", ForQueryInfoTracker.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });
        httpClientBinder(binder).bindHttpClient("query-tracker", ForClusterInfoTracker.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });
        binder.bind(ClusterStatusTracker.class).in(Scopes.SINGLETON);

        jaxrsBinder(binder).bind(RouterResource.class);
        jaxrsBinder(binder).bind(ClusterStatusResource.class);
    }
}
