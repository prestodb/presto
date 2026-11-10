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
package com.facebook.presto.plugin.prometheus;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.http.server.TheServlet;
import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import jakarta.servlet.Servlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Map;

import static com.facebook.presto.plugin.prometheus.PrometheusClient.METRICS_ENDPOINT;

public class PrometheusHttpServer
{
    public static final String BEARER_TOKEN = "test-bearer-token";

    private final LifeCycleManager lifeCycleManager;
    private final URI baseUri;

    public PrometheusHttpServer()
    {
        this(null);
    }

    public PrometheusHttpServer(String requiredBearerToken)
    {
        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                new PrometheusHttpServerModule(requiredBearerToken));

        Injector injector = app
                .noStrictConfig()
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        baseUri = injector.getInstance(TestingHttpServer.class).getBaseUrl();
    }

    public void stop()
    {
        lifeCycleManager.stop();
    }

    public URI resolve(String s)
    {
        return baseUri.resolve(s);
    }

    private static class PrometheusHttpServerModule
            implements Module
    {
        private final String requiredBearerToken;

        PrometheusHttpServerModule(String requiredBearerToken)
        {
            this.requiredBearerToken = requiredBearerToken;
        }

        @Override
        public void configure(Binder binder)
        {
            binder.bind(new TypeLiteral<Map<String, String>>() {}).annotatedWith(TheServlet.class).toInstance(ImmutableMap.of());
            binder.bind(Servlet.class).annotatedWith(TheServlet.class).toInstance(new PrometheusHttpServlet(requiredBearerToken));
        }
    }

    private static class PrometheusHttpServlet
            extends HttpServlet
    {
        private final String requiredBearerToken;

        PrometheusHttpServlet(String requiredBearerToken)
        {
            this.requiredBearerToken = requiredBearerToken;
        }

        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException
        {
            if (requiredBearerToken != null) {
                String authHeader = request.getHeader("Authorization");
                if (!("Bearer " + requiredBearerToken).equals(authHeader)) {
                    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                    response.getWriter().write("Unauthorized");
                    return;
                }
            }
            String pathInfo = request.getPathInfo();
            if (pathInfo == null) {
                pathInfo = "/";
            }
            URL dataUrl;
            // allow for special response on Prometheus metrics endpoint
            if (pathInfo.contains(METRICS_ENDPOINT)) {
                dataUrl = Resources.getResource(getClass(), pathInfo.split(METRICS_ENDPOINT)[0]);
            }
            else {
                dataUrl = Resources.getResource(getClass(), pathInfo);
            }
            Resources.asByteSource(dataUrl).copyTo(response.getOutputStream());
        }
    }
}
