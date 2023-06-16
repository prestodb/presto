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

import com.facebook.airlift.discovery.client.AnnouncementHttpServerInfo;
import com.facebook.airlift.http.server.Authenticator;
import com.facebook.airlift.http.server.HttpRequestEvent;
import com.facebook.airlift.http.server.HttpServer;
import com.facebook.airlift.http.server.HttpServerBinder;
import com.facebook.airlift.http.server.HttpServerConfig;
import com.facebook.airlift.http.server.HttpServerInfo;
import com.facebook.airlift.http.server.HttpServerProvider;
import com.facebook.airlift.http.server.LocalAnnouncementHttpServerInfo;
import com.facebook.airlift.http.server.RequestStats;
import com.facebook.airlift.http.server.TheAdminServlet;
import com.facebook.airlift.http.server.TheServlet;
import com.facebook.presto.server.security.AuthenticationFilter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.servlet.Filter;
import javax.servlet.Servlet;

import java.util.List;
import java.util.Set;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.event.client.EventBinder.eventBinder;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class HttpServerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.disableCircularProxies();

        binder.bind(HttpServer.class).toProvider(HttpServerProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(HttpServer.class).withGeneratedName();
        binder.bind(HttpServerInfo.class).in(Scopes.SINGLETON);
        binder.bind(RequestStats.class).in(Scopes.SINGLETON);
        newMapBinder(binder, String.class, Servlet.class, TheServlet.class);
        newSetBinder(binder, Filter.class, TheServlet.class);
        newSetBinder(binder, Filter.class, TheAdminServlet.class);
        newSetBinder(binder, HttpServerBinder.HttpResourceBinding.class, TheServlet.class);

        newExporter(binder).export(RequestStats.class).withGeneratedName();

        configBinder(binder).bindConfig(HttpServerConfig.class);

        eventBinder(binder).bindEventClient(HttpRequestEvent.class);

        binder.bind(AnnouncementHttpServerInfo.class).to(LocalAnnouncementHttpServerInfo.class).in(Scopes.SINGLETON);
        newSetBinder(binder, Filter.class, TheServlet.class).addBinding()
                .to(AuthenticationFilter.class).in(Scopes.SINGLETON);
    }

    @Provides
    List<Authenticator> getAuthenticatorList(Set<Authenticator> authenticators)
    {
        return ImmutableList.copyOf(authenticators);
    }
}
