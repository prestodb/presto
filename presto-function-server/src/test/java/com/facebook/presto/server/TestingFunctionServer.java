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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.server.HttpServerInfo;
import com.facebook.airlift.http.server.HttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.server.PrestoSystemRequirements.verifyJvmRequirements;
import static com.facebook.presto.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;

public class TestingFunctionServer
{
    private final FunctionPluginManager functionPluginManager;
    private final Injector injector;
    private final HttpServerInfo serverInfo;

    /**
     * Create a function server with the given configuration properties.
     */
    public TestingFunctionServer(Map<String, String> properties)
    {
        verifyJvmRequirements();
        verifySystemTimeIsReasonable();

        Logger log = Logger.get(FunctionServer.class);

        List<Module> modules = ImmutableList.of(
                new FunctionServerModule(),
                new HttpServerModule(),
                new JaxrsModule());

        Bootstrap app = new Bootstrap(modules);
        injector = app
                .setRequiredConfigurationProperties(new HashMap<>(properties))
                .initialize();

        functionPluginManager = injector.getInstance(FunctionPluginManager.class);
        serverInfo = injector.getInstance(HttpServerInfo.class);
        log.info("======== REMOTE FUNCTION SERVER STARTED at: " + FunctionServer.getServerUri(serverInfo) + " =========");
    }

    public TestingFunctionServer(int port)
    {
        this(ImmutableMap.of("http-server.http.port", Integer.toString(port)));
    }

    public void installPlugin(Plugin plugin)
    {
        functionPluginManager.installPlugin(plugin);
    }

    public String getServerUri()
    {
        return FunctionServer.getServerUri(serverInfo).toString();
    }
}
