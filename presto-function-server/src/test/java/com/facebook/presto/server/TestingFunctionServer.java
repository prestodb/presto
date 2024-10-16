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

import java.util.List;

import static com.facebook.presto.server.PrestoSystemRequirements.verifyJvmRequirements;
import static com.facebook.presto.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;

public class TestingFunctionServer
{
    private final FunctionPluginManager functionPluginManager;

    public TestingFunctionServer(int port)
    {
        verifyJvmRequirements();
        verifySystemTimeIsReasonable();

        Logger log = Logger.get(FunctionServer.class);

        List<Module> modules = ImmutableList.of(
                new FunctionServerModule(),
                new HttpServerModule(),
                new JaxrsModule());

        Bootstrap app = new Bootstrap(modules);
        Injector injector = app
                .setRequiredConfigurationProperties(ImmutableMap.of("http-server.http.port", Integer.toString(port)))
                .initialize();

        functionPluginManager = injector.getInstance(FunctionPluginManager.class);
        HttpServerInfo serverInfo = injector.getInstance(HttpServerInfo.class);
        log.info("======== REMOTE FUNCTION SERVER STARTED at: " + serverInfo.getHttpUri() + " =========");
    }

    public void installPlugin(Plugin plugin)
    {
        functionPluginManager.installPlugin(plugin);
    }
}
