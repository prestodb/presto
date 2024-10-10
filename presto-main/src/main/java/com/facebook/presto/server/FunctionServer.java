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
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.server.PrestoSystemRequirements.verifyJvmRequirements;
import static com.facebook.presto.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;

public class FunctionServer
        implements Runnable
{
    private static final Logger log = Logger.get(FunctionServer.class);

    public FunctionServer()
    {
    }

    public static void main(String[] args)
    {
        new FunctionServer().run();
    }

    @Override
    public void run()
    {
        verifyJvmRequirements();
        verifySystemTimeIsReasonable();

        Map<String, String> configMap = new HashMap<>();
        configMap.put("http-server.http.enabled", "true");
        configMap.put("http-server.http.port", "8085");

        List<Module> modules = ImmutableList.of(
                new FunctionServerModule(),
                new HttpServerModule(),
                new JaxrsModule());

        try {
            Bootstrap app = new Bootstrap(modules)
                    .setRequiredConfigurationProperties(configMap);  // Set the configuration map
            Injector injector = app.initialize();

            HttpServerInfo serverInfo = injector.getInstance(HttpServerInfo.class);
            String httpUrl = serverInfo.getHttpUri().toString();  // This will give you the HTTP URL
            System.out.println(httpUrl);
            log.info("======== REMOTE FUNCTION SERVER STARTED at: " + serverInfo.getHttpUri() + " =========");

            Thread.currentThread().join();  // Keep the server running
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }
}
