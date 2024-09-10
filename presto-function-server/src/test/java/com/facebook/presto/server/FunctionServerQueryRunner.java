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
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.server.PrestoSystemRequirements.verifyJvmRequirements;
import static com.facebook.presto.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;

public class FunctionServerQueryRunner
{
    private FunctionServerQueryRunner()
    {
    }

    public static DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        TestingFunctionServer functionServer = new TestingFunctionServer(ImmutableMap.of("http-server.http.port", "8082"));

        DistributedQueryRunner runner = TpchQueryRunnerBuilder.builder()
                .setExtraProperties(
                        ImmutableMap.of(
                                "http-server.http.port", "8080",
                                "list-built-in-functions-only", "false"))
                .build();
        runner.installPlugin(new FunctionNamespaceManagerPlugin());
        runner.loadFunctionNamespaceManager(
                RestBasedFunctionNamespaceManagerFactory.NAME,
                "rest",
                ImmutableMap.of(
                        "supported-function-languages", "JAVA",
                        "function-implementation-type", "REST",
                        "rest-based-function-manager.rest.url", "http://localhost:8082"));

        Thread.sleep(5000);
        Logger log = Logger.get(TpchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", runner.getCoordinator().getBaseUrl());
        return runner;
    }

    private static class TestingFunctionServer
    {
        public TestingFunctionServer(Map<String, String> requiredConfigurationProperties)
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
                    .setRequiredConfigurationProperties(requiredConfigurationProperties)
                    .initialize();

            HttpServerInfo serverInfo = injector.getInstance(HttpServerInfo.class);
            log.info("======== REMOTE FUNCTION SERVER STARTED at: " + serverInfo.getHttpUri() + " =========");
        }
    }
}
