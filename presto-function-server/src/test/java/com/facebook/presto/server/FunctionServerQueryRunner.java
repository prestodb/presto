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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.functionNamespace.FunctionNamespaceManagerPlugin;
import com.facebook.presto.functionNamespace.rest.RestBasedFunctionNamespaceManagerFactory;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.lang.String.format;

public class FunctionServerQueryRunner
{
    private FunctionServerQueryRunner()
    {
    }

    public static DistributedQueryRunner createQueryRunner(int functionServerPort, Map<String, String> queryRunnerExtraProperties)
            throws Exception
    {
        DistributedQueryRunner runner = TpchQueryRunnerBuilder.builder()
                .setExtraProperties(
                        queryRunnerExtraProperties)
                .build();
        runner.installPlugin(new FunctionNamespaceManagerPlugin());
        runner.loadFunctionNamespaceManager(
                RestBasedFunctionNamespaceManagerFactory.NAME,
                "rest",
                ImmutableMap.of(
                        "supported-function-languages", "JAVA",
                        "function-implementation-type", "REST",
                        "rest-based-function-manager.rest.url", format("http://localhost:%s", functionServerPort)));

        Thread.sleep(5000);
        Logger log = Logger.get(TpchQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", runner.getCoordinator().getBaseUrl());
        return runner;
    }

    public static void main(String[] args)
            throws Exception
    {
        int functionServerPort = 8082;
        TestingFunctionServer functionServer = new TestingFunctionServer(functionServerPort);
        createQueryRunner(
                functionServerPort,
                ImmutableMap.of("http-server.http.port", "8080", "list-built-in-functions-only", "false"));
    }
}
