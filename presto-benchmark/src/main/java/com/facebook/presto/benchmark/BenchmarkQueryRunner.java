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
package com.facebook.presto.benchmark;

import com.facebook.presto.Session;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.Session.SessionBuilder;
import static com.facebook.presto.testing.LocalQueryRunner.queryRunnerWithInitialTransaction;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public final class BenchmarkQueryRunner
{
    private BenchmarkQueryRunner()
    {
    }

    public static LocalQueryRunner createLocalQueryRunnerHashEnabled()
    {
        return createLocalQueryRunner(true);
    }

    public static LocalQueryRunner createLocalQueryRunner()
    {
        return createLocalQueryRunner(false);
    }
    public static LocalQueryRunner createLocalQueryRunner(boolean hashingEnabled)
    {
        SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME);

        if (hashingEnabled) {
            sessionBuilder.setSystemProperties(ImmutableMap.of("optimizer.optimize_hash_generation", "true"));
        }

        Session session = sessionBuilder.build();
        LocalQueryRunner localQueryRunner = queryRunnerWithInitialTransaction(session);

        // add tpch
        NodeManager nodeManager = localQueryRunner.getNodeManager();
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.<String, String>of());

        return localQueryRunner;
    }
}
