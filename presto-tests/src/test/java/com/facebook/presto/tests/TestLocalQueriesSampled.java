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
package com.facebook.presto.tests;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.testing.SampledTpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestLocalQueriesSampled
        extends AbstractTestSampledQueries
{
    private static final String TPCH_SAMPLED_SCHEMA = "tpch_sampled";

    public TestLocalQueriesSampled()
    {
        super(createLocalQueryRunner(), createDefaultSampledSession());
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        ((LocalQueryRunner) queryRunner).getExecutor().shutdownNow();
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        ConnectorSession defaultSession = new ConnectorSession("user", "test", "tpch", TpchMetadata.TINY_SCHEMA_NAME, UTC_KEY, ENGLISH, null, null);
        LocalQueryRunner queryRunner = new LocalQueryRunner(defaultSession, newCachedThreadPool(daemonThreadsNamed("test-sampled")));
        queryRunner.createCatalog(defaultSession.getCatalog(), new SampledTpchConnectorFactory(queryRunner.getNodeManager(), 1, 1), ImmutableMap.<String, String>of());
        queryRunner.createCatalog(TPCH_SAMPLED_SCHEMA, new SampledTpchConnectorFactory(queryRunner.getNodeManager(), 1, 2), ImmutableMap.<String, String>of());
        queryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);
        return queryRunner;
    }

    private static ConnectorSession createDefaultSampledSession()
    {
        return new ConnectorSession("user", "test", TPCH_SAMPLED_SCHEMA, TpchMetadata.TINY_SCHEMA_NAME, UTC_KEY, ENGLISH, null, null);
    }
}
