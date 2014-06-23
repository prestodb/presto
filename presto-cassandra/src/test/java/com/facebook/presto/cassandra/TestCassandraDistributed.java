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
package com.facebook.presto.cassandra;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.tpch.testing.SampledTpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.tests.QueryAssertions.copyAllTables;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

@Test(singleThreaded = true)
public class TestCassandraDistributed
        extends AbstractTestDistributedQueries
{
    private static final Logger log = Logger.get("TestQueries");
    private static final String TPCH_SAMPLED_SCHEMA = "tpch_sampled";

    public TestCassandraDistributed()
            throws Exception
    {
        super(createQueryRunner(), createSession(TPCH_SAMPLED_SCHEMA));
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        queryRunner.close();
    }

    private static QueryRunner createQueryRunner()
            throws Exception
    {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        TestCassandraConnector.createOrReplaceKeyspace("tpch");
        TestCassandraConnector.createOrReplaceKeyspace("tpch_sampled");

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession("tpch"), 4);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new SampledTpchPlugin());
        queryRunner.createCatalog("tpch_sampled", "tpch_sampled");

        queryRunner.installPlugin(new CassandraPlugin());
        queryRunner.createCatalog("cassandra", "cassandra", ImmutableMap.of(
                "cassandra.contact-points", "localhost",
                "cassandra.native-protocol-port", "9142",
                "cassandra.allow-drop-table", "true"));

        log.info("Loading data...");
        long startTime = System.nanoTime();
        copyAllTables(queryRunner, "tpch", TpchMetadata.TINY_SCHEMA_NAME, createSession("tpch"));
        copyAllTables(queryRunner, "tpch_sampled", TpchMetadata.TINY_SCHEMA_NAME, createSession(TPCH_SAMPLED_SCHEMA));
        log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

        return queryRunner;
    }

    private static ConnectorSession createSession(String schema)
    {
        return new ConnectorSession("user", "test", "cassandra", schema, UTC_KEY, ENGLISH, null, null);
    }

    @Override
    public void testView()
            throws Exception
    {
        // Cassandra connector currently does not support views
    }

    @Override
    public void testViewMetadata()
            throws Exception
    {
        // Cassandra connector currently does not support views
    }
}
