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

import com.datastax.driver.core.Cluster;
import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import static com.facebook.presto.cassandra.CassandraTestingUtils.createOrReplaceKeyspace;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public final class CassandraQueryRunner
{
    private CassandraQueryRunner()
    {
    }

    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createCassandraQueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return createCassandraQueryRunner(ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createCassandraQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();

        try (Cluster cluster = CassandraTestingUtils.getCluster();
                com.datastax.driver.core.Session session = cluster.connect()) {
            createOrReplaceKeyspace(session, "tpch");
        }

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createSession(), 4);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new CassandraPlugin());
        queryRunner.createCatalog("cassandra", "cassandra", ImmutableMap.of(
                "cassandra.contact-points", "localhost",
                "cassandra.native-protocol-port", "9142",
                "cassandra.allow-drop-table", "true"));

        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);

        return queryRunner;
    }

    public static Session createSession()
    {
        return createCassandraSession(TPCH_SCHEMA);
    }

    public static Session createCassandraSession(String schema)
    {
        return testSessionBuilder()
                .setCatalog("cassandra")
                .setSchema(schema)
                .build();
    }
}
