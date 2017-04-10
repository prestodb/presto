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

import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;

import java.util.List;

import static com.facebook.presto.cassandra.CassandraTestingUtils.createKeyspace;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public final class CassandraQueryRunner
{
    private CassandraQueryRunner()
    {
    }

    private static boolean tpchLoaded = false;

    public static synchronized DistributedQueryRunner createCassandraQueryRunner()
            throws Exception
    {
        EmbeddedCassandra.start();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(createCassandraSession("tpch"), 4);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new CassandraPlugin());
        queryRunner.createCatalog("cassandra", "cassandra", ImmutableMap.of(
                "cassandra.contact-points", EmbeddedCassandra.getHost(),
                "cassandra.native-protocol-port", Integer.toString(EmbeddedCassandra.getPort()),
                "cassandra.allow-drop-table", "true"));

        if (!tpchLoaded) {
            createKeyspace(EmbeddedCassandra.getSession(), "tpch");
            List<TpchTable<?>> tables = TpchTable.getTables();
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createCassandraSession("tpch"), tables);
            for (TpchTable table : tables) {
                EmbeddedCassandra.flush("tpch", table.getTableName());
            }
            EmbeddedCassandra.refreshSizeEstimates();
            tpchLoaded = true;
        }

        return queryRunner;
    }

    public static Session createCassandraSession(String schema)
    {
        return testSessionBuilder()
                .setCatalog("cassandra")
                .setSchema(schema)
                .build();
    }
}
