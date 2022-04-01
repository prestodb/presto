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

import com.facebook.presto.Session;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.sql.planner.planPrinter.IOPlanPrinter;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.tpch.IndexedTpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.emptySet;
import static org.testng.Assert.assertEquals;

public class TestDistributedQueriesIndexed
        extends AbstractTestIndexedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch_indexed")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        // set spill path so we can enable spill by session property
        ImmutableMap<String, String> extraProperties = ImmutableMap.of(
                "experimental.spiller-spill-path",
                Paths.get(System.getProperty("java.io.tmpdir"), "presto", "spills").toString());
        DistributedQueryRunner queryRunner = new DistributedQueryRunner.Builder(session)
                .setNodeCount(3)
                .setExtraProperties(extraProperties)
                .build();

        queryRunner.installPlugin(new IndexedTpchPlugin(INDEX_SPEC));
        queryRunner.createCatalog("tpch_indexed", "tpch_indexed");
        return queryRunner;
    }

    @Test
    public void testExplainIOIndexJoin()
    {
        String query =
                "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 8 = 0) l\n" +
                        "JOIN orders o\n" +
                        "  ON l.orderkey = o.orderkey";
        MaterializedResult result = computeActual("EXPLAIN (TYPE IO, FORMAT JSON) " + query);
        IOPlanPrinter.IOPlan.TableColumnInfo lineitem = new IOPlanPrinter.IOPlan.TableColumnInfo(
                new CatalogSchemaTableName("tpch_indexed", "sf0.01", "lineitem"),
                emptySet());

        IOPlanPrinter.IOPlan.TableColumnInfo orders = new IOPlanPrinter.IOPlan.TableColumnInfo(
                new CatalogSchemaTableName("tpch_indexed", "sf0.01", "orders"),
                emptySet());

        assertEquals(
                jsonCodec(IOPlanPrinter.IOPlan.class).fromJson((String) getOnlyElement(result.getOnlyColumnAsSet())),
                new IOPlanPrinter.IOPlan(ImmutableSet.of(lineitem, orders), Optional.empty()));
    }
}
