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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestNativeTpchQueriesWithBinarySerialization
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        // No need to create tables - TPCH connector provides them
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Create a query runner with binary serialization enabled for TPCH connector
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setExtraProperties(ImmutableMap.<String, String>builder()
                        .put("use-connector-provided-serialization-codecs", "true")
                        .build())
                .setExtraCoordinatorProperties(ImmutableMap.<String, String>builder()
                        .put("use-connector-provided-serialization-codecs", "true")
                        .build())
                .build();
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(getSession())
                .setNodeCount(1)
                .build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpchstandard", "tpch",
                ImmutableMap.of("tpch.column-naming", "STANDARD"));
        return queryRunner;
    }

    @Override
    protected Session getSession()
    {
        // Override the session to use TPCH connector catalog and schema by default
        return testSessionBuilder()
                .setCatalog("tpchstandard")
                .setSchema("tiny")
                .build();
    }

    @Test
    public void testTpchConnectorDirectQuery()
    {
        assertQuery("SELECT * FROM tpchstandard.tiny.nation");
        assertQuery("SELECT * FROM tpchstandard.tiny.region");
    }

    @Test
    public void testTpchConnectorFilterPushdown()
    {
        assertQuery("SELECT * FROM tpchstandard.tiny.nation WHERE n_nationkey = 1");
        // Select only columns that match between Java and C++ TPCH implementations
        // Avoiding c_address which has different random string generation
        assertQuery("SELECT c_custkey, c_name, c_nationkey, c_phone, ROUND(c_acctbal, 2), c_mktsegment, c_comment " +
                    "FROM tpchstandard.tiny.customer WHERE c_custkey < 10");
    }

    @Test
    public void testTpchConnectorJoin()
    {
        assertQuery("SELECT n.n_name, r.r_name " +
                "FROM tpchstandard.tiny.nation n " +
                "JOIN tpchstandard.tiny.region r ON n.n_regionkey = r.r_regionkey");
    }

    @Test
    public void testTpchConnectorAggregation()
    {
        assertQuery("SELECT n_regionkey, COUNT(*) " +
                "FROM tpchstandard.tiny.nation " +
                "GROUP BY n_regionkey");

        assertQuery("SELECT r_name, COUNT(*) as nation_count " +
                "FROM tpchstandard.tiny.nation n " +
                "JOIN tpchstandard.tiny.region r ON n.n_regionkey = r.r_regionkey " +
                "GROUP BY r_name " +
                "ORDER BY nation_count DESC");
    }

    @Test
    public void testTpchConnectorComplexQuery()
    {
        assertQuerySucceeds("SELECT " +
                "    l.l_orderkey, " +
                "    SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue, " +
                "    o.o_orderdate, " +
                "    o.o_shippriority " +
                "FROM " +
                "    tpchstandard.tiny.customer c, " +
                "    tpchstandard.tiny.orders o, " +
                "    tpchstandard.tiny.lineitem l " +
                "WHERE " +
                "    c.c_mktsegment = 'BUILDING' " +
                "    AND c.c_custkey = o.o_custkey " +
                "    AND l.l_orderkey = o.o_orderkey " +
                "    AND o.o_orderdate < DATE '1995-03-15' " +
                "    AND l.l_shipdate > DATE '1995-03-15' " +
                "GROUP BY " +
                "    l.l_orderkey, " +
                "    o.o_orderdate, " +
                "    o.o_shippriority " +
                "ORDER BY " +
                "    revenue DESC, " +
                "    o.o_orderdate " +
                "LIMIT 10");
    }

    @Test
    public void testDelimitedIdentifiers()
    {
        assertQuery("SELECT \"c_custkey\", \"c_name\" FROM tpchstandard.tiny.customer WHERE \"c_custkey\" < 10");
    }
}
