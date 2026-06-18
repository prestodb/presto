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
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.KEY_BASED_SAMPLING_ENABLED;
import static com.facebook.presto.SystemSessionProperties.KEY_BASED_SAMPLING_FUNCTION;
import static com.facebook.presto.SystemSessionProperties.KEY_BASED_SAMPLING_PERCENTAGE;
import static com.facebook.presto.SystemSessionProperties.PUSH_DOWN_FILTER_EXPRESSION_EVALUATION_THROUGH_CROSS_JOIN;
import static com.facebook.presto.SystemSessionProperties.REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN;
import static com.facebook.presto.SystemSessionProperties.REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN;

public abstract class AbstractTestSqlInvokedFunctions
        extends AbstractTestQueryFramework
{
    @Test
    public void testArraySplitIntoChunks()
    {
        @Language("SQL") String sql = "select array_split_into_chunks(array[1, 2, 3, 4, 5, 6], 2)";
        assertQuery(sql, "values array[array[1, 2], array[3, 4], array[5, 6]]");

        sql = "select array_split_into_chunks(array[1, 2, 3, 4, 5], 3)";
        assertQuery(sql, "values array[array[1, 2, 3], array[4, 5]]");

        sql = "select array_split_into_chunks(array[1, 2, 3], 5)";
        assertQuery(sql, "values array[array[1, 2, 3]]");

        sql = "select array_split_into_chunks(null, 2)";
        assertQuery(sql, "values null");

        sql = "select array_split_into_chunks(array[1, 2, 3], 0)";
        assertQueryFails(sql, ".*Invalid slice size: 0. Size must be greater than zero.*");

        sql = "select array_split_into_chunks(array[1, 2, 3], -1)";
        assertQueryFails(sql, ".*Invalid slice size: -1. Size must be greater than zero.*");

        sql = "select array_split_into_chunks(array[1, null, 3, null, 5], 2)";
        assertQuery(sql, "values array[array[1, null], array[3, null], array[5]]");

        sql = "select array_split_into_chunks(array['a', 'b', 'c', 'd'], 2)";
        assertQuery(sql, "values array[array['a', 'b'], array['c', 'd']]");

        sql = "select array_split_into_chunks(array[1.1, 2.2, 3.3, 4.4, 5.5], 2)";
        assertQuery(sql, "values array[array[1.1, 2.2], array[3.3, 4.4], array[5.5]]");

        sql = "select array_split_into_chunks(array[null, null, null], 0)";
        assertQueryFails(sql, ".*Invalid slice size: 0. Size must be greater than zero.*");

        sql = "select array_split_into_chunks(array[null, null, null], 2)";
        assertQuery(sql, "values array[array[null, null], array[null]]");

        sql = "select array_split_into_chunks(array[null, 1, 2], 5)";
        assertQuery(sql, "values array[array[null, 1, 2]]");

        sql = "select array_split_into_chunks(array[], 0)";
        assertQueryFails(sql, ".*Invalid slice size: 0. Size must be greater than zero.*");
    }

    @Test
    public void testCrossJoinWithArrayNotContainsCondition()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(PUSH_DOWN_FILTER_EXPRESSION_EVALUATION_THROUGH_CROSS_JOIN, "REWRITTEN_TO_INNER_JOIN")
                .setSystemProperty(REWRITE_CROSS_JOIN_ARRAY_NOT_CONTAINS_TO_ANTI_JOIN, "true")
                .build();

        @Language("SQL") String sql = "with t1 as (select * from (values (array[1, 2, 3])) t(arr)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t2.k, t2.v from t2 where not contains((select t1.arr from t1), t2.k)";
        assertQuery(enableOptimization, sql, "values (4, 'b')");

        sql = "with t1 as (select * from (values (array[1, 2, 3, 3, null])) t(arr)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t2.k, t2.v from t2 where not contains((select t1.arr from t1), t2.k)";
        assertQuery(enableOptimization, sql, "values (4, 'b')");

        sql = "with t1 as (select * from (values (1, 'JAPAN'), (2, 'invalid_nation')) t(k, nation)) " +
                "select t1.k, t1.nation from t1 where not contains((select array_agg(name) from nation), t1.nation)";
        assertQuery(enableOptimization, sql, "values (2, 'invalid_nation')");

        // array is an expression that needs to be pushed down
        sql = "with t1 as (select * from (values (1, 'JAPAN'), (2, 'invalid_nation')) t(k, nation)) " +
                "select t1.k, t1.nation from t1 where not contains(array_distinct((select array_agg(name) from nation)), t1.nation)";
        assertQuery(enableOptimization, sql, "values (2, 'invalid_nation')");

        // check not applicable cases for optimization

        // optimization doesn't apply when there are additional columns on array side
        sql = "with t1 as (select * from (values (array[1, 1, 3], 10)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t1 join t2 on not contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (10, 4, 'b')");

        // optimization doesn't apply for multi-row array tables
        sql = "with t1 as (select * from (values (array[1, 2, 3]), (array[4, 5, 6])) t(arr)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.arr, t2.k, t2.v from t1 join t2 on not contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (array[1,2,3], 4, 'b'), (array[4,5,6], 1, 'a')");

        // we currently don't support the optimization for cases that didn't come from a subquery
        sql = "with t1 as (select * from (values (array[1, 2, 3])) t(arr)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t2.k, t2.v from t1 join t2 on not contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (4, 'b')");

        sql = "with t1 as (select * from (values (array[1, 2, 3])) t(arr)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.arr, t2.k, t2.v from t1 join t2 on not contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (array[1,2,3], 4, 'b')");

        // transform function considered non-deterministic and doesn't get pushed down
        sql = "with t1 as (select * from (values (1, 'JAPAN'), (2, 'invalid_nation')) t(k, nation)) " +
                "select t1.k, t1.nation from t1 where not contains(transform((select array_agg(name) from nation), (x) ->lower(x)), lower(t1.nation))";
        assertQuery(enableOptimization, sql, "values (2, 'invalid_nation')");
    }

    @Test
    public void testDefaultSamplingPercent()
    {
        assertQuery("select key_sampling_percent('abc')", "select 0.56");
    }

    @Test
    public void testKeyBasedSampling()
    {
        String[] queries = {
                "select count(1) from orders join lineitem using(orderkey)",
                "select count(1) from (select custkey, max(orderkey) from orders group by custkey)",
                "select count_if(m >= 1) from (select max(orderkey) over(partition by custkey) m from orders)",
                "select cast(m as bigint) from (select sum(totalprice) over(partition by custkey order by comment) m from orders order by 1 desc limit 1)",
                "select count(1) from lineitem where orderkey in (select orderkey from orders where length(comment) > 7)",
                "select count(1) from lineitem where orderkey not in (select orderkey from orders where length(comment) > 27)",
                "select count(1) from (select distinct orderkey, custkey from orders)",
        };

        int[] unsampledResults = {60175, 1000, 15000, 5408941, 60175, 9256, 15000};
        for (int i = 0; i < queries.length; i++) {
            assertQuery(queries[i], "select " + unsampledResults[i]);
        }

        Session sessionWithKeyBasedSampling = Session.builder(getSession())
                .setSystemProperty(KEY_BASED_SAMPLING_ENABLED, "true")
                .setSystemProperty(KEY_BASED_SAMPLING_PERCENTAGE, "0.2")
                .build();

        int[] sampled20PercentResults = {37170, 616, 9189, 5408941, 37170, 5721, 9278};
        for (int i = 0; i < queries.length; i++) {
            assertQuery(sessionWithKeyBasedSampling, queries[i], "select " + sampled20PercentResults[i]);
        }

        sessionWithKeyBasedSampling = Session.builder(getSession())
                .setSystemProperty(KEY_BASED_SAMPLING_ENABLED, "true")
                .setSystemProperty(KEY_BASED_SAMPLING_PERCENTAGE, "0.1")
                .build();

        int[] sampled10PercentResults = {33649, 557, 8377, 4644937, 33649, 5098, 8397};
        for (int i = 0; i < queries.length; i++) {
            assertQuery(sessionWithKeyBasedSampling, queries[i], "select " + sampled10PercentResults[i]);
        }
    }

    @Test
    public void testLeftJoinWithArrayContainsCondition()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REWRITE_LEFT_JOIN_ARRAY_CONTAINS_TO_EQUI_JOIN, "ALWAYS_ENABLED")
                .build();

        @Language("SQL") String sql = "with t1 as (select * from (values (array[1, 2, 3], 10), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 left join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (10, 1, 'a'), (11, 4, 'b')");

        sql = "with t1 as (select * from (values (array[1, 2, 3, null], 10), (array[4, 5, 6, null, null], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 left join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (10, 1, 'a'), (11, 4, 'b')");

        sql = "with t1 as (select * from (values (array[1, 2, 3], 10), (array[4, 5, 6], 11), (array[null, 9], 12)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b'), (null, 'c'), (9, 'd'), (8, 'd')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 left join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (10, 1, 'a'), (11, 4, 'b'), (null, null, 'c'), (12, 9, 'd'), (null, 8, 'd')");

        sql = "with t1 as (select * from (values (array[1, 2, 3, null, null], 10), (array[4, 5, 6, null, null], 11), (array[null, 9], 12)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b'), (null, 'c'), (9, 'd'), (8, 'd')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 left join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (10, 1, 'a'), (11, 4, 'b'), (null, null, 'c'), (12, 9, 'd'), (null, 8, 'd')");

        sql = "with t1 as (select * from (values (array[1, 1, 3], 10), (array[4, 4, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 left join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (10, 1, 'a'), (11, 4, 'b')");

        sql = "with t1 as (select * from (values (array[1, 1, 3, null, null], 10), (array[4, 4, 6, null, null], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 left join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (10, 1, 'a'), (11, 4, 'b')");

        sql = "with t1 as (select * from (values (array[1, null, 3], 10), (array[4, null, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (null, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 left join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (10, 1, 'a'), (NULL, NULL, 'b')");

        sql = "with t1 as (select * from (values (array[1, 2, 3], 10), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 left join t1 on contains(t1.arr, t2.k) and t1.k > 10";
        assertQuery(enableOptimization, sql, "values (NULL, 1, 'a'), (11, 4, 'b')");

        sql = "with t1 as (select * from (values (array[1, 2, 3], 1), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (1, 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 left join t1 on contains(t1.arr, t2.k) or t1.k = t2.k";
        assertQuery(enableOptimization, sql, "values (1, 1, 'a'), (11, 4, 'b')");

        sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from orders o left join t1 on contains(t1.orderkey, o.orderkey) where o.totalprice < 2000";
        // Because the UDF has different names in H2, which is `array_contains`
        String h2Sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from orders o left join t1 on array_contains(t1.orderkey, o.orderkey) where o.totalprice < 2000";
        assertQuery(enableOptimization, sql, h2Sql);

        sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from orders o left join t1 on contains(t1.orderkey, o.orderkey) and t1.partkey < o.orderkey where o.totalprice < 2000";
        h2Sql = "with t1 as (select array_agg(orderkey) orderkey, partkey from lineitem l where l.quantity < 5 group by partkey) " +
                "select t1.partkey, o.orderkey, o.totalprice from orders o left join t1 on array_contains(t1.orderkey, o.orderkey) and t1.partkey < o.orderkey where o.totalprice < 2000";
        assertQuery(enableOptimization, sql, h2Sql);

        // Element type and array type does not match
        sql = "with t1 as (select * from (values (array[cast(1 as bigint), 2, 3], 10), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (cast(1 as integer), 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 left join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (11, 4, 'b'), (10, 1, 'a')");

        sql = "with t1 as (select * from (values (array[cast(1 as integer), 2, 3], 10), (array[4, 5, 6], 11)) t(arr, k)), t2 as (select * from (values (cast(1 as bigint), 'a'), (4, 'b')) t(k, v)) " +
                "select t1.k, t2.k, t2.v from t2 left join t1 on contains(t1.arr, t2.k)";
        assertQuery(enableOptimization, sql, "values (11, 4, 'b'), (10, 1, 'a')");
    }

    @Test
    public void testKeyBasedSamplingFunctionError()
    {
        Session sessionWithKeyBasedSampling = Session.builder(getSession())
                .setSystemProperty(KEY_BASED_SAMPLING_ENABLED, "true")
                .setSystemProperty(KEY_BASED_SAMPLING_FUNCTION, "blah")
                .build();

        assertQueryFails(sessionWithKeyBasedSampling, "select count(1) from orders join lineitem using(orderkey)", "Sampling function: blah not cannot be resolved");
    }

    @Test
    public void testSamplingJoinChain()
    {
        Session sessionWithKeyBasedSampling = Session.builder(getSession())
                .setSystemProperty(KEY_BASED_SAMPLING_ENABLED, "true")
                .build();
        @Language("SQL") String sql = "select count(1) FROM lineitem l left JOIN orders o ON l.orderkey = o.orderkey JOIN customer c ON o.custkey = c.custkey";

        assertQuery(sql, "select 60175");
        assertQuery(sessionWithKeyBasedSampling, sql, "select 16185");
    }

    @Test
    public void testTry()
    {
        // Test try with map method and value parameter is optional and argument is an array with null,
        // the error should be suppressed and just return null.
        assertQuery("SELECT\n" +
                "    TRY(map_keys_by_top_n_values(c0, BIGINT '6455219767830808341'))\n" +
                "FROM (\n" +
                "    VALUES\n" +
                "        MAP(\n" +
                "            ARRAY[1, 2], ARRAY[\n" +
                "                ARRAY[1, null],\n" +
                "                ARRAY[1, null]\n" +
                "            ]\n" +
                "        )\n" +
                ") t(c0)", "SELECT NULL");

        assertQuery("SELECT\n" +
                "    TRY(map_keys_by_top_n_values(c0, BIGINT '6455219767830808341'))\n" +
                "FROM (\n" +
                "    VALUES\n" +
                "        MAP(\n" +
                "            ARRAY[1, 2], ARRAY[\n" +
                "                ARRAY[null, null],\n" +
                "                ARRAY[1, 2]\n" +
                "            ]\n" +
                "        )\n" +
                ") t(c0)", "SELECT NULL");

        // Test try with array method with an input array containing null values.
        // the error should be suppressed and just return null.
        assertQuery("SELECT TRY(ARRAY_MAX(ARRAY [ARRAY[1, NULL], ARRAY[1, 2]]))", "SELECT NULL");
    }
}
