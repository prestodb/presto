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
package com.facebook.presto.nativetests;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.scalar.sql.SqlInvokedFunctionsPlugin;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.sidecar.TestNativeSidecarPlugin.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

public class TestPrestoNativeUnknownType
        extends AbstractTestQueryFramework
{
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    private void installSqlInvokedFunctionsPlugin(QueryRunner queryRunner)
    {
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        else {
            installSqlInvokedFunctionsPlugin(queryRunner);
        }
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
        installSqlInvokedFunctionsPlugin(queryRunner);
        return queryRunner;
    }

    @Test
    public void testArrayContainsWithUnknownType()
    {
        assertQuery("SELECT contains(arr, val) FROM (VALUES (ARRAY[], NULL)) AS t(arr, val)");
        assertQuery("SELECT contains(arr, val) FROM (VALUES (ARRAY[], 1)) AS t(arr, val)");
        assertQuery("SELECT contains(arr, val) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)), NULL)) AS t(arr, val)");
    }

    @Test
    public void testArrayPositionWithUnknownType()
    {
        assertQuery("SELECT array_position(arr, val) FROM (VALUES (ARRAY[], NULL)) AS t(arr, val)");
        assertQuery("SELECT array_position(arr, val) FROM (VALUES (ARRAY[], 1)) AS t(arr, val)");
        assertQuery("SELECT array_position(arr, val) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)), NULL)) AS t(arr, val)");
    }

    @Test
    public void testArrayDistinctWithUnknownType()
    {
        assertQuery("SELECT array_distinct(arr) FROM (VALUES (ARRAY[])) AS t(arr)");
        assertQuery("SELECT array_distinct(arr) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)))) AS t(arr)");
        assertQuery("SELECT array_distinct(arr) FROM (VALUES (ARRAY[NULL, NULL, NULL])) AS t(arr)");
    }

    @Test
    public void testArrayDuplicatesWithUnknownType()
    {
        assertQuery("SELECT array_duplicates(arr) FROM (VALUES (ARRAY[])) AS t(arr)");
        assertQuery("SELECT array_duplicates(arr) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)))) AS t(arr)");
        assertQuery("SELECT array_duplicates(arr) FROM (VALUES (ARRAY[NULL, NULL, NULL])) AS t(arr)");
    }

    @Test
    public void testArrayIntersectWithUnknownType()
    {
        assertQuery("SELECT array_intersect(arr1, arr2) FROM (VALUES (ARRAY[], ARRAY[])) AS t(arr1, arr2)");
        assertQuery("SELECT array_intersect(arr1, arr2) FROM (VALUES (ARRAY[], ARRAY[1, 2, 3])) AS t(arr1, arr2)");
        assertQuery("SELECT array_intersect(arr1, arr2) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)), CAST(ARRAY[] AS ARRAY(UNKNOWN)))) AS t(arr1, arr2)");
        assertQuery("SELECT array_intersect(arr1, arr2) FROM (VALUES (ARRAY[NULL], ARRAY[NULL])) AS t(arr1, arr2)");
    }

    @Test
    public void testArrayExceptWithUnknownType()
    {
        assertQuery("SELECT array_except(arr1, arr2) FROM (VALUES (ARRAY[], ARRAY[])) AS t(arr1, arr2)");
        assertQuery("SELECT array_except(arr1, arr2) FROM (VALUES (ARRAY[], ARRAY[1, 2, 3])) AS t(arr1, arr2)");
        assertQuery("SELECT array_except(arr1, arr2) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)), CAST(ARRAY[] AS ARRAY(UNKNOWN)))) AS t(arr1, arr2)");
        assertQuery("SELECT array_except(arr1, arr2) FROM (VALUES (ARRAY[NULL, NULL], ARRAY[NULL])) AS t(arr1, arr2)");
    }

    @Test
    public void testArraysOverlapWithUnknownType()
    {
        assertQuery("SELECT arrays_overlap(arr1, arr2) FROM (VALUES (ARRAY[], ARRAY[])) AS t(arr1, arr2)");
        assertQuery("SELECT arrays_overlap(arr1, arr2) FROM (VALUES (ARRAY[], ARRAY[1, 2, 3])) AS t(arr1, arr2)");
        assertQuery("SELECT arrays_overlap(arr1, arr2) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)), CAST(ARRAY[] AS ARRAY(UNKNOWN)))) AS t(arr1, arr2)");
        assertQuery("SELECT arrays_overlap(arr1, arr2) FROM (VALUES (ARRAY[NULL], ARRAY[NULL])) AS t(arr1, arr2)");
    }

    @Test
    public void testAggregatesWithUnknownType()
    {
        assertQuery("SELECT set_agg(x) FROM (SELECT NULL as x FROM (VALUES 1) t(y)) t2");

        assertQuery("SELECT set_union(x) FROM (VALUES ARRAY[NULL], ARRAY[NULL]) t(x)");
        assertQuery("SELECT set_union(x) FROM (VALUES ARRAY[], ARRAY[]) t(x)");
        assertQuery("SELECT set_union(x) FROM (VALUES CAST(ARRAY[] AS ARRAY(UNKNOWN)), CAST(ARRAY[] AS ARRAY(UNKNOWN))) t(x)");

        assertQuery("SELECT map_union(x) FROM (VALUES MAP(), MAP()) t(x)");
        assertQuery("SELECT map_union(x) FROM (VALUES CAST(MAP() AS MAP(UNKNOWN, UNKNOWN)), CAST(MAP() AS MAP(UNKNOWN, UNKNOWN))) t(x)");

        assertQuery("SELECT approx_most_frequent(3, x, 100) FROM (VALUES NULL, NULL, NULL) t(x)");

        assertQuery("SELECT map_agg(k, v) FROM (SELECT NULL as k, NULL as v FROM (VALUES 1) t(x)) t2");

        assertQuery("SELECT approx_distinct(x) FROM (VALUES NULL, NULL, NULL) t(x)");
        assertQuery("SELECT approx_distinct(x, 0.023) FROM (VALUES NULL, NULL, NULL) t(x)");

        assertQuery("SELECT histogram(x) FROM (VALUES NULL, NULL, NULL) t(x)");

        assertQuery("SELECT multimap_agg(k, v) FROM (SELECT NULL as k, NULL as v FROM (VALUES 1) t(x)) t2");

        // merge combines multiple HyperLogLog sketches
        // empty_approx_set() creates an empty sketch with cardinality 0
        assertQuery("SELECT cardinality(merge(empty_approx_set())) FROM (VALUES 1, 2, 3) t(x)");

        // khyperloglog_agg with all NULL values for both parameters returns NULL cardinality
        // Note: Java Presto doesn't support UNKNOWN type for khyperloglog_agg, the velox support for the type
        // included with https://github.com/prestodb/presto/issues/27907
        assertQueryWithSameQueryRunner(
                "SELECT cardinality(khyperloglog_agg(x, y)) FROM (VALUES (NULL, NULL), (NULL, NULL)) t(x, y)",
                "VALUES (CAST(NULL AS BIGINT))");
    }

    @Test
    public void testComparisonOperatorsWithUnknownType()
    {
        assertQuery("SELECT x = (SELECT NULL) FROM (VALUES 1, 2, 3) t(x)");
        assertQuery("SELECT x != (SELECT NULL) FROM (VALUES 1, 2, 3) t(x)");
        assertQuery("SELECT x < (SELECT NULL) FROM (VALUES 1, 2, 3) t(x)");
        assertQuery("SELECT x > (SELECT NULL) FROM (VALUES 1, 2, 3) t(x)");
    }

    @Test
    public void testComplexQueriesWithUnknownType()
    {
        assertQuery(
                "SELECT array_distinct(arr) AS empty_distinct, array_duplicates(arr) AS empty_duplicates FROM (VALUES (ARRAY[])) AS t(arr)");

        assertQuery("SELECT CASE WHEN true THEN contains(arr, val) ELSE false END FROM (VALUES (ARRAY[], NULL)) AS t(arr, val)");

        assertQuery("SELECT array_distinct(arr) AS distinct_result, array_duplicates(arr) AS duplicates_result, " +
                "array_intersect(arr, arr) AS intersect_result, array_except(arr, arr) AS except_result, " +
                "arrays_overlap(arr, arr) AS overlap_result FROM (VALUES (ARRAY[])) AS t(arr)");

        assertQuery("SELECT array_distinct(array_intersect(arr1, arr2)) FROM (VALUES (ARRAY[], ARRAY[])) AS t(arr1, arr2)");

        // contains(arr, x) returns NULL when x is NULL, so NULL IS NULL is true
        assertQuery(
                "SELECT * FROM (VALUES (NULL), (NULL), (NULL)) AS t(x) WHERE contains((SELECT arr FROM (VALUES (ARRAY[])) AS t2(arr)), x) IS NULL",
                "SELECT * FROM (VALUES (NULL), (NULL), (NULL)) AS t(x)");

        assertQuery("SELECT COUNT(*) AS cnt, array_distinct(arr) AS empty_array FROM (VALUES (ARRAY[]), (ARRAY[]), (ARRAY[])) AS t(arr) GROUP BY array_distinct(arr)");
    }

    @Test
    public void testUnknownTypeWithJoinsAndSubqueries()
    {
        assertQuery("SELECT t1.arr AS arr1, t2.arr AS arr2, array_distinct(t1.arr) AS empty FROM (VALUES (ARRAY[])) AS t1(arr) CROSS JOIN (VALUES (ARRAY[])) AS t2(arr)");
        assertQuery("SELECT arr FROM (VALUES (ARRAY[])) AS t(arr) WHERE arr IN (SELECT arr FROM (VALUES (ARRAY[])) AS t2(arr))");
        assertQuery("SELECT array_intersect(arr1, arr2) AS result1, array_except(arr1, arr2) AS result2 FROM (VALUES (ARRAY[1, 2, 3], ARRAY[])) AS t(arr1, arr2)");
        assertQuery("SELECT arr FROM (VALUES (ARRAY[], NULL)) AS t(arr, val) WHERE contains(arr, val) IS NULL");
    }

    @Test
    public void testUnknownTypeEdgeCases()
    {
        assertQuery("WITH empty_arrays AS (SELECT array_distinct(arr) AS arr1, array_duplicates(arr) AS arr2 FROM (VALUES (ARRAY[])) t(arr)) SELECT * FROM empty_arrays");
        assertQuery("SELECT COALESCE(array_distinct(arr), ARRAY[1, 2, 3]) FROM (VALUES (ARRAY[])) AS t(arr)");
        assertQuery("SELECT NULLIF(array_distinct(arr1), arr2) FROM (VALUES (ARRAY[], ARRAY[])) AS t(arr1, arr2)");
        assertQuery("SELECT TRY(contains(arr, val)) AS test1, TRY(array_position(arr, val)) AS test2, TRY(array_distinct(arr)) AS test3 FROM (VALUES (ARRAY[], NULL)) AS t(arr, val)");
    }
}
