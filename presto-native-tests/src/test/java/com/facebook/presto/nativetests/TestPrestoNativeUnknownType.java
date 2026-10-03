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
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
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

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        queryRunner.execute("DROP TABLE IF EXISTS unknown_type_test_table");
        queryRunner.execute("CREATE TABLE unknown_type_test_table (" +
                "null_col   INTEGER, " +
                "arr_col    ARRAY(INTEGER), " +
                "map_col    MAP(INTEGER, INTEGER), " +
                "bigint_col BIGINT" +
                ")");
        queryRunner.execute("INSERT INTO unknown_type_test_table VALUES " +
                "(NULL, ARRAY[], MAP(), NULL), " +
                "(NULL, ARRAY[], MAP(), NULL), " +
                "(NULL, ARRAY[], MAP(), NULL)");
    }

    private void testQuery(@Language("SQL") String query, @Language("SQL") String expected)
    {
        assertQuery(query);
        assertQuery(query, expected);
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
        testQuery(
                "SELECT contains(arr_col, null_col) FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN))");
        testQuery(
                "SELECT contains(arr_col, null_col) FROM unknown_type_test_table WHERE null_col IS NULL",
                "VALUES (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN))");
        testQuery(
                "SELECT contains(arr, val) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)), NULL)) AS t(arr, val)",
                "VALUES (CAST(NULL AS BOOLEAN))");
    }

    @Test
    public void testArrayPositionWithUnknownType()
    {
        testQuery(
                "SELECT array_position(arr_col, null_col) FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS BIGINT)), (CAST(NULL AS BIGINT)), (CAST(NULL AS BIGINT))");
        testQuery(
                "SELECT array_position(arr, val) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)), NULL)) AS t(arr, val)",
                "VALUES (CAST(NULL AS BIGINT))");
    }

    @Test
    public void testArrayDistinctWithUnknownType()
    {
        testQuery(
                "SELECT array_distinct(arr_col) FROM unknown_type_test_table",
                "VALUES (CAST(ARRAY[] AS ARRAY(INTEGER))), (CAST(ARRAY[] AS ARRAY(INTEGER))), (CAST(ARRAY[] AS ARRAY(INTEGER)))");
        testQuery(
                "SELECT array_distinct(arr) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)))) AS t(arr)",
                "VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)))");
        testQuery(
                "SELECT array_distinct(arr) FROM (VALUES (ARRAY[NULL, NULL, NULL])) AS t(arr)",
                "VALUES (ARRAY[NULL])");
    }

    @Test
    public void testArrayDuplicatesWithUnknownType()
    {
        // array_duplicates is registered in Velox for BIGINT, VARCHAR, and UNKNOWN
        // (not INTEGER), so we use the bigint_col column from the table.
        testQuery(
                "SELECT array_duplicates(ARRAY[bigint_col]) FROM unknown_type_test_table",
                "VALUES (CAST(ARRAY[] AS ARRAY(BIGINT))), (CAST(ARRAY[] AS ARRAY(BIGINT))), (CAST(ARRAY[] AS ARRAY(BIGINT)))");
        testQuery(
                "SELECT array_duplicates(arr) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)))) AS t(arr)",
                "VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)))");
        testQuery(
                "SELECT array_duplicates(arr) FROM (VALUES (ARRAY[NULL, NULL, NULL])) AS t(arr)",
                "VALUES (ARRAY[NULL])");
    }

    @Test
    public void testArrayIntersectWithUnknownType()
    {
        testQuery(
                "SELECT array_intersect(arr_col, arr_col) FROM unknown_type_test_table",
                "VALUES (CAST(ARRAY[] AS ARRAY(INTEGER))), (CAST(ARRAY[] AS ARRAY(INTEGER))), (CAST(ARRAY[] AS ARRAY(INTEGER)))");
        testQuery(
                "SELECT array_intersect(arr1, arr2) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)), CAST(ARRAY[] AS ARRAY(UNKNOWN)))) AS t(arr1, arr2)",
                "VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)))");
        testQuery(
                "SELECT array_intersect(arr1, arr2) FROM (VALUES (ARRAY[NULL], ARRAY[NULL])) AS t(arr1, arr2)",
                "VALUES (ARRAY[NULL])");
    }

    @Test
    public void testArrayExceptWithUnknownType()
    {
        testQuery(
                "SELECT array_except(arr_col, arr_col) FROM unknown_type_test_table",
                "VALUES (CAST(ARRAY[] AS ARRAY(INTEGER))), (CAST(ARRAY[] AS ARRAY(INTEGER))), (CAST(ARRAY[] AS ARRAY(INTEGER)))");
        testQuery(
                "SELECT array_except(arr1, arr2) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)), CAST(ARRAY[] AS ARRAY(UNKNOWN)))) AS t(arr1, arr2)",
                "VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)))");
        testQuery(
                "SELECT array_except(arr1, arr2) FROM (VALUES (ARRAY[NULL, NULL], ARRAY[NULL])) AS t(arr1, arr2)",
                "VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)))");
    }

    @Test
    public void testArraysOverlapWithUnknownType()
    {
        // arrays_overlap(ARRAY[], ARRAY[]) returns FALSE (no elements to overlap)
        testQuery(
                "SELECT arrays_overlap(arr_col, arr_col) FROM unknown_type_test_table",
                "VALUES (FALSE), (FALSE), (FALSE)");
        testQuery(
                "SELECT arrays_overlap(arr1, arr2) FROM (VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)), CAST(ARRAY[] AS ARRAY(UNKNOWN)))) AS t(arr1, arr2)",
                "VALUES (FALSE)");
        testQuery(
                "SELECT arrays_overlap(arr1, arr2) FROM (VALUES (ARRAY[NULL], ARRAY[NULL])) AS t(arr1, arr2)",
                "VALUES (CAST(NULL AS BOOLEAN))");
    }

    @Test
    public void testAggregatesWithUnknownType()
    {
        testQuery(
                "SELECT set_agg(null_col) FROM unknown_type_test_table",
                "VALUES (ARRAY[CAST(NULL AS INTEGER)])");
        testQuery(
                "SELECT map_agg(null_col, null_col) FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS MAP(INTEGER, INTEGER)))");
        testQuery(
                "SELECT cardinality(map_col) FROM unknown_type_test_table",
                "VALUES (BIGINT '0'), (BIGINT '0'), (BIGINT '0')");
        testQuery(
                "SELECT approx_distinct(null_col) FROM unknown_type_test_table",
                "VALUES (BIGINT '0')");
        testQuery(
                "SELECT approx_distinct(null_col, 0.023) FROM unknown_type_test_table",
                "VALUES (BIGINT '0')");
        testQuery(
                "SELECT histogram(null_col) FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS MAP(INTEGER, BIGINT)))");
        testQuery(
                "SELECT multimap_agg(null_col, null_col) FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS MAP(INTEGER, ARRAY(INTEGER))))");

        // MAP(UNKNOWN, UNKNOWN) and ARRAY(UNKNOWN) cannot be stored in Hive;
        // use VALUES to exercise those specific UNKNOWN type paths.
        testQuery(
                "SELECT set_union(x) FROM (VALUES CAST(ARRAY[] AS ARRAY(UNKNOWN)), CAST(ARRAY[] AS ARRAY(UNKNOWN))) t(x)",
                "VALUES (CAST(ARRAY[] AS ARRAY(UNKNOWN)))");
        testQuery(
                "SELECT map_union(x) FROM (VALUES CAST(MAP() AS MAP(UNKNOWN, UNKNOWN)), CAST(MAP() AS MAP(UNKNOWN, UNKNOWN))) t(x)",
                "VALUES (CAST(MAP() AS MAP(UNKNOWN, UNKNOWN)))");
        testQuery(
                "SELECT approx_most_frequent(3, x, 100) FROM (VALUES NULL, NULL, NULL) t(x)",
                "VALUES (CAST(NULL AS MAP(UNKNOWN, BIGINT)))");

        // merge combines multiple HyperLogLog sketches; empty_approx_set() creates an empty sketch
        testQuery(
                "SELECT cardinality(merge(empty_approx_set())) FROM unknown_type_test_table",
                "VALUES (BIGINT '0')");

        // khyperloglog_agg with all NULL values returns NULL cardinality.
        // Note: Java Presto doesn't support UNKNOWN type for khyperloglog_agg; Velox support
        // added with https://github.com/prestodb/presto/issues/27907
        assertQueryWithSameQueryRunner(
                "SELECT cardinality(khyperloglog_agg(null_col, null_col)) FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS BIGINT))");
    }

    @Test
    public void testComparisonOperatorsWithUnknownType()
    {
        // Any comparison with NULL yields NULL
        testQuery(
                "SELECT null_col = (SELECT NULL) FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN))");
        testQuery(
                "SELECT null_col != (SELECT NULL) FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN))");
        testQuery(
                "SELECT null_col < (SELECT NULL) FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN))");
        testQuery(
                "SELECT null_col > (SELECT NULL) FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN))");
    }

    // typeof() must stay VALUES-based: a persisted INTEGER NULL column returns
    // typeof = 'integer', not 'unknown'.
    @Test
    public void testUnknownTypeOf()
    {
        testQuery(
                "SELECT typeof(x) FROM (SELECT NULL as x FROM (VALUES 1)) t",
                "VALUES ('unknown')");
        testQuery(
                "SELECT typeof(x) FROM (VALUES (NULL)) t(x)",
                "VALUES ('unknown')");
        testQuery(
                "SELECT typeof(arr) FROM (VALUES (ARRAY[])) t(arr)",
                "VALUES ('array(unknown)')");
        testQuery(
                "SELECT typeof(m) FROM (VALUES (MAP())) t(m)",
                "VALUES ('map(unknown, unknown)')");
    }

    @Test
    public void testComplexQueriesWithUnknownType()
    {
        testQuery(
                "SELECT array_distinct(arr_col), array_intersect(arr_col, arr_col), " +
                        "array_except(arr_col, arr_col), arrays_overlap(arr_col, arr_col) " +
                        "FROM unknown_type_test_table",
                "VALUES " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER)), CAST(ARRAY[] AS ARRAY(INTEGER)), CAST(ARRAY[] AS ARRAY(INTEGER)), FALSE), " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER)), CAST(ARRAY[] AS ARRAY(INTEGER)), CAST(ARRAY[] AS ARRAY(INTEGER)), FALSE), " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER)), CAST(ARRAY[] AS ARRAY(INTEGER)), CAST(ARRAY[] AS ARRAY(INTEGER)), FALSE)");

        testQuery(
                "SELECT CASE WHEN true THEN contains(arr_col, null_col) ELSE false END " +
                        "FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN)), (CAST(NULL AS BOOLEAN))");

        testQuery(
                "SELECT array_distinct(array_intersect(arr_col, arr_col)) " +
                        "FROM unknown_type_test_table",
                "VALUES (CAST(ARRAY[] AS ARRAY(INTEGER))), (CAST(ARRAY[] AS ARRAY(INTEGER))), (CAST(ARRAY[] AS ARRAY(INTEGER)))");

        testQuery(
                "SELECT COUNT(*) AS cnt, array_distinct(arr_col) AS empty_array " +
                        "FROM unknown_type_test_table " +
                        "GROUP BY array_distinct(arr_col)",
                "VALUES (BIGINT '3', CAST(ARRAY[] AS ARRAY(INTEGER)))");

        // contains(arr, x) returns NULL when x is NULL; NULL IS NULL is true
        testQuery(
                "SELECT * FROM unknown_type_test_table " +
                        "WHERE contains(arr_col, null_col) IS NULL",
                "VALUES " +
                        "(CAST(NULL AS INTEGER), CAST(ARRAY[] AS ARRAY(INTEGER)), CAST(MAP() AS MAP(INTEGER, INTEGER)), CAST(NULL AS BIGINT)), " +
                        "(CAST(NULL AS INTEGER), CAST(ARRAY[] AS ARRAY(INTEGER)), CAST(MAP() AS MAP(INTEGER, INTEGER)), CAST(NULL AS BIGINT)), " +
                        "(CAST(NULL AS INTEGER), CAST(ARRAY[] AS ARRAY(INTEGER)), CAST(MAP() AS MAP(INTEGER, INTEGER)), CAST(NULL AS BIGINT))");
    }

    @Test
    public void testUnknownTypeWithJoinsAndSubqueries()
    {
        assertQuery(
                "SELECT t1.arr_col, t2.arr_col, array_distinct(t1.arr_col) " +
                        "FROM unknown_type_test_table t1 CROSS JOIN unknown_type_test_table t2 " +
                        "LIMIT 1");

        testQuery(
                "SELECT arr_col FROM unknown_type_test_table " +
                        "WHERE arr_col IN (SELECT arr_col FROM unknown_type_test_table)",
                "VALUES " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER))), " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER))), " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER)))");

        testQuery(
                "SELECT arr_col FROM unknown_type_test_table " +
                        "WHERE contains(arr_col, null_col) IS NULL",
                "VALUES " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER))), " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER))), " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER)))");
    }

    @Test
    public void testUnknownTypeEdgeCases()
    {
        testQuery(
                "SELECT COALESCE(array_distinct(arr_col), ARRAY[1]) FROM unknown_type_test_table",
                "VALUES (CAST(ARRAY[] AS ARRAY(INTEGER))), (CAST(ARRAY[] AS ARRAY(INTEGER))), (CAST(ARRAY[] AS ARRAY(INTEGER)))");
        testQuery(
                "SELECT NULLIF(array_distinct(arr_col), arr_col) FROM unknown_type_test_table",
                "VALUES (CAST(NULL AS ARRAY(INTEGER))), (CAST(NULL AS ARRAY(INTEGER))), (CAST(NULL AS ARRAY(INTEGER)))");
        testQuery(
                "SELECT TRY(contains(arr_col, null_col)), " +
                        "TRY(array_position(arr_col, null_col)), " +
                        "TRY(array_distinct(arr_col)) " +
                        "FROM unknown_type_test_table",
                "VALUES " +
                        "(CAST(NULL AS BOOLEAN), CAST(NULL AS BIGINT), CAST(ARRAY[] AS ARRAY(INTEGER))), " +
                        "(CAST(NULL AS BOOLEAN), CAST(NULL AS BIGINT), CAST(ARRAY[] AS ARRAY(INTEGER))), " +
                        "(CAST(NULL AS BOOLEAN), CAST(NULL AS BIGINT), CAST(ARRAY[] AS ARRAY(INTEGER)))");
        testQuery(
                "WITH t AS (SELECT array_distinct(arr_col) AS arr FROM unknown_type_test_table) " +
                        "SELECT arr FROM t",
                "VALUES " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER))), " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER))), " +
                        "(CAST(ARRAY[] AS ARRAY(INTEGER)))");
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        queryRunner.execute("DROP TABLE IF EXISTS unknown_type_test_table");
    }
}
