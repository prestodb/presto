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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.Boolean.parseBoolean;

public class TestPrestoNativeArrayFunctionQueries
        extends AbstractTestQueryFramework
{
    private String storageFormat;
    private boolean sidecarEnabled;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .setCoordinatorSidecarEnabled(sidecarEnabled)
                .build();
        if (sidecarEnabled) {
            setupNativeSidecarPlugin(queryRunner);
        }
        else {
            queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        }
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .build();
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Test
    public void testArrayMaxBy()
    {
        assertQuery("SELECT array_max_by(a, x -> length(x)) from (values(ARRAY['a', 'bbb', 'cc'])) as t(a)");
        assertQuery("SELECT array_max_by(a, x -> length(x)) from (values(ARRAY['aa', 'bb', 'c'])) as t(a)");
        assertQuery("SELECT array_max_by(a, x -> length(x)) from (values(ARRAY['a', NULL, 'bbb'])) as t(a)");
        assertQuery("SELECT array_max_by(a, x -> length(x)) from (values(ARRAY[NULL, NULL])) as t(a)");
        assertQuery("SELECT array_max_by(a, x -> length(x)) from (values(ARRAY['aa', 'bb', 'c'])) as t(a)");
        assertQuery("SELECT array_max_by(a, x -> x) from (values(ARRAY[])) as t(a)");
        assertQuery("SELECT array_max_by(a, x -> abs(x)) from (values(ARRAY[-10, 5, 7])) as t(a)");
        assertQuery("SELECT array_max_by(a, x -> IF(x = 2, NULL, x)) from (values(ARRAY[1, 2, 3])) as t(a)");
        assertQuery("SELECT array_max_by(a, x -> x) from (values(CAST(NULL AS ARRAY(INTEGER)))) as t(a)");
    }

    @Test
    public void testArrayMinBy()
    {
        assertQuery("SELECT array_min_by(a, x -> length(x)) from (values(ARRAY['a', 'bbb', 'cc'])) as t(a)");
        assertQuery("SELECT array_min_by(a, x -> length(x)) from (values(ARRAY['aa', 'bb', 'c'])) as t(a)");
        assertQuery("SELECT array_min_by(a, x -> length(x)) from (values(ARRAY['a', NULL, 'bbb'])) as t(a)");
        assertQuery("SELECT array_min_by(a, x -> length(x)) from (values(ARRAY[NULL, NULL])) as t(a)");
        assertQuery("SELECT array_min_by(a, x -> length(x)) from (values(ARRAY['aa', 'bb', 'c'])) as t(a)");
        assertQuery("SELECT array_min_by(a, x -> x) from (values(ARRAY[])) as t(a)");
        assertQuery("SELECT array_min_by(a, x -> abs(x)) from (values(ARRAY[-10, 5, 7])) as t(a)");
        assertQuery("SELECT array_min_by(a, x -> IF(x = 2, NULL, x)) from (values(ARRAY[1, 2, 3])) as t(a)");
        assertQuery("SELECT array_min_by(a, x -> x) from (values(CAST(NULL AS ARRAY(INTEGER)))) as t(a)");
    }

    @Test
    public void testArrayTopN()
    {
        assertQuery("SELECT array_top_n(a, b) FROM (VALUES(ARRAY[1, 5, 3, 9, 2],3)) as t(a,b)");
        assertQuery("SELECT array_top_n(a, b) FROM (VALUES(ARRAY[1, 2], 5)) as t(a,b)");
        assertQuery("SELECT array_top_n(a, b) FROM (VALUES(ARRAY[5, 1, 5, 3], 2)) as t(a,b)");
        assertQuery("SELECT array_top_n(a, b) FROM (VALUES(ARRAY[1, NULL, 3, 2], 2)) as t(a,b)");
        assertQuery("SELECT array_top_n(a, b) FROM (VALUES(ARRAY[1, 2, 3], 0)) as t(a,b)");
        assertQuery("SELECT array_top_n(a, b) FROM (VALUES(ARRAY[], 2)) as t(a,b)");
        assertQuery("SELECT array_top_n(a, b) FROM (VALUES(CAST(NULL AS ARRAY(INTEGER)), 2)) as t(a,b)");
        assertQueryFails("SELECT array_top_n(a, b) FROM (VALUES(ARRAY[1, 2, 3], -2)) as t(a,b)",
                "n >= 0 \\(-2 vs\\. 0\\) Parameter n: -2 to ARRAY_TOP_N is negative Top-level Expression: (presto|native)\\.default\\.array_top_n\\(field, field_0\\)");
    }

    @Test
    public void testArrayTopNTransform()
    {
        // The single-argument transform overload of array_top_n is registered only through the native sidecar's
        // function namespace. Without the sidecar the coordinator knows only the two-argument comparator overload,
        // so a one-argument lambda fails analysis.
        if (sidecarEnabled) {
            // Identity transform behaves like natural descending order.
            assertQuery("SELECT array_top_n(a, 2, x -> x) FROM (VALUES(ARRAY[1, 2, 3])) as t(a)",
                    "SELECT ARRAY[3, 2]");
            assertQuery("SELECT array_top_n(a, 3, x -> x) FROM (VALUES(ARRAY[1, 5, 3, 9, 2])) as t(a)",
                    "SELECT ARRAY[9, 5, 3]");

            // Transform by absolute value (distinct keys keep ordering deterministic).
            assertQuery("SELECT array_top_n(a, 3, x -> abs(x)) FROM (VALUES(ARRAY[-5, 2, -3, 4, 1])) as t(a)",
                    "SELECT ARRAY[-5, 4, -3]");

            // Negated transform yields the smallest elements first.
            assertQuery("SELECT array_top_n(a, 2, x -> 0 - x) FROM (VALUES(ARRAY[1, 2, 3])) as t(a)",
                    "SELECT ARRAY[1, 2]");

            // Transform over strings (sort by length, longest first).
            assertQuery("SELECT array_top_n(a, 2, x -> length(x)) FROM (VALUES(ARRAY['a', 'bbb', 'cc', 'dddd'])) as t(a)",
                    "SELECT ARRAY['dddd', 'bbb']");

            // Null elements in the input are ordered last by their (null) key.
            assertQuery("SELECT array_top_n(a, 3, x -> x) FROM (VALUES(ARRAY[1, NULL, 3, NULL, 5])) as t(a)",
                    "SELECT ARRAY[5, 3, 1]");

            // A transform that returns null pushes those elements to the end (stable by original index).
            assertQuery("SELECT array_top_n(a, 3, x -> IF(x % 2 = 0, CAST(NULL AS INTEGER), x)) FROM (VALUES(ARRAY[1, 2, 3])) as t(a)",
                    "SELECT ARRAY[3, 1, 2]");

            // Duplicate elements are all retained when they tie for the top.
            assertQuery("SELECT array_top_n(a, 3, x -> x) FROM (VALUES(ARRAY[3, 1, 3, 2, 1, 3])) as t(a)",
                    "SELECT ARRAY[3, 3, 3]");

            // n = 0 returns an empty array.
            assertQuery("SELECT array_top_n(a, 0, x -> x) FROM (VALUES(ARRAY[1, 2, 3])) as t(a)",
                    "SELECT CAST(ARRAY[] AS ARRAY(INTEGER))");

            // n larger than the array size returns the whole array, ordered.
            assertQuery("SELECT array_top_n(a, 10, x -> x) FROM (VALUES(ARRAY[1, 2])) as t(a)",
                    "SELECT ARRAY[2, 1]");

            // Empty and all-null inputs.
            assertQuery("SELECT array_top_n(a, 2, x -> x) FROM (VALUES(CAST(ARRAY[] AS ARRAY(INTEGER)))) as t(a)",
                    "SELECT CAST(ARRAY[] AS ARRAY(INTEGER))");
            assertQuery("SELECT array_top_n(a, 2, x -> x) FROM (VALUES(CAST(ARRAY[NULL, NULL, NULL] AS ARRAY(INTEGER)))) as t(a)",
                    "SELECT CAST(ARRAY[NULL, NULL] AS ARRAY(INTEGER))");

            // A null top-level array produces a null result.
            assertQuery("SELECT array_top_n(a, 2, x -> x) FROM (VALUES(CAST(NULL AS ARRAY(INTEGER)))) as t(a)",
                    "SELECT CAST(NULL AS ARRAY(INTEGER))");

            // Larger numeric types.
            assertQuery("SELECT array_top_n(a, 2, x -> x) FROM (VALUES(ARRAY[100000000000, 200000000000, 50000000000])) as t(a)",
                    "SELECT ARRAY[200000000000, 100000000000]");
            assertQuery("SELECT array_top_n(a, 3, x -> x) FROM (VALUES(ARRAY[DOUBLE '1.5', DOUBLE '2.7', DOUBLE '0.3', DOUBLE '4.1'])) as t(a)",
                    "SELECT ARRAY[DOUBLE '4.1', DOUBLE '2.7', DOUBLE '1.5']");

            // Negative n is rejected by the Velox implementation.
            assertQueryFails("SELECT array_top_n(a, -1, x -> x) FROM (VALUES(ARRAY[1, 2, 3])) as t(a)",
                    ".*n must be greater than or equal to 0.*");
        }
        else {
            String transformLambdaUnsupported = ".*Expected a lambda that takes 2 argument\\(s\\) but got 1.*";
            assertQueryFails("SELECT array_top_n(a, 2, x -> x) FROM (VALUES(ARRAY[1, 2, 3])) as t(a)",
                    transformLambdaUnsupported);
            assertQueryFails("SELECT array_top_n(a, 3, x -> abs(x)) FROM (VALUES(ARRAY[-5, 2, -3, 4, 1])) as t(a)",
                    transformLambdaUnsupported);
        }
    }
}
