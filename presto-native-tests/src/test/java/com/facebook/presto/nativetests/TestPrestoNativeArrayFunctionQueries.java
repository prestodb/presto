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
    public void testArrayLeastFrequent()
    {
        // Single least frequent element.
        assertQuery("SELECT array_least_frequent(a) FROM (VALUES (ARRAY[1, 2, 2, 3, 3, 3])) AS t(a)");
        assertQuery("SELECT array_least_frequent(a) FROM (VALUES (CAST(ARRAY['a', 'b', 'b', 'c', 'c', 'c'] AS ARRAY(VARCHAR)))) AS t(a)");

        // Unsorted input exercises ascending-order output.
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (ARRAY[3, 1, 2, 3, 2, 3], 2)) AS t(a, b)");
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (CAST(ARRAY['c', 'a', 'b', 'c', 'b', 'c'] AS ARRAY(VARCHAR)), 2)) AS t(a, b)");

        // Tie-break: returns the smallest element among those with the lowest frequency.
        assertQuery("SELECT array_least_frequent(a) FROM (VALUES (ARRAY[1, 1, 2, 2, 3, 3])) AS t(a)");
        assertQuery("SELECT array_least_frequent(a) FROM (VALUES (CAST(ARRAY['abc', 'bc', 'aaa'] AS ARRAY(VARCHAR)))) AS t(a)");

        // Single-element and all-equal arrays.
        assertQuery("SELECT array_least_frequent(a) FROM (VALUES (ARRAY[42])) AS t(a)");
        assertQuery("SELECT array_least_frequent(a) FROM (VALUES (ARRAY[42, 42])) AS t(a)");
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (ARRAY[42], 1)) AS t(a, b)");
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (ARRAY[42, 42], 1)) AS t(a, b)");
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (ARRAY[42, 42], 2)) AS t(a, b)");

        // Null handling.
        assertQuery("SELECT array_least_frequent(a) FROM (VALUES (CAST(NULL AS ARRAY(INTEGER)))) AS t(a)");
        assertQuery("SELECT array_least_frequent(a) FROM (VALUES (CAST(ARRAY[] AS ARRAY(INTEGER)))) AS t(a)");
        assertQuery("SELECT array_least_frequent(a) FROM (VALUES (CAST(ARRAY[NULL, NULL, NULL] AS ARRAY(INTEGER)))) AS t(a)");

        // Nulls in array are ignored.
        assertQuery("SELECT array_least_frequent(a) FROM (VALUES (ARRAY[1, 2, 2, NULL])) AS t(a)");

        // Nulls are also ignored for the N-argument variant.
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (ARRAY[1, 2, 2, NULL], 1)) AS t(a, b)");
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (ARRAY[1, 2, 2, NULL], 2)) AS t(a, b)");

        // N least frequent elements returned in ascending order.
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (ARRAY[1, 2, 2, 3, 3, 3], 2)) AS t(a, b)");
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (CAST(ARRAY['a', 'b', 'b', 'c', 'c', 'c'] AS ARRAY(VARCHAR)), 3)) AS t(a, b)");
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (ARRAY[1, 1, 2, 2, 3, 3], 1)) AS t(a, b)");

        // N equal to number of distinct non-null elements: returns all.
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (ARRAY[1, 2, 2, 3, 3, 3], 3)) AS t(a, b)");

        // N greater than number of distinct non-null elements: returns all.
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (ARRAY[1, 2, 2, 3, 3, 3, -1], 5)) AS t(a, b)");

        // N = 0 returns empty array (not null) when array has non-null elements.
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (ARRAY[1, 2, 2, NULL], 0)) AS t(a, b)");

        // Null/empty input returns null regardless of n.
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (CAST(NULL AS ARRAY(INTEGER)), 3)) AS t(a, b)");
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (CAST(ARRAY[] AS ARRAY(INTEGER)), 2)) AS t(a, b)");
        assertQuery("SELECT array_least_frequent(a, b) FROM (VALUES (CAST(ARRAY[NULL, NULL, NULL] AS ARRAY(INTEGER)), 1)) AS t(a, b)");

        // Negative n fails.
        assertQueryFails(
                "SELECT array_least_frequent(a, b) FROM (VALUES (CAST(ARRAY['a', 'b', 'b', 'c', 'c', 'c'] AS ARRAY(VARCHAR)), -1)) AS t(a, b)",
                ".*n must be greater than or equal to 0.*");
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
}
