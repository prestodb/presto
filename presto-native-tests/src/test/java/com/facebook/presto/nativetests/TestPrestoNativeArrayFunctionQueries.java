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
import org.testng.annotations.Test;

public class TestPrestoNativeArrayFunctionQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
        queryRunner.installPlugin(new SqlInvokedFunctionsPlugin());
        return queryRunner;
    }

    @Test
    public void testArrayMaxBy()
    {
        assertQuery("SELECT array_max_by(ARRAY['a', 'bbb', 'cc'], x -> length(x))");
        assertQuery("SELECT array_max_by(ARRAY['aa', 'bb', 'c'], x -> length(x))");
        assertQuery("SELECT array_max_by(ARRAY['a', NULL, 'bbb'], x -> length(x))");
        assertQuery("SELECT array_max_by(ARRAY[NULL, NULL], x -> length(x))");
        assertQuery("SELECT array_max_by(ARRAY[], x -> x)");
        assertQuery("SELECT array_max_by(ARRAY[-10, 5, 7], x -> abs(x))");
        assertQuery("SELECT array_max_by(ARRAY[1, 2, 3], x -> IF(x = 2, NULL, x))");
    }

    @Test
    public void testArrayMinBy()
    {
        assertQuery("SELECT array_min_by(ARRAY['a', 'bbb', 'cc'], x -> length(x))");
        assertQuery("SELECT array_min_by(ARRAY['aa', 'bb', 'c'], x -> length(x))");
        assertQuery("SELECT array_min_by(ARRAY['a', NULL, 'bbb'], x -> length(x))");
        assertQuery("SELECT array_min_by(ARRAY[NULL, NULL], x -> length(x))");
        assertQuery("SELECT array_min_by(ARRAY[], x -> x)");
        assertQuery("SELECT array_min_by(ARRAY[-10, 5, 7], x -> abs(x))");
        assertQuery("SELECT array_min_by(ARRAY[1, 2, 3], x -> IF(x = 2, NULL, x))");
    }

    @Test
    public void testArrayTopN()
    {
        assertQuery("SELECT array_top_n(ARRAY[1, 5, 3, 9, 2], 3)");
        assertQuery("SELECT array_top_n(ARRAY[1, 2], 5)");
        assertQuery("SELECT array_top_n(ARRAY[5, 1, 5, 3], 2)");
        assertQuery("SELECT array_top_n(ARRAY[1, NULL, 3, 2], 2)");
        assertQuery("SELECT array_top_n(ARRAY[1, 2, 3], 0)");
    }
}
