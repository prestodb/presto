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

import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestOrderByQueries;
import org.testng.annotations.Test;

public class TestOrderByQueries
        extends AbstractTestOrderByQueries
{
    private static final String storageFormat = "PARQUET";

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(true, storageFormat);
    }

    @Override
    protected void createTables()
    {
        try {
            QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner(storageFormat);
            NativeQueryRunnerUtils.createAllTables(javaQueryRunner, false);
            javaQueryRunner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @Test
    public void testOrderByWithOutputColumnReferenceInLambdas()
    {
        assertQueryFails("SELECT x AS y FROM (values (1,2), (2,3)) t(x, y) GROUP BY x ORDER BY apply(x, x -> -x) + 2*x", ".*Scalar function name not registered: presto.default.apply.*");
        assertQueryFails("SELECT -y AS x FROM (values (1,2), (2,3)) t(x, y) GROUP BY y ORDER BY apply(x, x -> -x)", ".*Scalar function name not registered: presto.default.apply.*");
        assertQueryFails("SELECT -y AS x FROM (values (1,2), (2,3)) t(x, y) GROUP BY y ORDER BY sum(apply(-y, x -> x * 1.0))", ".*Scalar function name not registered: presto.default.apply.*");
    }
}
