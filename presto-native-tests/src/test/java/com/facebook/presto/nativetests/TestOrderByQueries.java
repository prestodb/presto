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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class TestOrderByQueries
        extends AbstractTestOrderByQueries
{
    @Parameters("storageFormat")
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createNativeQueryRunner(ImmutableMap.of(), System.getProperty("storageFormat"));
    }

    @Parameters("storageFormat")
    @Override
    protected void createTables()
    {
        try {
            String storageFormat = System.getProperty("storageFormat");
            QueryRunner javaQueryRunner = PrestoNativeQueryRunnerUtils.createJavaQueryRunner(storageFormat);
            if (storageFormat.equals("DWRF")) {
                NativeQueryRunnerUtils.createAllTables(javaQueryRunner, true);
            }
            else {
                NativeQueryRunnerUtils.createAllTables(javaQueryRunner, false);
            }
            javaQueryRunner.close();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test(enabled = false)
    public void testOrderByWithOutputColumnReferenceInLambdas() {}
}
