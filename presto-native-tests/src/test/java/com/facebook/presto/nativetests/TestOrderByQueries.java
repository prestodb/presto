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

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestOrderByQueries;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.Boolean.parseBoolean;

public class TestOrderByQueries
        extends AbstractTestOrderByQueries
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
    protected QueryRunner createQueryRunner() throws Exception
    {
        return NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test(enabled = false)
    public void testOrderByWithOutputColumnReferenceInLambdas() {}
}
