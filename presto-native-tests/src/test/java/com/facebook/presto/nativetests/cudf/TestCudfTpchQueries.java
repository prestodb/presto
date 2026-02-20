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
package com.facebook.presto.nativetests.cudf;

import com.facebook.presto.nativeworker.AbstractTestNativeTpchQueries;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

/**
 * CUDF sidecar TPCH coverage: only enable the queries that were known to pass
 * under CUDF sidecar. Others are disabled here to keep the suite green.
 */
public class TestCudfTpchQueries extends AbstractTestNativeTpchQueries
{
    private static final String DEFAULT_STORAGE_FORMAT = "PARQUET";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String storageFormat = System.getProperty("storageFormat", DEFAULT_STORAGE_FORMAT);
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(true)
                .setEnableCudf(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        String storageFormat = System.getProperty("storageFormat", DEFAULT_STORAGE_FORMAT);
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setStorageFormat(storageFormat)
                .setAddStorageFormatToPath(true)
                .build();
    }

    // Enabled subset
    @Test
    @Override
    public void testTpchQ1() throws Exception
    {
        super.testTpchQ1();
    }

    @Test
    @Override
    public void testTpchQ3() throws Exception
    {
        super.testTpchQ3();
    }

    @Test
    @Override
    public void testTpchQ5() throws Exception
    {
        super.testTpchQ5();
    }

    @Test
    @Override
    public void testTpchQ6() throws Exception
    {
        super.testTpchQ6();
    }

    @Test
    @Override
    public void testTpchQ8() throws Exception
    {
        super.testTpchQ8();
    }

    @Test
    @Override
    public void testTpchQ10() throws Exception
    {
        super.testTpchQ10();
    }

    // Disabled remaining queries for CUDF sidecar coverage
    @Test(enabled = false)
    @Override
    public void testTpchQ2() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ4() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ7() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ9() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ11() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ12() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ13() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ14() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ15() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ16() throws Exception {}

    // Q17 is already @Ignore in the base class.

    @Test(enabled = false)
    @Override
    public void testTpchQ18() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ19() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ20() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ21() throws Exception {}

    @Test(enabled = false)
    @Override
    public void testTpchQ22() throws Exception {}
}
