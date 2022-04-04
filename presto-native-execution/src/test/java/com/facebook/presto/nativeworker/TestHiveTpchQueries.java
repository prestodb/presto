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
package com.facebook.presto.nativeworker;

import com.facebook.presto.hive.HiveExternalWorkerQueryRunner;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertNotNull;

public abstract class TestHiveTpchQueries
        extends AbstractTestQueryFramework
{
    public static QueryRunner createNativeQueryRunner(boolean useThrift)
            throws Exception
    {
        String prestoServerPath = System.getProperty("PRESTO_SERVER");
        String baseDataDir = System.getProperty("DATA_DIR");
        String workerCount = System.getProperty("WORKER_COUNT");
        int cacheMaxSize = 0;

        assertNotNull(prestoServerPath);
        assertNotNull(baseDataDir);

        return HiveExternalWorkerQueryRunner.createNativeQueryRunner(baseDataDir, prestoServerPath, Optional.ofNullable(workerCount).map(Integer::parseInt), cacheMaxSize, useThrift);
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        String baseDataDir = System.getProperty("DATA_DIR");
        return HiveExternalWorkerQueryRunner.createJavaQueryRunner(Optional.of(Paths.get(baseDataDir)));
    }

    private static String getTpchQuery(int q)
            throws IOException
    {
        String sql = Resources.toString(Resources.getResource("tpch/queries/q" + q + ".sql"), UTF_8);
        sql = sql.replaceFirst("(?m);$", "");
        return sql;
    }

    // This test runs the 22 TPC-H queries.

    @Test
    public void testTpchQ1()
            throws Exception
    {
        assertQuery(getTpchQuery(1));
    }

    @Test
    public void testTpchQ2()
            throws Exception
    {
        assertQuery(getTpchQuery(2));
    }

    @Test
    public void testTpchQ3()
            throws Exception
    {
        assertQuery(getTpchQuery(3));
    }

    @Test
    public void testTpchQ4()
            throws Exception
    {
        assertQuery(getTpchQuery(4));
    }

    @Test
    public void testTpchQ5()
            throws Exception
    {
        assertQuery(getTpchQuery(5));
    }

    @Test
    public void testTpchQ6()
            throws Exception
    {
        assertQuery(getTpchQuery(6));
    }

    @Test
    public void testTpchQ7()
            throws Exception
    {
        assertQuery(getTpchQuery(7));
    }

    @Test
    public void testTpchQ8()
            throws Exception
    {
        assertQuery(getTpchQuery(8));
    }

    @Test
    public void testTpchQ9()
            throws Exception
    {
        assertQuery(getTpchQuery(9));
    }

    @Test
    public void testTpchQ10()
            throws Exception
    {
        assertQuery(getTpchQuery(10));
    }

    @Test
    public void testTpchQ11()
            throws Exception
    {
        assertQuery(getTpchQuery(11));
    }

    @Test
    public void testTpchQ12()
            throws Exception
    {
        assertQuery(getTpchQuery(12));
    }

    @Test
    public void testTpchQ13()
            throws Exception
    {
        assertQuery(getTpchQuery(13));
    }

    @Test
    public void testTpchQ14()
            throws Exception
    {
        assertQuery(getTpchQuery(14));
    }

    @Test
    public void testTpchQ15()
            throws Exception
    {
        // Q15 doesn't reliably return correct results.
        // The same issue is observed with Presto java also.
        // The errors are on account of 2 causes:
        //  i) WITH expansion in Presto expands the query in place each time.
        //     As per SQL spec, the expansion should happen only once.
        // ii) On account of the double expansion, the aggregate value with double
        //     type has minor differences in each expansion causing the
        //     subquery to not always find an equal match in values.
        // Creating a table with the revenue SQL always returns correct results,
        assertQuerySucceeds(getTpchQuery(15));
    }

    @Test
    public void testTpchQ16()
            throws Exception
    {
        assertQuery(getTpchQuery(16));
    }

    @Test
    public void testTpchQ17()
            throws Exception
    {
        assertQuery(getTpchQuery(17));
    }

    @Test
    public void testTpchQ18()
            throws Exception
    {
        assertQuery(getTpchQuery(18));
    }

    @Test
    public void testTpchQ19()
            throws Exception
    {
        assertQuery(getTpchQuery(19));
    }

    @Test
    public void testTpchQ20()
            throws Exception
    {
        assertQuery(getTpchQuery(20));
    }

    @Test
    public void testTpchQ21()
            throws Exception
    {
        assertQuery(getTpchQuery(21));
    }

    @Test
    public void testTpchQ22()
            throws Exception
    {
        assertQuery(getTpchQuery(22));
    }
}
