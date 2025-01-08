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
package com.facebook.presto.tests.hive;

import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.HIVE_CACHING;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static org.testng.Assert.assertEquals;

public class TestOrcCacheInvalidation
        extends ProductTest
{
    @BeforeTestWithContext
    public void setUp()
    {
        query("DROP TABLE IF EXISTS hivecached.default.test_orc_cache_invalidation");
        query("CREATE TABLE hivecached.default.test_orc_cache_invalidation AS SELECT * FROM tpch.tiny.region");
    }

    @Test(groups = {HIVE_CACHING})
    public void testOrcCacheInvalidation()
    {
        String fileTailCacheMetricsQuery = "SELECT sum(hitcount), sum(misscount) from jmx.current.\"com.facebook.presto.hive:name=hivecached_orcfiletail,type=cachestatsmbean\"";
        String stripeFooterCacheMetricsQuery = "SELECT sum(hitcount), sum(misscount) from jmx.current.\"com.facebook.presto.hive:name=hivecached_stripefooter,type=cachestatsmbean\"";
        String stripeStreamCacheMetricsQuery = "SELECT sum(hitcount), sum(misscount) from jmx.current.\"com.facebook.presto.hive:name=hivecached_stripestream,type=cachestatsmbean\"";
        String testQuery = "SELECT * FROM hivecached.default.test_orc_cache_invalidation";

        // Initial hitcount, misscount based on cache entries,
        QueryResult orcFileTailCacheMetrics = query(fileTailCacheMetricsQuery);
        QueryResult stripeFooterCacheMetrics = query(stripeFooterCacheMetricsQuery);
        QueryResult stripeStreamCacheMetrics = query(stripeStreamCacheMetricsQuery);

        long fileTailHitCountCold = (long) orcFileTailCacheMetrics.row(0).get(0);
        long fileTailMissCountCold = (long) orcFileTailCacheMetrics.row(0).get(1);
        long stripeFooterHitCountCold = (long) stripeFooterCacheMetrics.row(0).get(0);
        long stripeFooterMissCountCold = (long) stripeFooterCacheMetrics.row(0).get(1);
        long stripeStreamHitCountCold = (long) stripeStreamCacheMetrics.row(0).get(0);
        long stripeStreamMissCountCold = (long) stripeStreamCacheMetrics.row(0).get(1);

        for (int i = 0; i < 2; i++) {
            query(testQuery);
        }

        orcFileTailCacheMetrics = query(fileTailCacheMetricsQuery);
        stripeFooterCacheMetrics = query(stripeFooterCacheMetricsQuery);
        stripeStreamCacheMetrics = query(stripeStreamCacheMetricsQuery);

        long fileTailHitCountWarm = (long) orcFileTailCacheMetrics.row(0).get(0);
        long fileTailMissCountWarm = (long) orcFileTailCacheMetrics.row(0).get(1);
        long stripeFooterHitCountWarm = (long) stripeFooterCacheMetrics.row(0).get(0);
        long stripeFooterMissCountWarm = (long) stripeFooterCacheMetrics.row(0).get(1);
        long stripeStreamHitCountWarm = (long) stripeStreamCacheMetrics.row(0).get(0);
        long stripeStreamMissCountWarm = (long) stripeStreamCacheMetrics.row(0).get(1);

        assertEquals(fileTailHitCountWarm, fileTailHitCountCold + 1);
        assertEquals(fileTailMissCountWarm, fileTailMissCountCold + 1);
        assertEquals(stripeFooterHitCountWarm, stripeFooterHitCountCold + 1);
        assertEquals(stripeFooterMissCountWarm, stripeFooterMissCountCold + 1);
        assertEquals(stripeStreamHitCountWarm, stripeStreamHitCountCold + 2);
        assertEquals(stripeStreamMissCountWarm, stripeStreamMissCountCold + 2);

        // Invalidate ORC cache
        query("CALL hivecached.system.invalidate_orc_cache()");

        query(testQuery);

        orcFileTailCacheMetrics = query(fileTailCacheMetricsQuery);
        stripeFooterCacheMetrics = query(stripeFooterCacheMetricsQuery);
        stripeStreamCacheMetrics = query(stripeStreamCacheMetricsQuery);

        long fileTailHitCountAfter = (long) orcFileTailCacheMetrics.row(0).get(0);
        long fileTailMissCountAfter = (long) orcFileTailCacheMetrics.row(0).get(1);
        long stripeFooterHitCountAfter = (long) stripeFooterCacheMetrics.row(0).get(0);
        long stripeFooterMissCountAfter = (long) stripeFooterCacheMetrics.row(0).get(1);
        long stripeStreamHitCountAfter = (long) stripeStreamCacheMetrics.row(0).get(0);
        long stripeStreamMissCountAfter = (long) stripeStreamCacheMetrics.row(0).get(1);

        // No results are cached, miss count would increase but hit count remains same
        assertEquals(fileTailHitCountAfter, fileTailHitCountWarm);
        assertEquals(fileTailMissCountAfter, fileTailMissCountWarm + 1);
        assertEquals(stripeFooterHitCountAfter, stripeFooterHitCountWarm);
        assertEquals(stripeFooterMissCountAfter, stripeFooterMissCountWarm + 1);
        assertEquals(stripeStreamHitCountAfter, stripeStreamHitCountWarm);
        assertEquals(stripeStreamMissCountAfter, stripeStreamMissCountWarm + 2);
    }

    @AfterTestWithContext
    public void tearDown()
    {
        query("DROP TABLE IF EXISTS hive_listcache.default.test_orc_cache_invalidation");
    }
}
