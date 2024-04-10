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

import static com.facebook.presto.tests.TestGroups.HIVE_LIST_CACHING;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static org.testng.Assert.assertEquals;

public class TestDirectoryListCacheInvalidation
        extends ProductTest
{
    @BeforeTestWithContext
    public void setUp()
    {
        query("DROP TABLE IF EXISTS hive_listcache.default.region_cache");
        query("CREATE TABLE hive_listcache.default.region_cache AS SELECT * FROM tpch.tiny.region");
    }

    @Test(groups = {HIVE_LIST_CACHING})
    public void testDirectoryListCacheInvalidation()
    {
        String jmxMetricsQuery = "SELECT sum(hitcount), sum(misscount) from jmx.current.\"com.facebook.presto.hive:name=hive_listcache,type=cachingdirectorylister\"";
        String regionQuery = "SELECT * FROM hive_listcache.default.region_cache";

        // Initial hitcount, misscount based on cache entries,
        QueryResult queryResult = query(jmxMetricsQuery);
        long hitCountInitial = (long) queryResult.row(0).get(0);
        long missCountInitial = (long) queryResult.row(0).get(1);

        for (int i = 0; i < 2; i++) {
            query(regionQuery);
        }

        QueryResult result = query(jmxMetricsQuery);

        long hitCount = (long) result.row(0).get(0);
        long missCount = (long) result.row(0).get(1);

        assertEquals(hitCount, hitCountInitial + 1);
        assertEquals(missCount, missCountInitial + 1);

        // Invalidate directory list cache
        query("CALL hive_listcache.system.invalidate_directory_list_cache()");

        query(regionQuery);
        result = query(jmxMetricsQuery);

        long hitCountAfter = (long) result.row(0).get(0);
        long missCountAfter = (long) result.row(0).get(1);
        // No results are cached, miss count would increase but hit count remains same
        assertEquals(hitCountAfter, hitCount);
        assertEquals(missCountAfter, missCount + 1);
    }

    @AfterTestWithContext
    public void tearDown()
    {
        query("DROP TABLE IF EXISTS hive_listcache.default.region_cache");
    }
}
