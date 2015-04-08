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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Locale.ENGLISH;

public class TestClusterMemoryManager
{
    @Test(timeOut = 30_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded max memory size of 1kB.*")
    public void testQueryMemoryLimit()
            throws Exception
    {
        Session session = Session.builder()
                .setUser("user")
                .setSource("test")
                .setCatalog("tpch")
                // Use sf1000 to make sure this takes at least one second, so that the memory manager will fail the query
                .setSchema("sf1000")
                .setTimeZoneKey(UTC_KEY)
                .setLocale(ENGLISH)
                .build();

        try (QueryRunner queryRunner = createQueryRunner(session)) {
            queryRunner.execute(session, "SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
        }
    }

    private static DistributedQueryRunner createQueryRunner(Session session)
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("experimental.cluster-memory-manager-enabled", "true")
                .put("query.max-memory", "1kB")
                .put("task.operator-pre-allocated-memory", "0B")
                .build();
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 2, properties);

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }
}
