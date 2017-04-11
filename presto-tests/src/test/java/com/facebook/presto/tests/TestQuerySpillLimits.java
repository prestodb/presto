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
import com.google.common.io.Files;
import io.airlift.testing.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestQuerySpillLimits
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();

    private File spillPath;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        this.spillPath = Files.createTempDir();
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        FileUtils.deleteRecursively(spillPath);
    }

    @Test(timeOut = 240_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded local spill limit of 10B")
    public void testMaxSpillPerNodeLimit()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("experimental.spiller-spill-path", spillPath.getAbsolutePath())
                .put("experimental.spill-enabled", "true")
                .put("experimental.operator-memory-limit-before-spill", "1B")
                .put("experimental.max-spill-per-node", "10B")
                .put("experimental.spiller-max-used-space-threshold", "1.0")
                .build();
        try (QueryRunner queryRunner = createDistributedQueryRunner(SESSION, properties)) {
            queryRunner.execute(SESSION, "SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
        }
    }

    @Test(timeOut = 240_000, expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Query exceeded per-query local spill limit of 10B")
    public void testQueryMaxSpillPerNodeLimit()
            throws Exception
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("experimental.spiller-spill-path", spillPath.getAbsolutePath())
                .put("experimental.spill-enabled", "true")
                .put("experimental.operator-memory-limit-before-spill", "1B")
                .put("experimental.query-max-spill-per-node", "10B")
                .put("experimental.spiller-max-used-space-threshold", "1.0")
                .build();
        try (QueryRunner queryRunner = createDistributedQueryRunner(SESSION, properties)) {
            queryRunner.execute(SESSION, "SELECT COUNT(DISTINCT clerk) as count, orderdate FROM orders GROUP BY orderdate ORDER BY count, orderdate");
        }
    }

    private static DistributedQueryRunner createDistributedQueryRunner(Session session, Map<String, String> properties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 1, properties);

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
