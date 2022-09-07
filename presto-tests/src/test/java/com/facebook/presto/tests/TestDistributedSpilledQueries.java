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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestDistributedSpilledQueries
        extends AbstractTestQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDistributedSpillingQueryRunner(null);
    }

    public static QueryRunner createDistributedSpillingQueryRunner(Map<String, String> extraSessionProperties)
            throws Exception
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.TASK_CONCURRENCY, "1")
                .setSystemProperty(SystemSessionProperties.MAX_DRIVERS_PER_TASK, "1");

        if (extraSessionProperties != null) {
            for (Map.Entry<String, String> e : extraSessionProperties.entrySet()) {
                sessionBuilder.setSystemProperty(e.getKey(), e.getValue());
            }
        }

        ImmutableMap<String, String> extraProperties = ImmutableMap.<String, String>builder()
                .put("experimental.spill-enabled", "true")
                .put("experimental.spiller-spill-path", Paths.get(System.getProperty("java.io.tmpdir"), "presto", "spills").toString())
                .put("experimental.spiller-max-used-space-threshold", "1.0")
//                .put("experimental.memory-revoking-threshold", "0.0") // revoke always
//                .put("experimental.memory-revoking-target", "0.0")
                .build();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner(sessionBuilder.build(), 1, extraProperties);

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

    @Test(enabled = false)
    @Override
    public void testAssignUniqueId()
    {
        // TODO: disabled until https://github.com/prestodb/presto/issues/8926 is resolved
        //       due to long running query test created many spill files on disk.
    }
}
