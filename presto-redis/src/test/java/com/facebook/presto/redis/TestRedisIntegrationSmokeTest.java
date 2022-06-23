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
package com.facebook.presto.redis;

import com.facebook.presto.redis.util.EmbeddedRedis;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestIntegrationSmokeTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static com.facebook.presto.redis.RedisQueryRunner.createRedisQueryRunner;
import static com.facebook.presto.redis.util.EmbeddedRedis.createEmbeddedRedis;
import static io.airlift.tpch.TpchTable.ORDERS;

@Test
public class TestRedisIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private EmbeddedRedis embeddedRedis;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        embeddedRedis = createEmbeddedRedis();
        return createRedisQueryRunner(embeddedRedis, "string", ORDERS);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        embeddedRedis.close();
    }
}
