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
package io.prestosql.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.redis.util.EmbeddedRedis;
import io.prestosql.plugin.redis.util.JsonEncoder;
import io.prestosql.security.AllowAllAccessControl;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.BigintType;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.StandaloneQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;

import java.util.Optional;
import java.util.UUID;

import static io.prestosql.plugin.redis.util.RedisTestUtils.createEmptyTableDescription;
import static io.prestosql.plugin.redis.util.RedisTestUtils.installRedisPlugin;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("redis")
            .setSchema("default")
            .build();

    private EmbeddedRedis embeddedRedis;
    private String tableName;
    private StandaloneQueryRunner queryRunner;

    @BeforeClass
    public void startRedis()
            throws Exception
    {
        embeddedRedis = EmbeddedRedis.createEmbeddedRedis();
        embeddedRedis.start();
    }

    @AfterClass(alwaysRun = true)
    public void stopRedis()
    {
        embeddedRedis.close();
        embeddedRedis = null;
    }

    @BeforeMethod
    public void spinUp()
            throws Exception
    {
        this.tableName = "test_" + UUID.randomUUID().toString().replaceAll("-", "_");

        this.queryRunner = new StandaloneQueryRunner(SESSION);

        installRedisPlugin(embeddedRedis, queryRunner,
                ImmutableMap.<SchemaTableName, RedisTableDescription>builder()
                        .put(createEmptyTableDescription(new SchemaTableName("default", tableName)))
                        .build());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    private void populateData(int count)
    {
        JsonEncoder jsonEncoder = new JsonEncoder();
        for (long i = 0; i < count; i++) {
            Object value = ImmutableMap.of("id", Long.toString(i), "value", UUID.randomUUID().toString());
            try (Jedis jedis = embeddedRedis.getJedisPool().getResource()) {
                jedis.set(tableName + ":" + i, jsonEncoder.toString(value));
            }
        }
    }

    @Test
    public void testTableExists()
    {
        QualifiedObjectName name = new QualifiedObjectName("redis", "default", tableName);
        transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(session, name);
                    assertTrue(handle.isPresent());
                });
    }

    @Test
    public void testTableHasData()
    {
        MaterializedResult result = queryRunner.execute("SELECT count(1) from " + tableName);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();

        assertEquals(result, expected);

        int count = 1000;
        populateData(count);

        result = queryRunner.execute("SELECT count(1) from " + tableName);

        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row((long) count)
                .build();

        assertEquals(result, expected);
    }
}
