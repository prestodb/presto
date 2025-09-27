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

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.redis.util.EmbeddedRedis;
import com.facebook.presto.redis.util.JsonEncoder;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.security.AllowAllAccessControl;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.StandaloneQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;

import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.redis.util.RedisTestUtils.createEmptyTableDescriptions;
import static com.facebook.presto.redis.util.RedisTestUtils.createTableDescriptionsWithColumns;
import static com.facebook.presto.redis.util.RedisTestUtils.installRedisPlugin;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertContains;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRedisIntegrationMixedCase
{
    private static final Session SESSION = testSessionBuilder()
            .setCatalog("redis")
            .setSchema("default")
            .build();

    private EmbeddedRedis embeddedRedis;
    private final String lowerCaseTable = "test";
    private final String camelCaseTable = "Test";
    private final String upperCaseTable = "TEST";
    private final String testTable = "testTable";
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
        this.queryRunner = new StandaloneQueryRunner(SESSION);

        installRedisPlugin(
                embeddedRedis,
                queryRunner,
                ImmutableMap.<SchemaTableName, RedisTableDescription>builder()
                        .putAll(createEmptyTableDescriptions(
                                new SchemaTableName("default", lowerCaseTable),
                                new SchemaTableName("default", camelCaseTable),
                                new SchemaTableName("default", upperCaseTable)))
                        .putAll(createTableDescriptionsWithColumns(
                                ImmutableMap.of(
                                        new SchemaTableName("default", testTable),
                                        ImmutableList.of(
                                                new RedisTableFieldDescription("id", BIGINT, "id", null, null, null, false),
                                                new RedisTableFieldDescription("name", VARCHAR, "name", null, null, null, false),
                                                new RedisTableFieldDescription("NAME", VARCHAR, "NAME", null, null, null, false),
                                                new RedisTableFieldDescription("Address", VARCHAR, "Address", null, null, null, false),
                                                new RedisTableFieldDescription("age", BIGINT, "age", null, null, null, false),
                                                new RedisTableFieldDescription("BAND", VARCHAR, "BAND", null, null, null, false)))))
                        .build(),
                ImmutableMap.of("case-sensitive-name-matching", "true"));
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
                jedis.set(lowerCaseTable + ":" + i, jsonEncoder.toString(value));
                jedis.set(camelCaseTable + ":" + i, jsonEncoder.toString(value));
                jedis.set(upperCaseTable + ":" + i, jsonEncoder.toString(value));
            }
        }
    }

    @Test
    public void testTableExists()
    {
        QualifiedObjectName lowerCaseObjName = new QualifiedObjectName("redis", "default", lowerCaseTable);
        QualifiedObjectName camelCaseObjName = new QualifiedObjectName("redis", "default", camelCaseTable);
        QualifiedObjectName upperCaseObjName = new QualifiedObjectName("redis", "default", upperCaseTable);
        transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> lowerCaseHandle = queryRunner.getServer().getMetadata().getMetadataResolver(session).getTableHandle(lowerCaseObjName);
                    Optional<TableHandle> camelCaseHandle = queryRunner.getServer().getMetadata().getMetadataResolver(session).getTableHandle(camelCaseObjName);
                    Optional<TableHandle> upperCaseHandle = queryRunner.getServer().getMetadata().getMetadataResolver(session).getTableHandle(upperCaseObjName);
                    assertTrue(lowerCaseHandle.isPresent());
                    assertTrue(camelCaseHandle.isPresent());
                    assertTrue(upperCaseHandle.isPresent());
                });
    }

    private void assertTableCount(String tableName, long expectedCount)
    {
        MaterializedResult result = queryRunner.execute("SELECT count(1) from " + tableName);
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BIGINT).row(expectedCount).build();
        assertEquals(result, expected);
    }

    @Test
    public void testTableHasData()
    {
        assertTableCount(lowerCaseTable, 0L);
        assertTableCount(camelCaseTable, 0L);
        assertTableCount(upperCaseTable, 0L);

        int count = 10;
        populateData(count);

        assertTableCount(lowerCaseTable, (long) count);
        assertTableCount(camelCaseTable, (long) count);
        assertTableCount(upperCaseTable, (long) count);
    }

    @Test
    public void testShowTables()
    {
        MaterializedResult result = queryRunner.execute("show tables from redis.default");
        assertEquals(result.getRowCount(), 4);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, createUnboundedVarcharType())
                .row("test")
                .row("Test")
                .row("TEST")
                .row("testTable")
                .build();

        assertContains(result, expected);
    }

    @Test
    public void testShowColumns()
    {
        MaterializedResult result = queryRunner.execute("show columns from redis.default.testTable");

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, createUnboundedVarcharType())
                .row("id", "bigint", "", "", Long.valueOf(19), null, null)
                .row("name", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("NAME", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("Address", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .row("age", "bigint", "", "", Long.valueOf(19), null, null)
                .row("BAND", "varchar", "", "", null, null, Long.valueOf(2147483647))
                .build();
        assertContains(result, expected);
    }

    private void populateTestTable(long count)
    {
        JsonEncoder jsonEncoder = new JsonEncoder();
        for (long i = 0; i < count; i++) {
            Object value = ImmutableMap.of("id", Long.toString(i), "name", "name_value_" + i, "NAME", "NAME_value_" + i, "Address", "Address_value_" + i, "age", Long.toString(25), "BAND", "BAND_value_" + i);
            try (Jedis jedis = embeddedRedis.getJedisPool().getResource()) {
                jedis.set(testTable + ":" + i, jsonEncoder.toString(value));
            }
        }
    }

    @Test
    public void testSelect()
    {
        populateTestTable(3);
        MaterializedResult result = queryRunner.execute("SELECT * FROM " + testTable);

        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BIGINT, createUnboundedVarcharType(), createUnboundedVarcharType())
                .row(Long.valueOf(0), "name_value_0", "NAME_value_0", "Address_value_0", Long.valueOf(25), "BAND_value_0")
                .row(Long.valueOf(1), "name_value_1", "NAME_value_1", "Address_value_1", Long.valueOf(25), "BAND_value_1")
                .row(Long.valueOf(2), "name_value_2", "NAME_value_2", "Address_value_2", Long.valueOf(25), "BAND_value_2")
                .build();
        assertContains(result, expected);
    }
}
