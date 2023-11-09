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
package com.facebook.presto.hive;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static io.airlift.tpch.TpchTable.getTables;

public class TestHiveDistributedQueriesWithOptimizedRepartitioning
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                getTables(),
                ImmutableMap.of(
                        "experimental.optimized-repartitioning", "true",
                        // Use small SerializedPages to force flushing
                        "driver.max-page-partitioning-buffer-size", "10000B"),
                Optional.empty());
    }

    @Override
    protected boolean supportsNotNullColumns()
    {
        return false;
    }

    @Override
    public void testDelete()
    {
        // Hive connector currently does not support row-by-row delete
    }

    @Override
    public void testInsertWithCoercion()
    {
        assertUpdate("CREATE TABLE test_insert_with_coercion (" +
                "tinyint_column TINYINT, " +
                "integer_column INTEGER, " +
                "decimal_column DECIMAL(5, 3), " +
                "real_column REAL, " +
                "char_column CHAR(3), " +
                "bounded_varchar_column VARCHAR(3), " +
                "unbounded_varchar_column VARCHAR, " +
                "date_column DATE)");

        assertUpdate("INSERT INTO test_insert_with_coercion (tinyint_column, integer_column, decimal_column, real_column) VALUES (1e0, 2e0, 3e0, 4e0)", 1);
        assertUpdate("INSERT INTO test_insert_with_coercion (char_column, bounded_varchar_column, unbounded_varchar_column) VALUES (CAST('aa     ' AS varchar), CAST('aa     ' AS varchar), CAST('aa     ' AS varchar))", 1);
        assertUpdate("INSERT INTO test_insert_with_coercion (date_column) VALUES (TIMESTAMP '2019-11-18 22:13:40')", 1);

        assertQuery(
                "SELECT * FROM test_insert_with_coercion",
                "VALUES " +
                        "(1, 2, 3, 4, NULL, NULL, NULL, NULL), " +
                        "(NULL, NULL, NULL, NULL, 'aa ', 'aa ', 'aa     ', NULL), " +
                        "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, DATE '2019-11-18')");

        assertQueryFails("INSERT INTO test_insert_with_coercion (integer_column) VALUES (3e9)", "Out of range for integer: 3.0E9");
        /* Hive silently truncates non-space characters when inserting, the following tests are N/A */
        //assertQueryFails("INSERT INTO test_insert_with_coercion (char_column) VALUES ('abcd')", "Cannot truncate non-space characters on INSERT");
        //assertQueryFails("INSERT INTO test_insert_with_coercion (bounded_varchar_column) VALUES ('abcd')", "Cannot truncate non-space characters on INSERT");

        assertUpdate("DROP TABLE test_insert_with_coercion");
    }
    // Hive specific tests should normally go in TestHiveIntegrationSmokeTest
}
