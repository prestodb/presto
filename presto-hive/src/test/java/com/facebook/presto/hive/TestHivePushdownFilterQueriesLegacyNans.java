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
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.tpch.TpchTable.getTables;

public class TestHivePushdownFilterQueriesLegacyNans
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.createQueryRunner(getTables(),
                ImmutableMap.of("experimental.pushdown-subfields-enabled", "true",
                        "experimental.pushdown-dereference-enabled", "true",
                        "use-new-nan-definition", "false"),
                "sql-standard",
                ImmutableMap.of("hive.pushdown-filter-enabled", "true",
                        "hive.enable-parquet-dereference-pushdown", "true",
                        "hive.partial_aggregation_pushdown_enabled", "true",
                        "hive.partial_aggregation_pushdown_for_variable_length_datatypes_enabled", "true"),
                Optional.empty());

        return queryRunner;
    }

    @Test
    public void testNans()
    {
        assertUpdate("CREATE TABLE test_nan (double_value DOUBLE, float_value REAL)");
        try {
            assertUpdate("INSERT INTO test_nan VALUES (CAST('NaN' as DOUBLE), CAST('NaN' as REAL)), ((1, 1)), ((2, 2))", 3);
            assertQuery("SELECT double_value FROM test_nan WHERE double_value != 1", "SELECT CAST('NaN' as DOUBLE) UNION SELECT 2");
            assertQuery("SELECT float_value FROM test_nan WHERE float_value != 1", "SELECT CAST('NaN' as REAL) UNION SELECT 2");
            assertQuery("SELECT double_value FROM test_nan WHERE double_value NOT IN (1, 2)", "SELECT CAST('NaN' as DOUBLE)");
            assertQuery("SELECT double_value FROM test_nan WHERE double_value > 1", "SELECT 2.0");
            assertQuery("SELECT float_value FROM test_nan WHERE float_value > 1", "SELECT  CAST(2.0 AS REAL)");
        }
        finally {
            assertUpdate("DROP TABLE test_nan");
        }
    }
}
