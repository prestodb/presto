package com.facebook.presto.hive;
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

import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.createBucketedSession;
import static com.facebook.presto.hive.HiveQueryRunner.createMaterializeExchangesSession;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.PART_SUPPLIER;

public class TestHivePushdownIntegrationSmokeTest
        extends TestHiveIntegrationSmokeTest
{
    public TestHivePushdownIntegrationSmokeTest()
    {
        super(createBucketedSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))),
                createMaterializeExchangesSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))),
                HIVE_CATALOG,
                new HiveTypeTranslator());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, CUSTOMER, LINE_ITEM, PART_SUPPLIER, NATION),
                ImmutableMap.of("experimental.pushdown-subfields-enabled", "true",
                        "experimental.pushdown-dereference-enabled", "true"),
                "sql-standard",
                ImmutableMap.of("hive.pushdown-filter-enabled", "true",
                        "hive.enable-parquet-dereference-pushdown", "true",
                        "hive.partial_aggregation_pushdown_enabled", "true",
                        "hive.partial_aggregation_pushdown_for_variable_length_datatypes_enabled", "true",
                        "hive.orc.writer.string-statistics-limit", "128B"),
                Optional.empty());
    }
}
