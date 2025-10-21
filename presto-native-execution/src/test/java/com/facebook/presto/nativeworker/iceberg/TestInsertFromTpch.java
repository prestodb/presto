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
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.nativeworker.NativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;

public class TestInsertFromTpch
        extends AbstractTestQueryFramework
{
    private static final String TPCH_SCHEMA = "iceberg.tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        QueryRunner javaQueryRunner = javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setAddStorageFormatToPath(false)
                .build();
        NativeQueryRunnerUtils.createAllIcebergTables(javaQueryRunner);
        return javaQueryRunner;
    }

    @Test
    public void testInsertFromTpchNation()
    {
        String targetTable = "iceberg_nation";
        int iterations = 10;
        String sourceTable = String.format("%s.%s", TPCH_SCHEMA, "nation");
        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "nationkey BIGINT, " +
                    "name VARCHAR, " +
                    "regionkey BIGINT, " +
                    "comment VARCHAR" +
                    ") WITH (format = 'PARQUET')", targetTable));

            long rowCount = (Long) computeActual(String.format("SELECT COUNT(*) FROM %s", sourceTable)).getOnlyValue();
            for (int i = 1; i <= iterations; i++) {
                assertUpdate(String.format("INSERT INTO %s SELECT * FROM %s", targetTable, sourceTable), rowCount);
                assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable),
                        String.format("VALUES (BIGINT '%d')", rowCount * i));
            }

            assertQuery(String.format("SELECT nationkey, COUNT(*) as cnt FROM %s GROUP BY nationkey ORDER BY nationkey", targetTable),
                    String.format("SELECT nationkey, BIGINT '10' as cnt FROM %s ORDER BY nationkey", sourceTable));

            assertQuery(String.format("SELECT DISTINCT nationkey, name, comment FROM %s ORDER BY nationkey", targetTable),
                    String.format("SELECT nationkey, name, comment FROM %s ORDER BY nationkey", sourceTable));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
        }
    }

    @Test
    public void testInsertFromTpchRegion()
    {
        String targetTable = "iceberg_region";
        int iterations = 10;
        String sourceTable = String.format("%s.%s", TPCH_SCHEMA, "region");

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "regionkey BIGINT, " +
                    "name VARCHAR, " +
                    "comment VARCHAR" +
                    ") WITH (format = 'PARQUET')", targetTable));

            // First insert: filter that returns 0 rows (edge case)
            assertUpdate(String.format("INSERT INTO %s SELECT * FROM %s WHERE regionkey > 999", targetTable, sourceTable), 0);

            // Verify table is still empty after 0-row insert
            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '0')");

            // Multiple inserts: insert all regions 10 times
            long rowCount = (Long) computeActual(String.format("SELECT COUNT(*) FROM %s", sourceTable)).getOnlyValue();
            for (int i = 1; i <= iterations; i++) {
                assertUpdate(String.format("INSERT INTO %s SELECT * FROM %s", targetTable, sourceTable), rowCount);
                assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable),
                        String.format("VALUES (BIGINT '%d')", rowCount * i));
            }

            assertQuery(String.format("SELECT regionkey, COUNT(*) as cnt FROM %s GROUP BY regionkey ORDER BY regionkey", targetTable),
                    String.format("SELECT regionkey, BIGINT '10' as cnt FROM %s ORDER BY regionkey", sourceTable));

            assertQuery(String.format("SELECT DISTINCT regionkey, name, comment FROM %s ORDER BY regionkey", targetTable),
                    String.format("SELECT regionkey, name, comment FROM %s ORDER BY regionkey", sourceTable));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
        }
    }

    @Test
    public void testInsertFromTpchCustomerWithFilter()
    {
        String targetTable = "iceberg_customer_filtered";
        String sourceTable = String.format("%s.%s", TPCH_SCHEMA, "customer");

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "custkey BIGINT, " +
                    "name VARCHAR, " +
                    "nationkey BIGINT, " +
                    "acctbal DOUBLE" +
                    ") WITH (format = 'PARQUET')", targetTable));

            // filter that returns 0 rows
            assertUpdate(String.format("INSERT INTO %s " +
                    "SELECT custkey, name, nationkey, acctbal " +
                    "FROM %s " +
                    "WHERE acctbal > 999999999", targetTable, sourceTable), 0);

            // Verify table is still empty after 0-row insert
            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable), "VALUES (BIGINT '0')");

            long rowCount = (Long) computeActual(String.format("SELECT COUNT(*) FROM %s WHERE acctbal > 0", sourceTable)).getOnlyValue();
            assertUpdate(String.format("INSERT INTO %s " +
                    "SELECT custkey, name, nationkey, acctbal " +
                    "FROM %s " +
                    "WHERE acctbal > 0", targetTable, sourceTable), rowCount);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable),
                    String.format("SELECT COUNT(*) FROM %s WHERE acctbal > 0", sourceTable));

            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE acctbal <= 0", targetTable), "VALUES (BIGINT '0')");

            assertQuery(String.format("SELECT * FROM %s ORDER BY custkey LIMIT 5", targetTable),
                    String.format("SELECT custkey, name, nationkey, acctbal FROM %s WHERE acctbal > 0 ORDER BY custkey LIMIT 5", sourceTable));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
        }
    }

    @Test
    public void testInsertFromTpchOrdersWithProjection()
    {
        String targetTable = "iceberg_orders_summary";
        String sourceTable = String.format("%s.%s", TPCH_SCHEMA, "orders");

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "orderkey BIGINT, " +
                    "custkey BIGINT, " +
                    "orderstatus VARCHAR, " +
                    "totalprice DOUBLE, " +
                    "orderdate VARCHAR, " +
                    "orderyear VARCHAR" +
                    ") WITH (format = 'PARQUET')", targetTable));

            long rowCount = (Long) computeActual(String.format("SELECT COUNT(*) FROM %s WHERE totalprice > 100000", sourceTable)).getOnlyValue();
            assertUpdate(String.format("INSERT INTO %s " +
                    "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderdate " +
                    "FROM %s " +
                    "WHERE totalprice > 100000", targetTable, sourceTable), rowCount);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable),
                    String.format("SELECT COUNT(*) FROM %s WHERE totalprice > 100000", sourceTable));

            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE totalprice <= 100000", targetTable), "VALUES (BIGINT '0')");

            assertQuery(String.format("SELECT DISTINCT orderyear FROM %s ORDER BY orderyear", targetTable),
                    String.format("SELECT DISTINCT orderdate FROM %s WHERE totalprice > 100000 ORDER BY orderdate", sourceTable));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
        }
    }

    @Test
    public void testInsertFromTpchLineitemWithAggregation()
    {
        String targetTable = "iceberg_lineitem_summary";
        String sourceTable = String.format("%s.%s", TPCH_SCHEMA, "lineitem");

        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "orderkey BIGINT, " +
                    "total_quantity DOUBLE, " +
                    "total_price DOUBLE, " +
                    "avg_discount DOUBLE, " +
                    "item_count BIGINT" +
                    ") WITH (format = 'PARQUET')", targetTable));

            long rowCount = (Long) computeActual(String.format("SELECT COUNT(DISTINCT orderkey) FROM %s", sourceTable)).getOnlyValue();
            assertUpdate(String.format("INSERT INTO %s " +
                    "SELECT " +
                    "  orderkey, " +
                    "  SUM(quantity) as total_quantity, " +
                    "  SUM(extendedprice) as total_price, " +
                    "  AVG(discount) as avg_discount, " +
                    "  COUNT(*) as item_count " +
                    "FROM %s " +
                    "GROUP BY orderkey", targetTable, sourceTable), rowCount);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable),
                    String.format("SELECT COUNT(DISTINCT orderkey) FROM %s", sourceTable));

            assertQuery(String.format("SELECT total_quantity, item_count FROM %s WHERE orderkey = 1", targetTable),
                    String.format("SELECT SUM(quantity), COUNT(*) FROM %s WHERE orderkey = 1", sourceTable));

            assertQuery(String.format("SELECT COUNT(*) FROM %s WHERE item_count <= 0", targetTable), "VALUES (BIGINT '0')");
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
        }
    }

    @Test
    public void testInsertFromTpchJoin()
    {
        String targetTable = "iceberg_customer_nation";
        String sourceCustomer = String.format("%s.%s", TPCH_SCHEMA, "customer");
        String sourceNation = String.format("%s.%s", TPCH_SCHEMA, "nation");
        String sourceRegion = String.format("%s.%s", TPCH_SCHEMA, "region");
        try {
            assertUpdate(String.format("CREATE TABLE %s (" +
                    "custkey BIGINT, " +
                    "customer_name VARCHAR, " +
                    "nation_name VARCHAR, " +
                    "region_name VARCHAR, " +
                    "acctbal DOUBLE" +
                    ") WITH (format = 'PARQUET')", targetTable));

            long rowCount = (Long) computeActual(String.format("SELECT COUNT(*) " +
                    "FROM %s c " +
                    "JOIN %s n ON c.nationkey = n.nationkey " +
                    "JOIN %s r ON n.regionkey = r.regionkey", sourceCustomer, sourceNation, sourceRegion)).getOnlyValue();
            assertUpdate(String.format("INSERT INTO %s " +
                    "SELECT " +
                    "  c.custkey, " +
                    "  c.name as customer_name, " +
                    "  n.name as nation_name, " +
                    "  r.name as region_name, " +
                    "  c.acctbal " +
                    "FROM %s c " +
                    "JOIN %s n ON c.nationkey = n.nationkey " +
                    "JOIN %s r ON n.regionkey = r.regionkey", targetTable, sourceCustomer, sourceNation, sourceRegion), rowCount);

            assertQuery(String.format("SELECT COUNT(*) FROM %s", targetTable),
                    String.format("SELECT COUNT(*) " +
                            "FROM %s c " +
                            "JOIN %s n ON c.nationkey = n.nationkey " +
                            "JOIN %s r ON n.regionkey = r.regionkey", sourceCustomer, sourceNation, sourceRegion));

            assertQuery(String.format("SELECT customer_name, nation_name, region_name FROM %s WHERE custkey = 1", targetTable),
                    String.format("SELECT c.name, n.name, r.name " +
                            "FROM %s c " +
                            "JOIN %s n ON c.nationkey = n.nationkey " +
                            "JOIN %s r ON n.regionkey = r.regionkey " +
                            "WHERE c.custkey = 1", sourceCustomer, sourceNation, sourceRegion));

            assertQuery(String.format("SELECT DISTINCT region_name FROM %s ORDER BY region_name", targetTable),
                    String.format("SELECT DISTINCT r.name FROM %s r ORDER BY r.name", sourceRegion));
        }
        finally {
            assertUpdate(String.format("DROP TABLE IF EXISTS %s", targetTable));
        }
    }
}
