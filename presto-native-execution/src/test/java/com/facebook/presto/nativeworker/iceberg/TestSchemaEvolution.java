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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static java.lang.String.format;

public class TestSchemaEvolution
        extends IcebergPartitionTestBase
{
    private static final String BASE_TABLE = "page_views";

    @BeforeMethod
    public void setUp()
    {
        assertQuerySucceeds(format("DROP TABLE IF EXISTS %s", BASE_TABLE));
        assertQuerySucceeds(format(
                "CREATE TABLE %s (" +
                        "  user_id BIGINT, " +
                        "  page_url VARCHAR, " +
                        "  view_time TIMESTAMP, " +
                        "  session_id VARCHAR" +
                        ") WITH (format = 'PARQUET', partitioning = ARRAY['bucket(user_id, 4)'])", BASE_TABLE));

        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (1001, '/home', TIMESTAMP '2023-01-15 10:30:00', 'sess_001'), " +
                        "  (1002, '/products', TIMESTAMP '2023-01-15 11:45:30', 'sess_002'), " +
                        "  (1003, '/about', TIMESTAMP '2023-01-16 09:15:45', 'sess_003')", BASE_TABLE));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        assertQuerySucceeds(format("DROP TABLE IF EXISTS %s", BASE_TABLE));
    }

    @Test
    public void testBasicColumnOperations()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN zipcode VARCHAR", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s RENAME COLUMN zipcode TO location", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s DROP COLUMN location", BASE_TABLE));
        assertQuery(format("SELECT COUNT(*) FROM %s", BASE_TABLE), "VALUES (BIGINT '3')");
    }

    @Test
    public void testIdentityPartitionColumns()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN zipcode VARCHAR WITH (partitioning = 'identity')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN region_id INTEGER WITH (partitioning = 'identity')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN country_code BIGINT WITH (partitioning = 'identity')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN event_date DATE WITH (partitioning = 'identity')", BASE_TABLE));
        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (2001, '/search', TIMESTAMP '2023-02-01 14:20:00', 'sess_004', '12345', 1, 840, DATE '2023-02-01')", BASE_TABLE));
    }

    @Test
    public void testTruncatePartitionColumns()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN location VARCHAR WITH (partitioning = 'truncate(2)')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN category VARCHAR WITH (partitioning = 'truncate(5)')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN score INTEGER WITH (partitioning = 'truncate(10)')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN revenue BIGINT WITH (partitioning = 'truncate(1000)')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN price DECIMAL(10,2) WITH (partitioning = 'truncate(100)')", BASE_TABLE));
        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (3001, '/checkout', TIMESTAMP '2023-03-01 16:30:00', 'sess_005', 'US-CA', 'electronics', 85, 15000, DECIMAL '299.99')", BASE_TABLE));
    }

    @Test
    public void testBucketPartitionColumns()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN location VARCHAR WITH (partitioning = 'bucket(8)')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN user_segment INTEGER WITH (partitioning = 'bucket(4)')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN device_id BIGINT WITH (partitioning = 'bucket(16)')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN rating DECIMAL(3,1) WITH (partitioning = 'bucket(5)')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN hash_key VARBINARY WITH (partitioning = 'bucket(12)')", BASE_TABLE));
        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (4001, '/profile', TIMESTAMP '2023-04-01 12:15:00', 'sess_006', 'NY-NYC', 3, 987654321, DECIMAL '4.5', X'ABCDEF')", BASE_TABLE));
    }

    @Test
    public void testDateTimePartitionTransforms()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN dt DATE WITH (partitioning = 'year')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN ts TIMESTAMP WITH (partitioning = 'month')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN event_dt DATE WITH (partitioning = 'day')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN created_ts TIMESTAMP WITH (partitioning = 'hour')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN report_date DATE WITH (partitioning = 'month')", BASE_TABLE));
        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (5001, '/dashboard', TIMESTAMP '2023-05-15 08:30:00', 'sess_007', " +
                        "   DATE '2023-05-15', TIMESTAMP '2023-05-15 08:30:00', DATE '2023-05-15', " +
                        "   TIMESTAMP '2023-05-15 08:30:00', DATE '2023-05-01'), " +
                        "  (5002, '/reports', TIMESTAMP '2023-06-20 14:45:30', 'sess_008', " +
                        "   DATE '2023-06-20', TIMESTAMP '2023-06-20 14:45:30', DATE '2023-06-20', " +
                        "   TIMESTAMP '2023-06-20 14:45:30', DATE '2023-06-01')", BASE_TABLE));
    }

    @Test
    public void testMixedPartitionTransforms()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN region VARCHAR WITH (partitioning = 'identity')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN category VARCHAR WITH (partitioning = 'truncate(3)')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN bucket_id INTEGER WITH (partitioning = 'bucket(10)')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN event_year DATE WITH (partitioning = 'year')", BASE_TABLE));

        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (6001, '/analytics', TIMESTAMP '2023-07-10 10:00:00', 'sess_009', " +
                        "   'west', 'shopping', 42, DATE '2023-07-10'), " +
                        "  (6002, '/metrics', TIMESTAMP '2023-08-15 15:30:00', 'sess_010', " +
                        "   'east', 'browsing', 73, DATE '2023-08-15')", BASE_TABLE));

        assertQuery(format("SELECT COUNT(*) FROM %s WHERE region = 'west'", BASE_TABLE), "VALUES (BIGINT '1')");
        assertQuery(format("SELECT COUNT(*) FROM %s WHERE year(event_year) = 2023", BASE_TABLE), "VALUES (BIGINT '2')");
    }

    @Test
    public void testSchemaEvolutionWithExistingData()
    {
        assertQuery(format("SELECT COUNT(*) FROM %s", BASE_TABLE), "VALUES (BIGINT '3')");
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN status VARCHAR WITH (partitioning = 'identity')", BASE_TABLE));
        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (7001, '/new-feature', TIMESTAMP '2023-09-01 12:00:00', 'sess_011', 'active')", BASE_TABLE));

        assertQuery(format("SELECT COUNT(*) FROM %s WHERE status IS NULL", BASE_TABLE), "VALUES (BIGINT '3')");
        assertQuery(format("SELECT COUNT(*) FROM %s WHERE status = 'active'", BASE_TABLE), "VALUES (BIGINT '1')");
    }

    @Test
    public void testComplexDataTypes()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN amount DECIMAL(15,2) WITH (partitioning = 'identity')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN score DECIMAL(5,2) WITH (partitioning = 'bucket(6)')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN rating DECIMAL(10,3) WITH (partitioning = 'truncate(10)')", BASE_TABLE));

        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (8001, '/premium', TIMESTAMP '2023-10-01 09:30:00', 'sess_012', " +
                        "   DECIMAL '1234.56', DECIMAL '98.75', DECIMAL '456.789')", BASE_TABLE));
    }

    @Test
    public void testColumnRenaming()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN zipcode VARCHAR", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s RENAME COLUMN zipcode TO location", BASE_TABLE));
        assertQueryFails(format("SELECT zipcode FROM %s", BASE_TABLE), ".*Column 'zipcode' cannot be resolved.*");
        assertQuerySucceeds(format("SELECT location FROM %s", BASE_TABLE));

        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (9001, '/test', TIMESTAMP '2023-11-01 10:00:00', 'sess_test', 'NYC')", BASE_TABLE));
        assertQuery(format("SELECT COUNT(*) FROM %s WHERE location = 'NYC'", BASE_TABLE), "VALUES (BIGINT '1')");
    }

    @Test
    public void testRenamePartitionedColumn()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN zipcode VARCHAR WITH (partitioning = 'identity')", BASE_TABLE));
        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (9002, '/partition-test', TIMESTAMP '2023-11-02 11:00:00', 'sess_part', '10001')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s RENAME COLUMN zipcode TO postal_code", BASE_TABLE));

        assertQuery(format("SELECT COUNT(*) FROM %s WHERE postal_code = '10001'", BASE_TABLE), "VALUES (BIGINT '1')");

        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (9003, '/partition-test2', TIMESTAMP '2023-11-03 12:00:00', 'sess_part2', '10002')", BASE_TABLE));

        assertQuery(format("SELECT COUNT(*) FROM %s WHERE postal_code IN ('10001', '10002')", BASE_TABLE), "VALUES (BIGINT '2')");
    }

    @Test
    public void testRenameColumnErrorCases()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN zipcode VARCHAR", BASE_TABLE));
        assertQueryFails(
                format("ALTER TABLE %s RENAME COLUMN zipcode TO user_id", BASE_TABLE),
                ".*Column 'user_id' already exists.*");

        assertQueryFails(
                format("ALTER TABLE %s RENAME COLUMN nonexistent TO location", BASE_TABLE),
                ".*Column 'nonexistent' does not exist.*");
        assertQueryFails(
                format("ALTER TABLE %s RENAME COLUMN zipcode TO ZIPCODE", BASE_TABLE),
                ".*Column.*already exists.*");
    }

    @Test
    public void testMultipleColumnRenames()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN zipcode VARCHAR", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN region_id INTEGER", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN score DECIMAL(5,2)", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s RENAME COLUMN zipcode TO postal_code", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s RENAME COLUMN region_id TO area_code", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s RENAME COLUMN score TO rating", BASE_TABLE));

        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (9007, '/multi-rename', TIMESTAMP '2023-11-07 16:00:00', 'sess_multi', '12345', 100, DECIMAL '4.5')", BASE_TABLE));

        assertQuery(format("SELECT COUNT(*) FROM %s WHERE postal_code = '12345'", BASE_TABLE), "VALUES (BIGINT '1')");
        assertQuery(format("SELECT COUNT(*) FROM %s WHERE area_code = 100", BASE_TABLE), "VALUES (BIGINT '1')");
    }

    @Test
    public void testRenameWithDataIntegrity()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN zipcode VARCHAR", BASE_TABLE));
        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (9008, '/integrity-test', TIMESTAMP '2023-11-08 17:00:00', 'sess_integrity', '54321')", BASE_TABLE));

        assertQuery(format("SELECT COUNT(*) FROM %s WHERE zipcode = '54321'", BASE_TABLE), "VALUES (BIGINT '1')");
        assertQuerySucceeds(format("ALTER TABLE %s RENAME COLUMN zipcode TO location_code", BASE_TABLE));
        assertQuery(format("SELECT COUNT(*) FROM %s WHERE location_code = '54321'", BASE_TABLE), "VALUES (BIGINT '1')");
        assertQuery(format("SELECT COUNT(*) FROM %s", BASE_TABLE), "VALUES (BIGINT '4')");
    }

    @Test
    public void testRenameTruncatePartitionedColumn()
    {
        assertQuerySucceeds(format("ALTER TABLE %s ADD COLUMN category VARCHAR WITH (partitioning = 'truncate(3)')", BASE_TABLE));
        assertQuerySucceeds(format(
                "INSERT INTO %s VALUES " +
                        "  (9005, '/truncate-test', TIMESTAMP '2023-11-05 14:00:00', 'sess_trunc', 'electronics')", BASE_TABLE));
        assertQuerySucceeds(format("ALTER TABLE %s RENAME COLUMN category TO product_type", BASE_TABLE));
        assertQuerySucceeds(format("select product_type from %s", BASE_TABLE));
        assertQuery(format("SELECT count(*) FROM %s WHERE product_type = 'electronics'", BASE_TABLE), "VALUES (BIGINT '1')");
    }
}
