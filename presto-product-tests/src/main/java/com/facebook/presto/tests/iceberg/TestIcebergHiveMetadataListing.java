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
package com.facebook.presto.tests.iceberg;

import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.ICEBERG;
import static com.facebook.presto.tests.TestGroups.STORAGE_FORMATS;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;

public class TestIcebergHiveMetadataListing
        extends ProductTest
{
    @BeforeTestWithContext
    public void setUp()
    {
        onPresto().executeQuery("CREATE TABLE iceberg.default.iceberg_table1 (_string VARCHAR, _integer INTEGER)");
        onPresto().executeQuery("CREATE TABLE hive.default.hive_table (_double DOUBLE)");
        onPresto().executeQuery("CREATE VIEW hive.default.hive_view AS SELECT * FROM hive.default.hive_table");
    }

    @AfterTestWithContext
    public void cleanUp()
    {
        onPresto().executeQuery("DROP TABLE IF EXISTS hive.default.hive_table");
        onPresto().executeQuery("DROP VIEW IF EXISTS hive.default.hive_view");
        onPresto().executeQuery("DROP TABLE IF EXISTS iceberg.default.iceberg_table1");
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    public void testTableListing()
    {
        assertThat(onPresto().executeQuery("SHOW TABLES FROM iceberg.default"))
                .containsOnly(
                        row("iceberg_table1"),
                        row("hive_table"),
                        row("hive_view"));
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    public void testColumnListing()
    {
        assertThat(onPresto().executeQuery(
                "SELECT table_name, column_name FROM iceberg.information_schema.columns " +
                        "WHERE table_catalog = 'iceberg' AND table_schema = 'default'"))
                .containsOnly(
                        row("iceberg_table1", "_string"),
                        row("iceberg_table1", "_integer"));
    }
}
