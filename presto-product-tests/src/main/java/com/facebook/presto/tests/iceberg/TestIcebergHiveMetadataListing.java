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

import com.google.common.collect.ImmutableList;
import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import io.prestodb.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.tests.TestGroups.ICEBERG;
import static com.facebook.presto.tests.TestGroups.STORAGE_FORMATS;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;

public class TestIcebergHiveMetadataListing
        extends ProductTest
{
    private List<QueryAssert.Row> preexistingTables;
    private List<QueryAssert.Row> preexistingColumns;
    @BeforeTestWithContext
    public void setUp()
    {
        cleanUp();
        preexistingTables = onPresto().executeQuery("SHOW TABLES FROM iceberg.default").rows().stream()
                .map(list -> row(list.toArray()))
                .collect(Collectors.toList());
        preexistingColumns = onPresto().executeQuery("SELECT table_name, column_name FROM iceberg.information_schema.columns " +
                        "WHERE table_catalog = 'iceberg' AND table_schema = 'default'").rows().stream()
                .map(list -> row(list.toArray()))
                .collect(Collectors.toList());
        onPresto().executeQuery("CREATE TABLE iceberg.default.iceberg_table1 (_string VARCHAR, _integer INTEGER)");
        onPresto().executeQuery("CREATE TABLE hive.default.hive_table (_double DOUBLE)");
        onPresto().executeQuery("CREATE VIEW hive.default.hive_view AS SELECT * FROM hive.default.hive_table");
        onPresto().executeQuery("CREATE VIEW iceberg.default.iceberg_view AS SELECT * FROM iceberg.default.iceberg_table1");
    }

    @AfterTestWithContext
    public void cleanUp()
    {
        onPresto().executeQuery("DROP TABLE IF EXISTS hive.default.hive_table");
        onPresto().executeQuery("DROP VIEW IF EXISTS hive.default.hive_view");
        onPresto().executeQuery("DROP VIEW IF EXISTS iceberg.default.iceberg_view");
        onPresto().executeQuery("DROP TABLE IF EXISTS iceberg.default.iceberg_table1");
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    public void testTableListing()
    {
        assertThat(onPresto().executeQuery("SHOW TABLES FROM iceberg.default"))
                .containsOnly(ImmutableList.<QueryAssert.Row>builder()
                        .addAll(preexistingTables)
                        .add(row("iceberg_table1"))
                        .add(row("iceberg_view"))
                        .add(row("hive_view"))
                        .add(row("hive_table"))
                        .build());
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    public void testColumnListing()
    {
        assertThat(onPresto().executeQuery(
                "SELECT table_name, column_name FROM iceberg.information_schema.columns " +
                        "WHERE table_catalog = 'iceberg' AND table_schema = 'default'"))
                .containsOnly(ImmutableList.<QueryAssert.Row>builder()
                        .addAll(preexistingColumns)
                        .add(row("iceberg_table1", "_string"))
                        .add(row("iceberg_table1", "_integer"))
                        .add(row("iceberg_view", "_string"))
                        .add(row("iceberg_view", "_integer"))
                        .add(row("hive_view", "_double"))
                        .build());
    }
}
