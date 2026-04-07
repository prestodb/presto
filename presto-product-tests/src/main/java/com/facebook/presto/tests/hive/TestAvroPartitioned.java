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
package com.facebook.presto.tests.hive;

import io.prestodb.tempto.AfterTestWithContext;
import io.prestodb.tempto.BeforeTestWithContext;
import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.AVRO;
import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestodb.tempto.query.QueryExecutor.query;
import static java.lang.String.format;

public class TestAvroPartitioned
        extends ProductTest
{
    private static final String PARTITIONED_TABLE_NAME = "product_tests_avro_partitioned";
    private static final String ORIGINAL_SCHEMA = "file:///docker/volumes/presto-product-tests/avro/original_schema.avsc";

    private static final String CREATE_TABLE_PARTITIONED = format("" +
                    "CREATE TABLE %s (dummy_col VARCHAR, p_col VARCHAR) " +
                    "WITH (" +
                    "format='AVRO', " +
                    "partitioned_by=ARRAY['p_col'], " +
                    "avro_schema_url='%s'" +
                    ")",
            PARTITIONED_TABLE_NAME,
            ORIGINAL_SCHEMA);

    @BeforeTestWithContext
    public void createAndLoadTable()
    {
        query(CREATE_TABLE_PARTITIONED);
        query(format("INSERT INTO %s (string_col, int_col, p_col) VALUES ('string0', 0, 'p0')", PARTITIONED_TABLE_NAME));
    }

    @AfterTestWithContext
    public void dropTestTable()
    {
        query(format("DROP TABLE IF EXISTS %s", PARTITIONED_TABLE_NAME));
    }

    @Test(groups = {AVRO})
    public void testSelectFromPartitionedAvroTable()
    {
        assertThat(query(format("SELECT string_col, p_col FROM %s WHERE p_col = 'p0'", PARTITIONED_TABLE_NAME)))
                .containsExactly(row("string0", "p0"));
    }

    @Test(groups = {AVRO})
    public void testShowColumnsOnPartitionedAvroTable()
    {
        // We query information_schema to control exactly which columns are returned
        String sql = String.format(
                "SELECT column_name, data_type, extra_info, comment " +
                        "FROM information_schema.columns " +
                        "WHERE table_name = '%s' " +
                        "ORDER BY ordinal_position",
                PARTITIONED_TABLE_NAME);

        assertThat(query(sql))
                .containsExactly(
                        row("string_col", "varchar", null, null),
                        row("int_col", "integer", null, null),
                        row("p_col", "varchar", "partition key", null));
    }

    @Test(groups = {AVRO})
    public void testInsertAdditionalPartition()
    {
        query(format("INSERT INTO %s (string_col, int_col, p_col) VALUES ('string1', 1, 'p1')", PARTITIONED_TABLE_NAME));

        assertThat(query(format("SELECT count(DISTINCT p_col) FROM %s", PARTITIONED_TABLE_NAME)))
                .containsExactly(row(2));
    }
}
