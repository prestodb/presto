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

import com.facebook.presto.testing.QueryRunner;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestNativeIcebergGeneralQueries
        extends IcebergPartitionTestBase
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = ((QueryRunner) getExpectedQueryRunner());
        createTestTables(queryRunner);
    }

    private void createTestTables(QueryRunner queryRunner)
    {
        queryRunner.execute("DROP TABLE IF EXISTS test_hidden_columns");
        queryRunner.execute("CREATE TABLE test_hidden_columns AS SELECT * FROM tpch.tiny.region WHERE regionkey=0");
        queryRunner.execute("INSERT INTO test_hidden_columns SELECT * FROM tpch.tiny.region WHERE regionkey=1");

        queryRunner.execute("DROP TABLE IF EXISTS ice_table_partitioned");
        queryRunner.execute("CREATE TABLE ice_table_partitioned(c1 INT, ds DATE) WITH (partitioning = ARRAY['ds'])");
        queryRunner.execute("INSERT INTO ice_table_partitioned VALUES(1, date'2022-04-09'), (2, date'2022-03-18'), (3, date'1993-01-01')");

        queryRunner.execute("DROP TABLE IF EXISTS ice_table");
        queryRunner.execute("CREATE TABLE ice_table(c1 INT, ds DATE)");
        queryRunner.execute("INSERT INTO ice_table VALUES(1, date'2022-04-09'), (2, date'2022-03-18'), (3, date'1993-01-01')");
    }

    @Test
    public void testPathHiddenColumn()
    {
        assertQuery("SELECT * FROM test_hidden_columns");

//        assertEquals(
//                (Long) computeActual(format("SELECT count(*) from test_hidden_columns"))
//                        .getOnlyValue(),
//                0L);
    }

    @Test(enabled = false)
    public void testDataSequenceNumberHiddenColumn()
    {
        assertQuery("SELECT \"$data_sequence_number\", * FROM test_hidden_columns");

        // Fetch one of the data sequence numbers and use it in a filter
        Long dataSequenceNumber = (Long) computeActual("SELECT \"$data_sequence_number\" from test_hidden_columns LIMIT 1").getOnlyValue();
        assertQuery(format("SELECT * from test_hidden_columns WHERE \"$data_sequence_number\"=%d", dataSequenceNumber));

        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$data_sequence_number\"=%d", dataSequenceNumber))
                        .getOnlyValue(),
                1L);

        // Filter for $data_sequence_number that doesn't exist.
        assertEquals(
                (Long) computeActual(format("SELECT count(*) from test_hidden_columns WHERE \"$data_sequence_number\"=%d", 1000))
                        .getOnlyValue(),
                0L);
    }

    @Test
    public void testDateQueries()
    {
        assertQuery("SELECT * FROM ice_table_partitioned WHERE ds >= date'1994-01-01'", "VALUES (1, date'2022-04-09'), (2, date'2022-03-18')");
        assertQuery("SELECT * FROM ice_table WHERE ds = date'2022-04-09'", "VALUES (1, date'2022-04-09')");
    }
}
