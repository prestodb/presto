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
package com.facebook.presto.kudu;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKuduIntegrationRangePartitioning
        extends AbstractTestQueryFramework
{
    private QueryRunner queryRunner;

    static final TestRanges[] testRangesList = {
            new TestRanges("varchar",
                    "{\"lower\": null, \"upper\": \"D\"}",
                    "{\"lower\": \"D\", \"upper\": \"M\"}",
                    "{\"lower\": \"M\", \"upper\": \"S\"}",
                    "{\"lower\": \"S\", \"upper\": null}"),
            new TestRanges("timestamp",
                    "{\"lower\": null, \"upper\": \"2017-01-01T02:03:04.567Z\"}",
                    "{\"lower\": \"2017-01-01 03:03:04.567+01:00\", \"upper\": \"2017-02-01 12:34\"}",
                    "{\"lower\": \"2017-02-01 12:34\", \"upper\": \"2017-03-01\"}",
                    "{\"lower\": \"2017-03-01\", \"upper\": null}",
                    "{\"lower\": null, \"upper\": \"2017-01-01T02:03:04.567Z\"}",
                    "{\"lower\": \"2017-01-01T02:03:04.567Z\", \"upper\": \"2017-02-01T12:34:00.000Z\"}",
                    "{\"lower\": \"2017-02-01T12:34:00.000Z\", \"upper\": \"2017-03-01T00:00:00.000Z\"}",
                    "{\"lower\": \"2017-03-01T00:00:00.000Z\", \"upper\": null}"),
            new TestRanges("tinyint",
                    "{\"lower\": null, \"upper\": -10}",
                    "{\"lower\": \"-10\", \"upper\": 0}",
                    "{\"lower\": 0, \"upper\": 10}",
                    "{\"lower\": 10, \"upper\": 20}",
                    "{\"lower\": null, \"upper\": -10}",
                    "{\"lower\": -10, \"upper\": 0}",
                    "{\"lower\": 0, \"upper\": 10}",
                    "{\"lower\": 10, \"upper\": 20}"),
            new TestRanges("smallint",
                    "{\"lower\": null, \"upper\": -1000}",
                    "{\"lower\": -1000, \"upper\": 0}",
                    "{\"lower\": 0, \"upper\": 1000}",
                    "{\"lower\": 1000, \"upper\": 2000}"),
            new TestRanges("integer",
                    "{\"lower\": null, \"upper\": -1000000}",
                    "{\"lower\": -1000000, \"upper\": 0}",
                    "{\"lower\": 0, \"upper\": 10000}",
                    "{\"lower\": 10000, \"upper\": 1000000}"),
            new TestRanges("bigint",
                    "{\"lower\": null, \"upper\": \"-123456789012345\"}",
                    "{\"lower\": \"-123456789012345\", \"upper\": 0}",
                    "{\"lower\": 0, \"upper\": 123400}",
                    "{\"lower\": 123400, \"upper\": 123456789012345}",
                    "{\"lower\": null, \"upper\": -123456789012345}",
                    "{\"lower\": -123456789012345, \"upper\": 0}",
                    "{\"lower\": 0, \"upper\": 123400}",
                    "{\"lower\": 123400, \"upper\": 123456789012345}"),
            new TestRanges("varbinary",
                    "{\"lower\": null, \"upper\": \"YWI=\"}",
                    "{\"lower\": \"YWI=\", \"upper\": \"ZA==\"}",
                    "{\"lower\": \"ZA==\", \"upper\": \"bW1t\"}",
                    "{\"lower\": \"bW1t\", \"upper\": \"eg==\"}"),
            new TestRanges(new String[] {"smallint", "varchar"},
                    "{\"lower\": null, \"upper\": [1, \"M\"]}",
                    "{\"lower\": [1, \"M\"], \"upper\": [1, \"T\"]}",
                    "{\"lower\": [1, \"T\"], \"upper\": [2, \"Z\"]}",
                    "{\"lower\": [2, \"Z\"], \"upper\": null}"),
    };

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return KuduQueryRunnerFactory.createKuduQueryRunner("range_partitioning");
    }

    @Test
    public void testCreateAndChangeTableWithRangePartition()
    {
        for (TestRanges ranges : testRangesList) {
            doTestCreateAndChangeTableWithRangePartition(ranges);
        }
    }

    public void doTestCreateAndChangeTableWithRangePartition(TestRanges ranges)
    {
        String[] types = ranges.types;
        String name = String.join("_", ranges.types);
        String tableName = "range_partitioning_" + name;
        String createTable = "CREATE TABLE " + tableName + " (\n";
        String rangePartitionColumns = "ARRAY[";
        for (int i = 0; i < types.length; i++) {
            String type = types[i];
            String columnName = "key" + i;
            createTable += "  " + columnName + " " + type + " WITH (primary_key=true),\n";
            if (i > 0) {
                rangePartitionColumns += ",";
            }
            rangePartitionColumns += "'" + columnName + "'";
        }
        rangePartitionColumns += "]";

        createTable +=
                "  value varchar\n" +
                        ") WITH (\n" +
                        " partition_by_range_columns = " + rangePartitionColumns + ",\n" +
                        " range_partitions = '[" + ranges.range1 + "," + ranges.range2 + "]'\n" +
                        ")";
        queryRunner.execute(createTable);

        String schema = queryRunner.getDefaultSession().getSchema().get();

        String addPartition3 = "CALL kudu.system.add_range_partition('" + schema + "','" + tableName + "','" + ranges.range3 + "')";
        queryRunner.execute(addPartition3);
        String addPartition4 = "CALL kudu.system.add_range_partition('" + schema + "','" + tableName + "','" + ranges.range4 + "')";
        queryRunner.execute(addPartition4);

        String dropPartition3 = addPartition3.replace(".add_range_partition(", ".drop_range_partition(");
        queryRunner.execute(dropPartition3);

        MaterializedResult result = queryRunner.execute("SHOW CREATE TABLE " + tableName);
        assertEquals(result.getRowCount(), 1);
        String createSQL = result.getMaterializedRows().get(0).getField(0).toString();
        String rangesArray = "'[" + ranges.cmp1 + "," + ranges.cmp2 + "," + ranges.cmp4 + "]'";
        rangesArray = rangesArray.replaceAll("\\s+", "");
        String expectedRanges = "range_partitions = " + rangesArray;
        assertTrue(createSQL.contains(expectedRanges), createSQL + "\ncontains\n" + expectedRanges);
    }

    @BeforeClass
    public void setUp()
    {
        queryRunner = getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    static class TestRanges
    {
        final String[] types;
        final String range1;
        final String range2;
        final String range3;
        final String range4;
        final String cmp1;
        final String cmp2;
        final String cmp3;
        final String cmp4;

        TestRanges(String type, String range1, String range2, String range3, String range4)
        {
            this(new String[] {type}, range1, range2, range3, range4, range1, range2, range3, range4);
        }

        TestRanges(String type, String range1, String range2, String range3, String range4,
                String cmp1, String cmp2, String cmp3, String cmp4)
        {
            this(new String[] {type}, range1, range2, range3, range4, cmp1, cmp2, cmp3, cmp4);
        }

        TestRanges(String[] types, String range1, String range2, String range3, String range4)
        {
            this(types, range1, range2, range3, range4, range1, range2, range3, range4);
        }

        TestRanges(String[] types, String range1, String range2, String range3, String range4,
                String cmp1, String cmp2, String cmp3, String cmp4)
        {
            this.types = types;
            this.range1 = range1;
            this.range2 = range2;
            this.range3 = range3;
            this.range4 = range4;
            this.cmp1 = cmp1;
            this.cmp2 = cmp2;
            this.cmp3 = cmp3;
            this.cmp4 = cmp4;
        }
    }
}
