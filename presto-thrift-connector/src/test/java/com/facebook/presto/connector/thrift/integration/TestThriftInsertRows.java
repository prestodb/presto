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
package com.facebook.presto.connector.thrift.integration;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.tests.QueryAssertions.assertContains;

public class TestThriftInsertRows
{
    private QueryRunner queryRunner;

    public TestThriftInsertRows()
            throws Exception
    {
        queryRunner = ThriftQueryRunner.createThriftQueryRunner(1, 3, ThriftQueryRunner.EnableExtraPrestoFeature.INSERT_ROWS, ImmutableMap.of());
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult result = queryRunner.execute("SHOW SCHEMAS").toTestTypes();
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR)
                .row("matiash");
        assertContains(result, resultBuilder.build());
    }

    @Test
    public void testListTables()
    {
        MaterializedResult result = queryRunner.execute("SHOW TABLES FROM matiash").toTestTypes();
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR)
                .row("test_table");
        assertContains(result, resultBuilder.build());
    }

    @Test
    public void testGetTableMetadata()
    {
        MaterializedResult result = queryRunner.execute("SHOW COLUMNS FROM matiash.test_table").toTestTypes();
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR, DOUBLE, INTEGER)
                .row("value", "double", "", "")
                .row("x", "integer", "", "")
                .row("y", "integer", "", "")
                .row("zoom_level", "integer", "", "");
        assertContains(result, resultBuilder.build());
    }

    @Test
    public void testGetRows()
    {
        MaterializedResult result = queryRunner.execute("SELECT * FROM matiash.test_table").toTestTypes();
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), VARCHAR);
        assertContains(result, resultBuilder.build());
    }

    @Test
    public void testAddRows()
    {
        queryRunner.execute("INSERT INTO matiash.test_table VALUES ('0', 1, 0, 0, 1), ('00', 12.345, 42, 50, 2)").toTestTypes();
        MaterializedResult result = queryRunner.execute("SELECT * FROM matiash.test_table").toTestTypes();
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(queryRunner.getDefaultSession(), DOUBLE, INTEGER)
                .row("0", 1.0, 0, 0, 1)
                .row("00", 12.345, 42, 50, 2);
        assertContains(result, resultBuilder.build());
    }
}
