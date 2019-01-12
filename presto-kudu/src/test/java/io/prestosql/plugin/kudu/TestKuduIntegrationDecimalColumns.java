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
package io.prestosql.plugin.kudu;

import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestKuduIntegrationDecimalColumns
        extends AbstractTestQueryFramework
{
    private QueryRunner queryRunner;

    static final TestDec[] testDecList = {
            new TestDec(10, 0),
            new TestDec(15, 4),
            new TestDec(18, 6),
            new TestDec(18, 7),
            new TestDec(19, 8),
            new TestDec(24, 14),
            new TestDec(38, 20),
            new TestDec(38, 28),
    };

    public TestKuduIntegrationDecimalColumns()
    {
        super(() -> KuduQueryRunnerFactory.createKuduQueryRunner("decimal"));
    }

    @Test
    public void testCreateTableWithDecimalColumn()
    {
        for (TestDec dec : testDecList) {
            doTestCreateTableWithDecimalColumn(dec);
        }
    }

    private void doTestCreateTableWithDecimalColumn(TestDec dec)
    {
        String tableName = dec.getTableName();
        String dropTable = "DROP TABLE IF EXISTS " + tableName;
        String createTable = "CREATE TABLE " + tableName + " (\n";
        createTable += "  id INT WITH (primary_key=true),\n";
        createTable += "  dec DECIMAL(" + dec.precision + "," + dec.scale + ")\n";
        createTable += ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id'],\n" +
                " partition_by_hash_buckets = 2\n" +
                ")";

        queryRunner.execute(dropTable);
        queryRunner.execute(createTable);

        String fullPrecisionValue = "1234567890.1234567890123456789012345678";
        int maxScale = dec.precision - 10;
        int valuePrecision = dec.precision - maxScale + Math.min(maxScale, dec.scale);
        String insertValue = fullPrecisionValue.substring(0, valuePrecision + 1);
        queryRunner.execute("INSERT INTO " + tableName + " VALUES(1, DECIMAL '" + insertValue + "')");

        MaterializedResult result = queryRunner.execute("SELECT id, CAST((dec - (DECIMAL '" + insertValue + "')) as DOUBLE) FROM " + tableName);
        assertEquals(result.getRowCount(), 1);
        Object obj = result.getMaterializedRows().get(0).getField(1);
        assertTrue(obj instanceof Double);
        Double actual = (Double) obj;
        assertEquals(0, actual, 0.3 * Math.pow(0.1, dec.scale), "p=" + dec.precision + ",s=" + dec.scale + " => " + actual + ",insert = " + insertValue);
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

    static class TestDec
    {
        final int precision;
        final int scale;

        TestDec(int precision, int scale)
        {
            this.precision = precision;
            this.scale = scale;
        }

        String getTableName()
        {
            return "test_dec_" + precision + "_" + scale;
        }
    }
}
