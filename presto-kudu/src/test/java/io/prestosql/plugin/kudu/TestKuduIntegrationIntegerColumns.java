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
import static org.testng.Assert.fail;

public class TestKuduIntegrationIntegerColumns
        extends AbstractTestQueryFramework
{
    private QueryRunner queryRunner;

    static class TestInt
    {
        final String type;
        final int bits;

        TestInt(String type, int bits)
        {
            this.type = type;
            this.bits = bits;
        }
    }

    static final TestInt[] testList = {
            new TestInt("TINYINT", 8),
            new TestInt("SMALLINT", 16),
            new TestInt("INTEGER", 32),
            new TestInt("BIGINT", 64),
    };

    public TestKuduIntegrationIntegerColumns()
    {
        super(() -> KuduQueryRunnerFactory.createKuduQueryRunner("test_integer"));
    }

    @Test
    public void testCreateTableWithIntegerColumn()
    {
        for (TestInt test : testList) {
            doTestCreateTableWithIntegerColumn(test);
        }
    }

    public void doTestCreateTableWithIntegerColumn(TestInt test)
    {
        String dropTable = "DROP TABLE IF EXISTS test_int";
        String createTable = "CREATE TABLE test_int (\n";
        createTable += "  id INT WITH (primary_key=true),\n";
        createTable += "  intcol " + test.type + "\n";
        createTable += ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id'],\n" +
                " partition_by_hash_buckets = 2\n" +
                ")";

        queryRunner.execute(dropTable);
        queryRunner.execute(createTable);

        long maxValue = Long.MAX_VALUE;
        long casted = maxValue >> (64 - test.bits);
        queryRunner.execute("INSERT INTO test_int VALUES(1, CAST(" + casted + " AS " + test.type + "))");

        MaterializedResult result = queryRunner.execute("SELECT id, intcol FROM test_int");
        assertEquals(result.getRowCount(), 1);
        Object obj = result.getMaterializedRows().get(0).getField(1);
        switch (test.bits) {
            case 64:
                assertTrue(obj instanceof Long);
                assertEquals(((Long) obj).longValue(), casted);
                break;
            case 32:
                assertTrue(obj instanceof Integer);
                assertEquals(((Integer) obj).longValue(), casted);
                break;
            case 16:
                assertTrue(obj instanceof Short);
                assertEquals(((Short) obj).longValue(), casted);
                break;
            case 8:
                assertTrue(obj instanceof Byte);
                assertEquals(((Byte) obj).longValue(), casted);
                break;
            default:
                fail("Unexpected bits: " + test.bits);
                break;
        }
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
}
