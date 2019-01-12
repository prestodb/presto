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

public class TestKuduIntegrationHashPartitioning
        extends AbstractTestQueryFramework
{
    private QueryRunner queryRunner;

    public TestKuduIntegrationHashPartitioning()
    {
        super(() -> KuduQueryRunnerFactory.createKuduQueryRunner("hash"));
    }

    @Test
    public void testCreateTableSingleHashPartitionLevel()
    {
        String dropTable = "DROP TABLE IF EXISTS hashtest1";
        String createTable = "CREATE TABLE hashtest1 (\n";
        createTable += "  id INT WITH (primary_key=true,encoding='auto',compression='default'),\n";
        createTable += "  event_time TIMESTAMP WITH (primary_key=true, encoding='plain', compression='lz4'),\n";
        createTable += "  value DOUBLE WITH (primary_key=false,nullable=false,compression='no')\n";
        createTable += ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id','event_time'],\n" +
                " partition_by_hash_buckets = 3\n" +
                ")";

        doTestCreateTable("hashtest1", createTable);
    }

    @Test
    public void testCreateTableDoubleHashPartitionLevel()
    {
        String createTable = "CREATE TABLE hashtest2 (\n";
        createTable += "  id INT WITH (primary_key=true, encoding='bitshuffle',compression='zlib'),\n";
        createTable += "  event_time TIMESTAMP WITH (primary_key=true, encoding='runlength', compression='snappy'),\n";
        createTable += "  value DOUBLE WITH (nullable=true)\n";
        createTable += ") WITH (\n" +
                " partition_by_hash_columns = ARRAY['id'],\n" +
                " partition_by_hash_buckets = 3\n," +
                " partition_by_second_hash_columns = ARRAY['event_time'],\n" +
                " partition_by_second_hash_buckets = 3\n" +
                ")";

        doTestCreateTable("hashtest2", createTable);
    }

    private void doTestCreateTable(String tableName, String createTable)
    {
        String dropTable = "DROP TABLE IF EXISTS " + tableName;

        queryRunner.execute(dropTable);
        queryRunner.execute(createTable);

        String insert = "INSERT INTO " + tableName + " VALUES (1, TIMESTAMP '2001-08-22 03:04:05.321', 2.5)";

        queryRunner.execute(insert);

        MaterializedResult result = queryRunner.execute("SELECT id FROM " + tableName);
        assertEquals(result.getRowCount(), 1);
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
