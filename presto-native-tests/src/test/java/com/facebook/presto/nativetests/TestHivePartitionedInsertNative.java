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
package com.facebook.presto.nativetests;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestodb.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static java.lang.Boolean.parseBoolean;

public class TestHivePartitionedInsertNative
        extends AbstractTestQueryFramework
{
    private String storageFormat;
    private boolean sidecarEnabled;
    QueryRunner queryRunner;

    @BeforeClass
    @Override
    public void init() throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        queryRunner = NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);
        return queryRunner;
    }

    @Override
    protected void createTables()
    {
        NativeTestsUtils.createTables(storageFormat);
    }


@Test
    public void testInsertIntoBucketedTables()
    {
        String tableName = "hive.tpch.bucketed_nation";

//        // Clean up previous run
        queryRunner.execute("DROP TABLE IF EXISTS " + tableName);
//
//        // Create the bucketed table
        @Language("SQL") String createTableSql = "CREATE TABLE " + tableName + " (\n" +
                "    n_nationkey BIGINT,\n" +
                "    n_name VARCHAR,\n" +
                "    n_regionkey BIGINT,\n" +
                "    n_comment VARCHAR\n" +
                ")\n" +
                "WITH (\n" +
                "    format = 'PARQUET',\n" +
                "    bucketed_by = ARRAY['n_regionkey'],\n" +
                "    bucket_count = 2\n" +
                ")";
        queryRunner.execute(createTableSql);


        // Insert data twice
        queryRunner.execute("INSERT INTO "+ tableName + " VALUES(0, 'ALGERIA', 0, 'haggle. carefully final deposits detect slyly again.'),\n" +
                "(1, 'ARGENTINA', 1, 'al foxes promise slyly according to the regular accounts.')");
        System.out.println(computeActual("SELECT count(*) FROM " + tableName));

        // Validate total row count
//        queryRunner.execute(queryRunner.getDefaultSession(), "SELECT count(*) FROM " + tableName, "VALUES 50");
//
//        // Validate filtered row count
//        queryRunner.execute(queryRunner.getDefaultSession(), "SELECT count(*) FROM " + tableName + " WHERE n_regionkey = 0", "VALUES 10");
    }
}
