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

import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestKuduIntegrationSchemaNotExisting
        extends AbstractTestQueryFramework
{
    private static String oldPrefix;
    private QueryRunner queryRunner;

    private static final String SCHEMA_NAME = "test_presto_schema";

    private static final String CREATE_SCHEMA = "create schema kudu." + SCHEMA_NAME;

    private static final String DROP_SCHEMA = "drop schema if exists kudu." + SCHEMA_NAME;

    private static final String CREATE_TABLE = "create table if not exists kudu." + SCHEMA_NAME + ".test_presto_table (\n" +
            "id INT WITH (primary_key=true),\n" +
            "user_name VARCHAR\n" +
            ") WITH (\n" +
            " partition_by_hash_columns = ARRAY['id'],\n" +
            " partition_by_hash_buckets = 2\n" +
            ")";

    private static final String DROP_TABLE = "drop table if exists kudu." + SCHEMA_NAME + ".test_presto_table";

    public TestKuduIntegrationSchemaNotExisting()
    {
        super(() -> createKuduQueryRunner());
    }

    private static QueryRunner createKuduQueryRunner()
            throws Exception
    {
        oldPrefix = System.getProperty("kudu.schema-emulation.prefix");
        System.setProperty("kudu.schema-emulation.prefix", "");
        try {
            return KuduQueryRunnerFactory.createKuduQueryRunner("test_dummy");
        }
        catch (Throwable t) {
            System.setProperty("kudu.schema-emulation.prefix", oldPrefix);
            throw t;
        }
    }

    @Test
    public void testCreateTableWithoutSchema()
    {
        try {
            queryRunner.execute(CREATE_TABLE);
            fail();
        }
        catch (Exception e) {
            assertEquals("Schema " + SCHEMA_NAME + " not found", e.getMessage());
        }

        queryRunner.execute(CREATE_SCHEMA);
        queryRunner.execute(CREATE_TABLE);
    }

    @BeforeClass
    public void setUp()
    {
        queryRunner = getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        System.setProperty("kudu.schema-emulation.prefix", oldPrefix);
        if (queryRunner != null) {
            queryRunner.execute(DROP_TABLE);
            queryRunner.execute(DROP_SCHEMA);
            queryRunner.close();
            queryRunner = null;
        }
    }
}
