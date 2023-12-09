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
package com.facebook.presto.mongodb;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.mongodb.MongoQueryRunner.createMongoQueryRunner;

@Test
public class TestMongoDistributedQueries
        extends AbstractTestQueries
{
    private MongoQueryRunner mongoQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createMongoQueryRunner(TpchTable.getTables());
    }

    @BeforeClass
    public void setUp()
    {
        mongoQueryRunner = (MongoQueryRunner) getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        if (mongoQueryRunner != null) {
            mongoQueryRunner.shutdown();
        }
    }

    @Test
    public void testQuotedIdentifiers()
    {
        // Expected to fail as Table is stored in Uppercase in H2 db and exists in tpch as lowercase
        assertQueryFails("SELECT \"TOTALPRICE\" \"my price\" FROM \"ORDERS\"", "Table has no columns:.*");
    }

    @Test
    public void testInformationSchemaUppercaseName()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'LOCAL'",
                "SELECT '' WHERE false");
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'TINY'",
                "SELECT '' WHERE false");
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ORDERS'",
                "SELECT 'ORDERS' table_name");
    }
}
