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
package com.facebook.presto.hive;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static io.airlift.tpch.TpchTable.ORDERS;

public class TestHiveIntegrationInfoSchema
        extends AbstractTestQueryFramework
{
    // Note: Presto will have lower case in 'information_schema',
    //       while H2 will have upper case.

    public TestHiveIntegrationInfoSchema()
            throws Exception
    {
        super(createQueryRunner(ORDERS));
    }

    @Test
    public void testTablesWithoutEqualityConstraint()
            throws Exception
    {
        @Language("SQL")
        String actual = "" +
                "SELECT LOWER(table_name) FROM " +
                "  information_schema.tables " +
                "WHERE " +
                "  table_catalog = 'hive' " +
                "  and table_schema LIKE 'tpch' " +
                "  and table_name LIKE '%orders'";

        @Language("SQL")
        String expected = "" +
                "SELECT LOWER(table_name) FROM " +
                "  information_schema.tables " +
                "WHERE " +
                "  table_name LIKE '%ORDERS'";

        assertQuery(actual, expected);
    }

    @Test
    public void testColumnsWithoutEqualityConstraint()
            throws Exception
    {
        @Language("SQL")
        String actual = "" +
                "SELECT LOWER(table_name), LOWER(column_name) FROM " +
                "  information_schema.columns " +
                "WHERE " +
                "  table_catalog = 'hive' " +
                "  and table_schema = 'tpch' " +
                "  and table_name LIKE '%orders%'";

        @Language("SQL")
        String expected = "" +
                "SELECT LOWER(table_name), LOWER(column_name) FROM " +
                "  information_schema.columns " +
                "WHERE " +
                "  table_name LIKE '%ORDERS%'";

        assertQuery(actual, expected);
    }
}
