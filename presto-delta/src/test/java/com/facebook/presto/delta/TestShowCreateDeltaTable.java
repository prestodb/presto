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
package com.facebook.presto.delta;

import org.testng.annotations.Test;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestShowCreateDeltaTable
        extends AbstractDeltaDistributedQueryTestBase
{
    @Test
    public void testShowCreateTableWithExternalLocation()
    {
        String tableName = "delta_v3/data-reader-primitives";
        String fullTableName = format("%s.%s.\"%s\"", DELTA_CATALOG, DELTA_SCHEMA.toLowerCase(), tableName);

        String createTableQueryTemplate = "CREATE TABLE %s (\n" +
                "   \"as_int\" integer,\n" +
                "   \"as_long\" bigint,\n" +
                "   \"as_byte\" tinyint,\n" +
                "   \"as_short\" smallint,\n" +
                "   \"as_boolean\" boolean,\n" +
                "   \"as_float\" real,\n" +
                "   \"as_double\" double,\n" +
                "   \"as_string\" varchar,\n" +
                "   \"as_binary\" varbinary,\n" +
                "   \"as_big_decimal\" decimal(1,0)\n" +
                ")\n" +
                "WITH (\n" +
                "   external_location = '%s'\n" +
                ")";

        String expectedSqlCommand = format(createTableQueryTemplate, fullTableName, goldenTablePath(tableName));

        String showCreateTableCommandResult = (String) computeActual("SHOW CREATE TABLE " + fullTableName).getOnlyValue();

        assertEquals(showCreateTableCommandResult, expectedSqlCommand);
    }

    @Test
    public void testShowCreateTableWithPartitionedColumns()
    {
        String path = "delta_v3/data-reader-partition-values";
        String fullTableName = format("%s.%s.\"%s\"", DELTA_CATALOG, DELTA_SCHEMA.toLowerCase(), path);
        String createTableQueryTemplate = "CREATE TABLE %s (\n" +
                "   \"as_int\" integer,\n" +
                "   \"as_long\" bigint,\n" +
                "   \"as_byte\" tinyint,\n" +
                "   \"as_short\" smallint,\n" +
                "   \"as_boolean\" boolean,\n" +
                "   \"as_float\" real,\n" +
                "   \"as_double\" double,\n" +
                "   \"as_string\" varchar,\n" +
                "   \"as_date\" date,\n" +
                "   \"as_timestamp\" timestamp,\n" +
                "   \"as_big_decimal\" decimal(38,18),\n" +
                "   \"value\" varchar\n" +
                ")\n" +
                "WITH (\n" +
                "   external_location = '%s',\n" +
                "   partitioned_by = ARRAY['as_int','as_long','as_byte','as_short','as_boolean','as_float','as_double','as_string','as_date','as_timestamp','as_big_decimal']\n" +
                ")";
        String expectedSqlCommand = format(createTableQueryTemplate, fullTableName, goldenTablePath(path));
        String showCreateTableCommandResult = (String) computeActual("SHOW CREATE TABLE " + fullTableName).getOnlyValue();
        assertEquals(showCreateTableCommandResult, expectedSqlCommand);
    }

    @Test
    public void testShowCreateTableWithClusteredColumns()
    {
        String path = "delta_v3/test_liquid_clustering";
        String fullTableName = format("%s.%s.\"%s\"", DELTA_CATALOG, DELTA_SCHEMA.toLowerCase(), path);
        String createTableQueryTemplate = "CREATE TABLE %s (\n" +
                "   \"id\" integer,\n" +
                "   \"region\" varchar,\n" +
                "   \"category\" varchar,\n" +
                "   \"amount\" double,\n" +
                "   \"event_date\" date\n" +
                ")\n" +
                "WITH (\n" +
                "   clustered_by = ARRAY['region','event_date'],\n" +
                "   external_location = '%s'\n" +
                ")";
        String expectedSqlCommand = format(createTableQueryTemplate, fullTableName, goldenTablePath(path));
        getQueryRunner().execute(format("CREATE TABLE %s (a INTEGER) WITH (external_location = '%s')", fullTableName, goldenTablePath(path)));
        String showCreateTableCommandResult = (String) computeActual("SHOW CREATE TABLE " + fullTableName).getOnlyValue();
        assertEquals(showCreateTableCommandResult, expectedSqlCommand);
    }
}
