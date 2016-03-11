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

package com.facebook.presto.tests.hive;

import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.Requires;
import com.teradata.tempto.configuration.Configuration;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.QUARANTINE;
import static com.facebook.presto.tests.TestGroups.SMOKE;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_KNOWN_TO_PRESTO_TEXTFILE;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_TEXTFILE;
import static com.teradata.tempto.Requirements.compose;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static com.teradata.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static com.teradata.tempto.fulfillment.table.TableRequirements.mutableTable;
import static com.teradata.tempto.query.QueryExecutor.query;
import static com.teradata.tempto.util.DateTimeUtils.parseTimestampInUTC;

public class TestInsertIntoHiveTable
        extends ProductTest
{
    private static final String TABLE_NAME = "target_table";

    private static class AllSimpleTypesTables
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return compose(
                    mutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE, TABLE_NAME, CREATED),
                    immutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE));
        }
    }

    private static class AllSimpleTypesKnownToPrestoTables
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return compose(
                    mutableTable(ALL_HIVE_SIMPLE_TYPES_KNOWN_TO_PRESTO_TEXTFILE, TABLE_NAME, CREATED),
                    immutableTable(ALL_HIVE_SIMPLE_TYPES_KNOWN_TO_PRESTO_TEXTFILE));
        }
    }

    @Test(groups = {HIVE_CONNECTOR, QUARANTINE})
    @Requires(AllSimpleTypesTables.class)
    public void testInsertIntoValuesToHiveTableAllHiveSimpleTypes()
    {
        String tableNameInDatabase = mutableTablesState().get(TABLE_NAME).getNameInDatabase();
        assertThat(query("SELECT * FROM " + tableNameInDatabase)).hasNoRows();
        query("INSERT INTO " + tableNameInDatabase + " VALUES(" +
                "127, " +
                "32767, " +
                "2147483647, " +
                "9223372036854775807, " +
                "123.345, " +
                "234.567, " +
                "timestamp '2015-05-10 12:15:35.123', " +
                "date '2015-05-10', " +
                "'ala ma kota', " +
                "'ala ma kota', " +
                "true, " +
                "from_base64('a290IGJpbmFybnk=')" +
                ")");

        assertThat(query("SELECT * FROM " + tableNameInDatabase)).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.34500122070312,
                        234.567,
                        Timestamp.valueOf("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        true,
                        "kot binarny".getBytes()));
    }

    @Test(groups = {HIVE_CONNECTOR, QUARANTINE})
    @Requires(AllSimpleTypesTables.class)
    public void testInsertIntoSelectToHiveTableAllHiveSimpleTypes()
    {
        String tableNameInDatabase = mutableTablesState().get(TABLE_NAME).getNameInDatabase();
        assertThat(query("SELECT * FROM " + tableNameInDatabase)).hasNoRows();
        assertThat(query("INSERT INTO " + tableNameInDatabase + " SELECT * from textfile_all_types")).containsExactly(row(1));
        assertThat(query("SELECT * FROM " + tableNameInDatabase)).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.34500122070312,
                        234.567,
                        Timestamp.valueOf("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        true,
                        "kot binarny".getBytes()));
    }

    @Test(groups = {HIVE_CONNECTOR, SMOKE, QUARANTINE})
    @Requires(AllSimpleTypesKnownToPrestoTables.class)
    public void testInsertIntoValuesToHiveTableAllHiveSimpleTypesKnownToPresto()
    {
        String tableNameInDatabase = mutableTablesState().get(TABLE_NAME).getNameInDatabase();
        assertThat(query("SELECT * FROM " + tableNameInDatabase)).hasNoRows();
        query("INSERT INTO " + tableNameInDatabase + " VALUES(" +
                "127, " +
                "32767, " +
                "2147483647, " +
                "9223372036854775807, " +
                "123.345, " +
                "234.567, " +
                "timestamp '2015-05-10 12:15:35.123', " +
                "date '2015-05-10', " +
                "'ala ma kota', " +
                "'ala ma kota', " +
                "'ala ma', " +
                "true, " +
                "from_base64('a290IGJpbmFybnk=')" +
                ")");

        assertThat(query("SELECT * FROM " + tableNameInDatabase)).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.34500122070312,
                        234.567,
                        Timestamp.valueOf("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes()));
    }

    @Test(groups = {HIVE_CONNECTOR, SMOKE, QUARANTINE})
    @Requires(AllSimpleTypesKnownToPrestoTables.class)
    public void testInsertIntoSelectToHiveTableAllHiveSimpleTypesKnownToPresto()
    {
        String tableNameInDatabase = mutableTablesState().get(TABLE_NAME).getNameInDatabase();
        assertThat(query("SELECT * FROM " + tableNameInDatabase)).hasNoRows();
        query("INSERT INTO " + tableNameInDatabase + " SELECT * from textfile_all_types_known_to_presto");
        assertThat(query("SELECT * FROM " + tableNameInDatabase)).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.34500122070312,
                        234.567,
                        parseTimestampInUTC("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes()));
    }
}
