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
import com.teradata.tempto.query.QueryExecutionException;
import com.teradata.tempto.query.QueryResult;
import com.teradata.tempto.query.QueryType;
import io.airlift.log.Logger;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;

import static com.facebook.presto.tests.TestGroups.S3_CONNECTOR;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;
import static com.teradata.tempto.query.QueryExecutor.query;
import static com.teradata.tempto.util.DateTimeUtils.parseTimestampInUTC;
import static java.lang.String.format;

// This class is intended to be used to test the s3 connector (in particular, to test tables stored
// in s3, and available in the Hive metastore). The tests in this class require some predefined tables
// with data loaded. Though they are meant to test an s3 configuration, they can run on any cluster
// that has the required tables loaded in hive. If the tables do not exist, these tests will log a warning
// and move on.
public class TestHiveS3Connector
        extends ProductTest
{
    private static final Logger LOGGER = Logger.get(TestHiveS3Connector.class);

    @Test(groups = {S3_CONNECTOR})
    public void testSelectFromTextFile()
            throws SQLException
    {
        // This test uses a standard tpch nation table.  It assumes that table is already created in the hive catalog
        // and has data loaded.
        String tableName = "hive.default.nation_s3";
        if (isCreated(tableName)) {
            QueryResult queryResult = query(format("SELECT n_name FROM %s WHERE n_nationkey = 7", tableName));
            assertThat(queryResult).containsOnly(row("GERMANY"));
        }
        else {
            LOGGER.warn(format("Skipping test. Table %s does not exist.", tableName));
        }
    }

    @Test(groups = {S3_CONNECTOR})
    public void testAllDataTypesTextFile()
            throws SQLException
    {
        // This test uses the all_types table as defined in the AllSimpleTypesTableDefinition class and
        // data from hive/data/all_types/data.textfile. The table is create with the name 'alltypes_text'.
        // The test assumes that table is already created in the hive catalog and has data loaded.
        String tableName = "alltypes_s3_text";
        if (isCreated(tableName)) {
            assertProperAllDatatypesSchema(tableName);
            QueryResult queryResult = query(format("SELECT * FROM %s", tableName));

            assertThat(queryResult).containsOnly(
                    row(
                            127,
                            32767,
                            2147483647,
                            9223372036854775807L,
                            123.345f,
                            234.567,
                            new BigDecimal("346"),
                            new BigDecimal("345.67800"),
                            parseTimestampInUTC("2015-05-10 12:15:35.123"),
                            Date.valueOf("2015-05-10"),
                            "ala ma kota",
                            "ala ma kot",
                            "ala ma    ",
                            true,
                            "kot binarny".getBytes()
                    )
            );
        }
        else {
            LOGGER.warn(format("Skipping test. Table %s does not exist.", tableName));
        }
    }

    @Test(groups = {S3_CONNECTOR})
    public void testAllDataTypesRcFile()
            throws SQLException
    {
        // This test uses the all_types table as defined in the AllSimpleTypesTableDefinition class with data
        // from hive/data/all_types/data.rcfile.  the table is created with the name alltypes_rcfile.
        // The test assumes that table is already created in the hive catalog and has data loaded.

        String tableName = "alltypes_s3_rcfile";
        if (isCreated(tableName)) {
            assertProperAllDatatypesSchema(tableName);
            QueryResult queryResult = query(format("SELECT * FROM %s", tableName));

            assertThat(queryResult).containsOnly(
                    row(
                            127,
                            32767,
                            2147483647,
                            9223372036854775807L,
                            123.345f,
                            234.567,
                            new BigDecimal("346"),
                            new BigDecimal("345.67800"),
                            parseTimestampInUTC("2015-05-10 12:15:35.123"),
                            Date.valueOf("2015-05-10"),
                            "ala ma kota",
                            "ala ma kot",
                            "ala ma    ",
                            true,
                            "kot binarny".getBytes()
                    )
            );
        }
        else {
            LOGGER.warn(format("Skipping test. Table %s does not exist.", tableName));
        }
    }

    @Test(groups = {S3_CONNECTOR})
    public void testAllDataTypesOrc()
            throws SQLException
    {
        // This test uses the all_types table as defined in the AllSimpleTypesTableDefinition class and
        // with data from hive/data/all_types/data.orc.  The table is created with the name alltypes_orc.
        // The test assumes that table is already created in the hive catalog and has data loaded.

        String tableName = "alltypes_orc";
        if (isCreated(tableName)) {
            assertProperAllDatatypesSchema(tableName);
            QueryResult queryResult = query(format("SELECT * FROM %s", tableName));

            assertThat(queryResult).containsOnly(
                    row(
                            127,
                            32767,
                            2147483647,
                            9223372036854775807L,
                            123.345f,
                            234.567,
                            new BigDecimal("346"),
                            new BigDecimal("345.67800"),
                            parseTimestampInUTC("2015-05-10 12:15:35.123"),
                            Date.valueOf("2015-05-10"),
                            "ala ma kota",
                            "ala ma kot",
                            "ala ma    ",
                            true,
                            "kot binarny".getBytes()
                    )
            );
        }
        else {
            LOGGER.warn(format("Skipping test. Table %s does not exist.", tableName));
        }
    }

    @Test(groups = {S3_CONNECTOR})
    public void testAllDataTypesParquet()
            throws SQLException
    {
        // This test uses all_types table for parquet as defined in the AllSimpleTypesTableDefinition class.  The data
        // was generated by inserting and relevant columns into a parquet table using the data in
        // hive/data/all_types/data.textfile. The table is created with the name alltypes_parquet.
        // The test assumes that table is already created in the hive catalog and has data loaded.

        String tableName = "alltypes_s3_parquet";
        if (isCreated(tableName)) {
            assertProperParquetDatatypesSchema(tableName);
            QueryResult queryResult = query(format("SELECT * FROM %s", tableName));

            assertThat(queryResult).containsOnly(
                    row(
                            127,
                            32767,
                            2147483647,
                            9223372036854775807L,
                            123.345f,
                            234.567,
                            parseTimestampInUTC("2015-05-10 12:15:35.123"),
                            "ala ma kota",
                            "ala ma kot",
                            "ala ma    ",
                            true
                    )
            );
        }
        else {
            LOGGER.warn(format("Skipping test. Table %s does not exist.", tableName));
        }
    }

    @Test(groups = {S3_CONNECTOR})
    public void shouldCreateTableAsSelect()
            throws Exception
    {
        String selectTable = "nation_s3";
        if (isCreated(selectTable)) {
            String createTable = "create_table_as_select_s3";
            query(format("DROP TABLE IF EXISTS %s", createTable));
            query(format("CREATE TABLE %s AS SELECT * FROM %s", createTable, selectTable));
            assertThat(query(format("SELECT * FROM %s", createTable))).hasRowsCount(25);
        }
        else {
            LOGGER.warn(format("Skipping test. Table %s does not exist.", selectTable));
        }
    }

    private void assertProperAllDatatypesSchema(String tableName)
    {
        assertThat(query("SHOW COLUMNS FROM " + tableName, QueryType.SELECT).project(1, 2)).containsExactly(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "real"),
                row("c_double", "double"),
                row("c_decimal", "decimal(10,0)"),
                row("c_decimal_w_params", "decimal(10,5)"),
                row("c_timestamp", "timestamp"),
                row("c_date", "date"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_char", "char(10)"),
                row("c_boolean", "boolean"),
                row("c_binary", "varbinary")
        );
    }

    private void assertProperParquetDatatypesSchema(String tableName)
    {
        assertThat(query("SHOW COLUMNS FROM " + tableName, QueryType.SELECT).project(1, 2)).containsExactly(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "real"),
                row("c_double", "double"),
                row("c_timestamp", "timestamp"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_char", "char(10)"),
                row("c_boolean", "boolean")
        );
    }

    private boolean isCreated(String tableName)
    {
        try {
            defaultQueryExecutor().executeQuery(format("SELECT * FROM %s LIMIT 1", tableName));
            return true;
        }
        catch (QueryExecutionException e) {
            return false;
        }
    }
}
