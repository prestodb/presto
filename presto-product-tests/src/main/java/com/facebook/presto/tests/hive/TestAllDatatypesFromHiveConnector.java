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
import com.teradata.tempto.query.QueryResult;
import com.teradata.tempto.query.QueryType;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.QUARANTINE;
import static com.facebook.presto.tests.TestGroups.SMOKE;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_ORC;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_PARQUET;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_RCFILE;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_TEXTFILE;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static com.teradata.tempto.query.QueryExecutor.query;
import static com.teradata.tempto.util.DateTimeUtils.parseTimestampInUTC;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.LONGNVARCHAR;
import static java.sql.JDBCType.LONGVARBINARY;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TINYINT;

public class TestAllDatatypesFromHiveConnector
        extends ProductTest
{
    public static final class TextRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE);
        }
    }

    public static final class OrcRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(ALL_HIVE_SIMPLE_TYPES_ORC);
        }
    }

    public static final class RcfileRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(ALL_HIVE_SIMPLE_TYPES_RCFILE);
        }
    }

    public static final class ParquetRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(ALL_HIVE_SIMPLE_TYPES_PARQUET);
        }
    }

    @Requires(TextRequirements.class)
    @Test(groups = {HIVE_CONNECTOR, SMOKE})
    public void testSelectAllDatatypesTextFile()
            throws SQLException
    {
        assertProperAllDatatypesSchema("textfile_all_types");
        QueryResult queryResult = query("SELECT * " +
                "FROM textfile_all_types");

        assertColumnTypes(queryResult);
        assertThat(queryResult).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.34500122070312, // (double) 123.345f - see limitation #1
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        parseTimestampInUTC("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        true,
                        "kot binarny".getBytes()
                )
        );
    }

    @Requires(OrcRequirements.class)
    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectAllDatatypesOrc()
            throws SQLException
    {
        assertProperAllDatatypesSchema("orc_all_types");

        QueryResult queryResult = query("SELECT * " +
                "FROM orc_all_types");
        assertColumnTypes(queryResult);
        assertThat(queryResult).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        (double) 123.345f, // (double) 123.345f - see limitation #1
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        parseTimestampInUTC("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        true,
                        "kot binarny".getBytes()));
    }

    @Requires(RcfileRequirements.class)
    @Test(groups = {HIVE_CONNECTOR})
    public void testSelectAllDatatypesRcfile()
            throws SQLException
    {
        assertProperAllDatatypesSchema("rcfile_all_types");

        QueryResult queryResult = query("SELECT * " +
                "FROM rcfile_all_types");
        assertColumnTypes(queryResult);
        assertThat(queryResult).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.345, // for some reason we do not get float/double conversion issue like for text files
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        parseTimestampInUTC("2015-05-10 12:15:35.123"),
                        Date.valueOf("2015-05-10"),
                        "ala ma kota",
                        "ala ma kot",
                        true,
                        "kot binarny".getBytes()));
    }

    private void assertProperAllDatatypesSchema(String tableName)
    {
        assertThat(query("SHOW COLUMNS FROM " + tableName, QueryType.SELECT).project(1, 2)).containsExactly(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "double"),
                row("c_double", "double"),
                row("c_decimal", "decimal(10,0)"),
                row("c_decimal_w_params", "decimal(10,5)"),
                row("c_timestamp", "timestamp"),
                row("c_date", "date"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_boolean", "boolean"),
                row("c_binary", "varbinary")
        );
    }

    private void assertColumnTypes(QueryResult queryResult)
    {
        assertThat(queryResult).hasColumns(
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                DOUBLE,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                TIMESTAMP,
                DATE,
                LONGNVARCHAR,
                LONGNVARCHAR,
                BOOLEAN,
                LONGVARBINARY
        );
    }

    @Requires(ParquetRequirements.class)
    @Test(groups = {HIVE_CONNECTOR, QUARANTINE})
    public void testSelectAllDatatypesParquetFile()
            throws SQLException
    {
        assertThat(query("SHOW COLUMNS FROM parquet_all_types", QueryType.SELECT).project(1, 2)).containsExactly(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "double"),
                row("c_double", "double"),
                row("c_decimal", "decimal(10,0)"),
                row("c_decimal_w_params", "decimal(10,5)"),
                row("c_timestamp", "timestamp"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_boolean", "boolean"),
                row("c_binary", "varbinary")
        );

        assertThat(query("SELECT * FROM parquet_all_types")).containsOnly(
                row(
                        127,
                        32767,
                        2147483647,
                        9223372036854775807L,
                        123.34500122070312, // (double) 123.345f - see limitation #1
                        234.567,
                        new BigDecimal("346"),
                        new BigDecimal("345.67800"),
                        parseTimestampInUTC("2015-05-10 12:15:35.123"),
                        "ala ma kota",
                        "ala ma kot",
                        true,
                        "kot binarny".getBytes()));
    }
    // presto limitations referenced above:
    //
    // #1 we have float column with value in 123.345. But presto exposes this column as DOUBLE.
    //    As a result it is processed internally and exposed to the user as java double instead java float,
    //    which have different string representation from what is in hive data file.
    //    For 123.345 we get 123.34500122070312
}
