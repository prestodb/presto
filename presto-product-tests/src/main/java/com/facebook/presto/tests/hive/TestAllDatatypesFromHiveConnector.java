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
import com.teradata.tempto.Requirements;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.Requires;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.table.MutableTableRequirement;
import com.teradata.tempto.fulfillment.table.MutableTablesState;
import com.teradata.tempto.fulfillment.table.TableDefinition;
import com.teradata.tempto.fulfillment.table.TableHandle;
import com.teradata.tempto.fulfillment.table.TableInstance;
import com.teradata.tempto.query.QueryResult;
import com.teradata.tempto.query.QueryType;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.POST_HIVE_1_0_1;
import static com.facebook.presto.tests.TestGroups.QUARANTINE;
import static com.facebook.presto.tests.TestGroups.SMOKE;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_ORC;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_PARQUET;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_RCFILE;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_TEXTFILE;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.onHive;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.populateDataToHiveTable;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.usingPrestoJdbcDriver;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.usingTeradataJdbcDriver;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.context.ThreadLocalTestContextHolder.testContext;
import static com.teradata.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static com.teradata.tempto.fulfillment.table.TableHandle.tableHandle;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;
import static com.teradata.tempto.query.QueryExecutor.query;
import static com.teradata.tempto.util.DateTimeUtils.parseTimestampInUTC;
import static java.lang.String.format;
import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.LONGNVARCHAR;
import static java.sql.JDBCType.LONGVARBINARY;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARBINARY;
import static java.sql.JDBCType.VARCHAR;

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
            return Requirements.compose(
                    MutableTableRequirement.builder(ALL_HIVE_SIMPLE_TYPES_ORC).withState(CREATED).build(),
                    immutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE));
        }
    }

    public static final class RcfileRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return Requirements.compose(
                    MutableTableRequirement.builder(ALL_HIVE_SIMPLE_TYPES_RCFILE).withState(CREATED).build(),
                    immutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE));
        }
    }

    public static final class ParquetRequirements
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return MutableTableRequirement.builder(ALL_HIVE_SIMPLE_TYPES_PARQUET).withState(CREATED).build();
        }
    }

    @Requires(TextRequirements.class)
    @Test(groups = {HIVE_CONNECTOR, SMOKE, QUARANTINE})
    public void testSelectAllDatatypesTextFile()
            throws SQLException
    {
        String tableName = ALL_HIVE_SIMPLE_TYPES_TEXTFILE.getName();

        assertProperAllDatatypesSchema(tableName);
        QueryResult queryResult = query(format("SELECT * FROM %s", tableName));

        assertColumnTypes(queryResult);
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

    @Requires(OrcRequirements.class)
    @Test(groups = {HIVE_CONNECTOR, QUARANTINE})
    public void testSelectAllDatatypesOrc()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(ALL_HIVE_SIMPLE_TYPES_ORC).getNameInDatabase();

        populateDataToHiveTable(tableName);

        assertProperAllDatatypesSchema(tableName);

        QueryResult queryResult = query(format("SELECT * FROM %s", tableName));
        assertColumnTypes(queryResult);
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
                        "kot binarny".getBytes()));
    }

    @Requires(RcfileRequirements.class)
    @Test(groups = {HIVE_CONNECTOR, QUARANTINE})
    public void testSelectAllDatatypesRcfile()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(ALL_HIVE_SIMPLE_TYPES_RCFILE).getNameInDatabase();

        populateDataToHiveTable(tableName);

        assertProperAllDatatypesSchema(tableName);

        QueryResult queryResult = query(format("SELECT * FROM %s", tableName));
        assertColumnTypes(queryResult);
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
                        "kot binarny".getBytes()));
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

    private void assertColumnTypes(QueryResult queryResult)
    {
        Connection connection = defaultQueryExecutor().getConnection();
        if (usingPrestoJdbcDriver(connection)) {
            assertThat(queryResult).hasColumns(
                    TINYINT,
                    SMALLINT,
                    INTEGER,
                    BIGINT,
                    REAL,
                    DOUBLE,
                    DECIMAL,
                    DECIMAL,
                    TIMESTAMP,
                    DATE,
                    LONGNVARCHAR,
                    LONGNVARCHAR,
                    CHAR,
                    BOOLEAN,
                    LONGVARBINARY
            );
        }
        else if (usingTeradataJdbcDriver(connection)) {
            assertThat(queryResult).hasColumns(
                    TINYINT,
                    SMALLINT,
                    INTEGER,
                    BIGINT,
                    REAL,
                    DOUBLE,
                    DECIMAL,
                    DECIMAL,
                    TIMESTAMP,
                    DATE,
                    VARCHAR,
                    VARCHAR,
                    CHAR,
                    BOOLEAN,
                    VARBINARY
            );
        }
        else {
            throw new IllegalStateException();
        }
    }

    @Requires(ParquetRequirements.class)
    @Test(groups = {HIVE_CONNECTOR, POST_HIVE_1_0_1, QUARANTINE})
    public void testSelectAllDatatypesParquetFile()
            throws SQLException
    {
        String tableName = mutableTableInstanceOf(ALL_HIVE_SIMPLE_TYPES_PARQUET).getNameInDatabase();

        onHive().executeQuery(format("INSERT INTO %s VALUES(" +
                "127," +
                "32767," +
                "2147483647," +
                "9223372036854775807," +
                "123.345," +
                "234.567," +
                "346," +
                "345.67800," +
                "'" + parseTimestampInUTC("2015-05-10 12:15:35.123").toString() + "'," +
                "'ala ma kota'," +
                "'ala ma kot'," +
                "'ala ma    '," +
                "true," +
                "'kot binarny'" +
                ")", tableName));

        assertThat(query(format("SHOW COLUMNS FROM %s", tableName), QueryType.SELECT).project(1, 2)).containsExactly(
                row("c_tinyint", "tinyint"),
                row("c_smallint", "smallint"),
                row("c_int", "integer"),
                row("c_bigint", "bigint"),
                row("c_float", "real"),
                row("c_double", "double"),
                row("c_decimal", "decimal(10,0)"),
                row("c_decimal_w_params", "decimal(10,5)"),
                row("c_timestamp", "timestamp"),
                row("c_string", "varchar"),
                row("c_varchar", "varchar(10)"),
                row("c_char", "char(10)"),
                row("c_boolean", "boolean"),
                row("c_binary", "varbinary")
        );

        QueryResult queryResult = query(format("SELECT * FROM %s", tableName));
        assertThat(queryResult).hasColumns(
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                DECIMAL,
                DECIMAL,
                TIMESTAMP,
                LONGNVARCHAR,
                LONGNVARCHAR,
                CHAR,
                BOOLEAN,
                LONGVARBINARY
        );

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
                        "ala ma kota",
                        "ala ma kot",
                        "ala ma    ",
                        true,
                        "kot binarny".getBytes()));
    }

    private static TableInstance mutableTableInstanceOf(TableDefinition tableDefinition)
    {
        if (tableDefinition.getDatabase().isPresent()) {
            return mutableTableInstanceOf(tableDefinition, tableDefinition.getDatabase().get());
        }
        else {
            return mutableTableInstanceOf(tableHandleInSchema(tableDefinition));
        }
    }

    private static TableInstance mutableTableInstanceOf(TableDefinition tableDefinition, String database)
    {
        return mutableTableInstanceOf(tableHandleInSchema(tableDefinition).inDatabase(database));
    }

    private static TableInstance mutableTableInstanceOf(TableHandle tableHandle)
    {
        return testContext().getDependency(MutableTablesState.class).get(tableHandle);
    }

    private static TableHandle tableHandleInSchema(TableDefinition tableDefinition)
    {
        TableHandle tableHandle = tableHandle(tableDefinition.getName());
        if (tableDefinition.getSchema().isPresent()) {
            tableHandle = tableHandle.inSchema(tableDefinition.getSchema().get());
        }
        return tableHandle;
    }
}
