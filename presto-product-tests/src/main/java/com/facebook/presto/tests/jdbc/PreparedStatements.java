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
package com.facebook.presto.tests.jdbc;

import com.teradata.tempto.BeforeTestWithContext;
import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.Requires;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.query.QueryResult;
import com.teradata.tempto.query.QueryType;
import io.airlift.log.Logger;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import static com.facebook.presto.tests.TestGroups.JDBC;
import static com.facebook.presto.tests.TestGroups.SIMBA_JDBC;
import static com.facebook.presto.tests.hive.AllSimpleTypesTableDefinitions.ALL_HIVE_SIMPLE_TYPES_TEXTFILE;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.usingTeradataJdbcDriver;

import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static com.teradata.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static com.teradata.tempto.fulfillment.table.TableRequirements.immutableTable;
import static com.teradata.tempto.fulfillment.table.TableRequirements.mutableTable;
import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;
import static com.teradata.tempto.query.QueryExecutor.param;

import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.BOOLEAN;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.DATE;
import static java.sql.JDBCType.DECIMAL;
import static java.sql.JDBCType.DOUBLE;
import static java.sql.JDBCType.FLOAT;
import static java.sql.JDBCType.INTEGER;
import static java.sql.JDBCType.REAL;
import static java.sql.JDBCType.SMALLINT;
import static java.sql.JDBCType.TIMESTAMP;
import static java.sql.JDBCType.TINYINT;
import static java.sql.JDBCType.VARBINARY;
import static java.sql.JDBCType.VARCHAR;

public class PreparedStatements
        extends ProductTest
{
    private static final Logger LOGGER = Logger.get(PreparedStatements.class);
    private static final String TABLE_NAME = "textfile_all_types";
    private static final String TABLE_NAME_MUTABLE = "all_types_table_name";

    private Connection connection;

    private static class ImmutableAllTypesTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return immutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE);
        }
    }

    private static class MutableAllTypesTable
            implements RequirementsProvider
    {
        @Override
        public Requirement getRequirements(Configuration configuration)
        {
            return mutableTable(ALL_HIVE_SIMPLE_TYPES_TEXTFILE, TABLE_NAME_MUTABLE, CREATED);
        }
    }

    @BeforeTestWithContext
    public void setup()
            throws SQLException
    {
        connection = defaultQueryExecutor().getConnection();
    }

    @Test(groups = {JDBC, SIMBA_JDBC})
    @Requires(ImmutableAllTypesTable.class)
    public void preparedSelectApi()
            throws SQLException
    {
        if (usingTeradataJdbcDriver(connection)) {
            String selectSql = "SELECT c_int FROM " + TABLE_NAME + " WHERE c_int = ?";
            final int testValue = 2147483647;

            QueryResult result = defaultQueryExecutor().executeQuery(selectSql, param(INTEGER, Integer.valueOf(testValue)));
            assertThat(result).containsOnly(row(testValue));

            result = defaultQueryExecutor().executeQuery(selectSql, param(INTEGER, null));
            assertThat(result).hasNoRows();

            result = defaultQueryExecutor().executeQuery(selectSql, param(INTEGER, Integer.valueOf(2)));
            assertThat(result).hasNoRows();
        }
        else {
            LOGGER.warn("preparedSelectApi() only applies to TeradataJdbcDriver");
        }
    }

    @Test(groups = {JDBC, SIMBA_JDBC})
    @Requires(ImmutableAllTypesTable.class)
    public void preparedSelectSql()
            throws SQLException
    {
        if (usingTeradataJdbcDriver(connection)) {
            String prepareSql = "PREPARE ps1 from SELECT c_int FROM " + TABLE_NAME + " WHERE c_int = ?";
            final int testValue = 2147483647;
            String executeSql = "EXECUTE ps1 using ";

            Statement statement = connection.createStatement();
            statement.execute(prepareSql);

            QueryResult result = QueryResult.forResultSet(statement.executeQuery(executeSql + testValue));
            assertThat(result).containsOnly(row(testValue));

            result = QueryResult.forResultSet(statement.executeQuery(executeSql + "NULL"));
            assertThat(result).hasNoRows();

            result = QueryResult.forResultSet(statement.executeQuery(executeSql + 2));
            assertThat(result).hasNoRows();
        }
        else {
            LOGGER.warn("preparedSelectSql() only applies to TeradataJdbcDriver");
        }
    }

    @Test(groups = {JDBC, SIMBA_JDBC})
    @Requires(MutableAllTypesTable.class)
    public void preparedInsertApi()
            throws SQLException
    {
        if (usingTeradataJdbcDriver(connection)) {
            String tableNameInDatabase = mutableTablesState().get(TABLE_NAME_MUTABLE).getNameInDatabase();
            String insertSql = "INSERT INTO " + tableNameInDatabase + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String selectSql = "SELECT * FROM " + tableNameInDatabase;

            defaultQueryExecutor().executeQuery(insertSql, QueryType.UPDATE,
                    param(TINYINT, 127),
                    param(SMALLINT, 32767),
                    param(INTEGER, 2147483647),
                    param(BIGINT, new BigInteger("9223372036854775807")),
                    param(FLOAT, Float.valueOf("123.345")),
                    param(DOUBLE, 234.567),
                    param(DECIMAL, BigDecimal.valueOf(345.678)),
                    param(DECIMAL, BigDecimal.valueOf(345.678)),
                    param(TIMESTAMP, Timestamp.valueOf("2015-05-10 12:15:35")),
                    param(DATE, Date.valueOf("2015-05-10")),
                    param(VARCHAR, "ala ma kota"),
                    param(VARCHAR, "ala ma kot"),
                    param(CHAR, "ala ma"),
                    param(BOOLEAN, Boolean.TRUE),
                    param(VARBINARY, "a290IGJpbmFybnk=".getBytes()));

            defaultQueryExecutor().executeQuery(insertSql, QueryType.UPDATE,
                    param(TINYINT, 1),
                    param(SMALLINT, 2),
                    param(INTEGER, 3),
                    param(BIGINT, 4),
                    param(FLOAT, Float.valueOf("5.6")),
                    param(DOUBLE, 7.8),
                    param(DECIMAL, BigDecimal.valueOf(91)),
                    param(DECIMAL, BigDecimal.valueOf(2.3)),
                    param(TIMESTAMP, Timestamp.valueOf("2012-05-10 1:35:15")),
                    param(DATE, Date.valueOf("2014-03-10")),
                    param(VARCHAR, "abc"),
                    param(VARCHAR, "def"),
                    param(CHAR, "ghi"),
                    param(BOOLEAN, Boolean.FALSE),
                    param(VARBINARY, "jkl".getBytes()));

            defaultQueryExecutor().executeQuery(insertSql, QueryType.UPDATE,
                    param(TINYINT, null),
                    param(SMALLINT, null),
                    param(INTEGER, null),
                    param(BIGINT, null),
                    param(FLOAT, null),
                    param(DOUBLE, null),
                    param(DECIMAL, null),
                    param(DECIMAL, null),
                    param(TIMESTAMP, null),
                    param(DATE, null),
                    param(VARCHAR, null),
                    param(VARCHAR, null),
                    param(CHAR, null),
                    param(BOOLEAN, null),
                    param(VARBINARY, null));

            QueryResult result = defaultQueryExecutor().executeQuery(selectSql);
            assertColumnTypes(result);
            assertThat(result).containsOnly(
                    row(
                            127,
                            32767,
                            2147483647,
                            new BigInteger("9223372036854775807"),
                            Float.valueOf("123.345"),
                            234.567,
                            BigDecimal.valueOf(345.678),
                            BigDecimal.valueOf(345.678),
                            Timestamp.valueOf("2015-05-10 12:15:35"),
                            Date.valueOf("2015-05-10"),
                            "ala ma kota",
                            "ala ma kota",
                            "ala ma",
                            Boolean.TRUE,
                            "a290IGJpbmFybnk=".getBytes()),
                    row(
                            1,
                            2,
                            3,
                            4,
                            Float.valueOf("5.6"),
                            7.8,
                            BigDecimal.valueOf(9.1),
                            BigDecimal.valueOf(2.3),
                            Timestamp.valueOf("2012-05-10 1:35:15"),
                            Date.valueOf("2014-03-10"),
                            "abc",
                            "def",
                            "ghi",
                            Boolean.FALSE,
                            "jkl".getBytes()),
                    row(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null));
        }
        else {
            LOGGER.warn("preparedInsertApi() only applies to TeradataJdbcDriver");
        }
    }

    @Test(groups = {JDBC, SIMBA_JDBC})
    @Requires(MutableAllTypesTable.class)
    public void preparedInsertSql()
            throws SQLException
    {
        if (usingTeradataJdbcDriver(connection)) {
            String tableNameInDatabase = mutableTablesState().get(TABLE_NAME_MUTABLE).getNameInDatabase();
            String prepareSql = "PREPARE ps1 from INSERT INTO " + tableNameInDatabase + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String selectSql = "SELECT * FROM " + tableNameInDatabase;
            String executeSql = "EXECUTE ps1 using ";

            Statement statement = connection.createStatement();
            statement.execute(prepareSql);
            statement.execute(executeSql +
                    "cast(127 as tinyint), " +
                    "cast(32767 as smallint), " +
                    "2147483647, " +
                    "9223372036854775807, " +
                    "cast(123.345 as float), " +
                    "cast(234.567 as double), " +
                    "cast(345.678 as decimal(10)), " +
                    "cast(345.678 as decimal(10,5)), " +
                    "timestamp '2015-05-10 12:15:35', " +
                    "date '2015-05-10', " +
                    "'ala ma kota', " +
                    "'ala ma kot', " +
                    "cast('ala ma' as char(10)), " +
                    "true, " +
                    "varbinary 'a290IGJpbmFybnk='");

            statement.execute(executeSql +
                    "cast(1 as tinyint), " +
                    "cast(2 as smallint), " +
                    "3, " +
                    "4, " +
                    "cast(5.6 as float), " +
                    "cast(7.8 as double), " +
                    "cast(9.1 as decimal(10)), " +
                    "cast(2.3 as decimal(10,5)), " +
                    "timestamp '2012-05-10 1:35:15', " +
                    "date '2014-03-10', " +
                    "'abc', " +
                    "'def', " +
                    "cast('ghi' as char(10)), " +
                    "true, " +
                    "varbinary 'jkl'");

            statement.execute(executeSql +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null, " +
                    "null");

            QueryResult result = defaultQueryExecutor().executeQuery(selectSql);
            assertColumnTypes(result);
            assertThat(result).containsOnly(
                    row(
                            127,
                            32767,
                            2147483647,
                            new BigInteger("9223372036854775807"),
                            Float.valueOf("123.345"),
                            234.567,
                            BigDecimal.valueOf(345.678),
                            BigDecimal.valueOf(345.678),
                            Timestamp.valueOf("2015-05-10 12:15:35"),
                            Date.valueOf("2015-05-10"),
                            "ala ma kota",
                            "ala ma kota",
                            "ala ma",
                            Boolean.TRUE,
                            "a290IGJpbmFybnk=".getBytes()),
                    row(
                            1,
                            2,
                            3,
                            4,
                            Float.valueOf("5.6"),
                            7.8,
                            BigDecimal.valueOf(9.1),
                            BigDecimal.valueOf(2.3),
                            Timestamp.valueOf("2012-05-10 1:35:15"),
                            Date.valueOf("2014-03-10"),
                            "abc",
                            "def",
                            "ghi",
                            Boolean.FALSE,
                            "jkl".getBytes()),
                    row(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null));
        }
        else {
            LOGGER.warn("preparedInsertSql() only applies to TeradataJdbcDriver");
        }
    }

    private void assertColumnTypes(QueryResult queryResult)
    {
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
}
