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
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;

import static com.facebook.presto.tests.TemptoProductTestRunner.PRODUCT_TESTS_TIME_ZONE;
import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.TIMESTAMP;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.resetSessionProperty;
import static com.facebook.presto.tests.utils.JdbcDriverUtils.setSessionProperty;
import static com.facebook.presto.tests.utils.QueryExecutors.onHive;
import static com.teradata.tempto.assertions.QueryAssert.Row.row;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;
import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;
import static com.teradata.tempto.query.QueryExecutor.query;
import static com.teradata.tempto.util.DateTimeUtils.parseTimestampInLocalTime;

public class TestTimestampCompatibility
        extends ProductTest
{
    private static final String TABLE_NAME = "timestamp_compatibility";
    private static final String TIMESTAMP_LITERAL = "2000-01-01 00:00:00";
    private static final Timestamp EXPECTED_TIMESTAMP = parseTimestampInLocalTime(TIMESTAMP_LITERAL, PRODUCT_TESTS_TIME_ZONE);

    // format, params, legacy_mode
    @DataProvider(name = "storage_formats")
    public static Object[][] storageFormats()
    {
        return new String[][] {
                {"ORC", "true"},
                {"ORC", "false"},
                {"PARQUET", "true"},
                {"PARQUET", "false"},
                {"RCBINARY", "true"},
                {"RCBINARY", "false"},
                {"RCTEXT", "true"},
                {"RCTEXT", "false"},
                {"SEQUENCEFILE", "true"},
                {"SEQUENCEFILE", "false"},
                {"TEXTFILE", "true"},
                {"TEXTFILE", "false"}
        };
    }

    @Test(dataProvider = "storage_formats", groups = {HIVE_CONNECTOR, TIMESTAMP})
    public void testTimestampCompatibility(String storageFormat, String legacyTimestamp)
            throws SQLException
    {
        Connection connection = defaultQueryExecutor().getConnection();
        setSessionProperty(connection, "legacy_timestamp", legacyTimestamp);

        query(String.format("DROP TABLE IF EXISTS %s", TABLE_NAME));
        query(String.format("CREATE TABLE %s WITH (format = '%s') AS SELECT TIMESTAMP '%s' ts", TABLE_NAME, storageFormat, TIMESTAMP_LITERAL));

        QueryResult prestoResult = query(String.format("SELECT * FROM %s", TABLE_NAME));
        QueryResult hiveResult = onHive().executeQuery(String.format("SELECT * FROM %s", TABLE_NAME));
        assertThat(hiveResult).containsExactly(row(EXPECTED_TIMESTAMP));
        assertThat(prestoResult).containsExactly(row(EXPECTED_TIMESTAMP));

        resetSessionProperty(connection, "legacy_timestamp");
    }
}
