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
package com.facebook.presto.hdfs.jdbc;

import io.airlift.log.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author jelly.guodong.jin@gmail.com
 */
public class JDBCDriver
{
    private static final Logger log = Logger.get(JDBCDriver.class);

    private final String jdbcDriver;
    private final String user;
    private final String pass;
    private final String dbUrl;
    private static Connection connection;

    public JDBCDriver(
            String jdbcDriver,
            String user,
            String pass,
            String dbUrl)
    {
        this.jdbcDriver = requireNonNull(jdbcDriver, "jdbcDriver is null");
        this.user = requireNonNull(user, "user is null");
        this.pass = requireNonNull(pass, "pass is null");
        this.dbUrl = requireNonNull(dbUrl, "dbUrl is null");

        setup();
    }

    private void setup()
    {
        try {
//            Class.forName(jdbcDriver);
            connection = DriverManager.getConnection(dbUrl, user, pass);
        }
//        catch (ClassNotFoundException e) {
//            log.error(e, "JDBC driver class not found");
//        }
        catch (SQLException e) {
            log.error(e, "JDBC driver connection error");
        }
    }

    public void shutdown()
    {
        try {
            connection.close();
        }
        catch (SQLException e) {
            log.error(e, "Close connection error");
        }
    }

    public List<JDBCRecord> executeQuery(String sql, String[] fields)
    {
        List<JDBCRecord> recordList = new ArrayList<>();
        try (
                PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet resultSet = statement.executeQuery()) {
            log.debug("Executing: " + sql);
            while (resultSet != null && resultSet.next()) {
                JDBCRecord record = new JDBCRecord();
                for (String field : fields) {
                    record.setField(field, resultSet.getString(field));
                }
                recordList.add(record);
            }
        }
        catch (SQLException e) {
            log.debug("Error sql: " + sql);
            log.error(e, "Sql execute error.");
        }
        return recordList;
    }

    public int executeUpdate(String sql)
    {
        try (
                PreparedStatement statement = connection.prepareStatement(sql)
                ) {
            return statement.executeUpdate();
        }
        catch (SQLException e) {
            e.printStackTrace();
            log.debug("Error sql: " + sql);
            log.error(e, "DDL error");
        }
        return -1;
    }

    public DatabaseMetaData getDbMetaData()
    {
        try {
            return connection.getMetaData();
        }
        catch (SQLException e) {
            log.error(e, "get metadata error");
            shutdown();
        }
        return null;
    }
}
