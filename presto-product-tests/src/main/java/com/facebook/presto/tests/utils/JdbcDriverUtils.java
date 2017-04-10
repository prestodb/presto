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
package com.facebook.presto.tests.utils;

import com.facebook.presto.jdbc.PrestoConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcDriverUtils
{
    public static String getSessionProperty(Connection connection, String key) throws SQLException
    {
        return getSessionProperty(connection, key, "Value");
    }

    public static String getSessionPropertyDefault(Connection connection, String key) throws SQLException
    {
        return getSessionProperty(connection, key, "Default");
    }

    private static String getSessionProperty(Connection connection, String key, String valueType) throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SHOW SESSION");
            while (rs.next()) {
                if (rs.getString("Name").equals(key)) {
                    return rs.getString(valueType);
                }
            }
        }
        return null;
    }

    public static void setSessionProperty(Connection connection, String key, String value) throws SQLException
    {
        if (usingPrestoJdbcDriver(connection)) {
            PrestoConnection prestoConnection = connection.unwrap(PrestoConnection.class);
            prestoConnection.setSessionProperty(key, value);
        }
        else if (usingTeradataJdbcDriver(connection)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format("set session %s=%s", key, value));
            }
        }
        else {
            throw new IllegalStateException();
        }
    }

    public static void resetSessionProperty(Connection connection, String key) throws SQLException
    {
        if (usingPrestoJdbcDriver(connection)) {
            setSessionProperty(connection, key, getSessionPropertyDefault(connection, key));
        }
        else if (usingTeradataJdbcDriver(connection)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format("RESET SESSION %s", key));
            }
        }
        else {
            throw new IllegalStateException();
        }
    }

    public static boolean usingPrestoJdbcDriver(Connection connection)
    {
        return getClassNameForJdbcDriver(connection).equals("com.facebook.presto.jdbc.PrestoConnection");
    }

    public static boolean usingTeradataJdbcDriver(Connection connection)
    {
        return  getClassNameForJdbcDriver(connection).startsWith("com.teradata.jdbc.");
    }

    public static boolean usingTeradataJdbc4Driver(Connection connection)
    {
        return getClassNameForJdbcDriver(connection).startsWith("com.teradata.jdbc.jdbc4.");
    }

    private static String getClassNameForJdbcDriver(Connection connection)
    {
        return connection.getClass().getCanonicalName();
    }

    private JdbcDriverUtils() {}
}
