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
package com.facebook.presto.plugin.oracle;

import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.MountableFile;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;

public class OracleServerTester
        extends OracleContainer
        implements Closeable
{
    private static final String TEST_TABLESPACE = "presto_test";

    public static final String TEST_USER = "presto_test";
    public static final String TEST_SCHEMA = TEST_USER; // schema and user is the same thing in Oracle
    public static final String TEST_PASS = "presto_test_password";

    public OracleServerTester()
    {
        super("gvenzl/oracle-xe:11-slim");

        // Set custom username, password and database for the container
        withUsername(TEST_USER);
        withPassword(TEST_PASS);
        withDatabaseName(TEST_SCHEMA);
        
        withCopyFileToContainer(MountableFile.forClasspathResource("init.sql"), "/container-entrypoint-initdb.d/init.sql");

        start();

        // gvenzl/oracle-xe image creates the user automatically with the specified credentials
        // Just need to create tablespace and grant additional privileges
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
                Statement statement = connection.createStatement()) {
            statement.execute(format("CREATE TABLESPACE %s DATAFILE 'test_db.dat' SIZE 100M ONLINE", TEST_TABLESPACE));
            statement.execute(format("ALTER USER %s DEFAULT TABLESPACE %s", TEST_USER, TEST_TABLESPACE));
            statement.execute(format("GRANT UNLIMITED TABLESPACE TO %s", TEST_USER));
            statement.execute(format("GRANT ALL PRIVILEGES TO %s", TEST_USER));
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void execute(String sql)
    {
        execute(sql, TEST_USER, TEST_PASS);
    }

    public void execute(String sql, String user, String password)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl(), user, password);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        stop();
    }
}
