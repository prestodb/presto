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
package com.facebook.presto.plugin.postgresql;

import com.nesscomputing.db.postgres.embedded.EmbeddedPostgreSQL;
import io.airlift.log.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public final class TestingPostgreSqlServer
        implements Closeable
{
    private static final Logger log = Logger.get(TestingPostgreSqlServer.class);

    private final String user;
    private final String database;
    private final int port;
    private final EmbeddedPostgreSQL server;

    public TestingPostgreSqlServer(String user, String database)
            throws Exception
    {
        this.user = checkNotNull(user, "user is null");
        this.database = checkNotNull(database, "database is null");

        server = EmbeddedPostgreSQL.builder().start();
        port = server.getPort();

        try (Connection connection = server.getPostgresDatabase().getConnection()) {
            try (Statement statement = connection.createStatement()) {
                execute(statement, format("CREATE ROLE %s WITH LOGIN", user));
                execute(statement, format("CREATE DATABASE %s OWNER %s ENCODING = 'utf8'", database, user));
            }
        }
        catch (SQLException e) {
            close();
            throw e;
        }

        log.info("PostgreSQL server ready: %s", getJdbcUrl());
    }

    private static void execute(Statement statement, String sql)
            throws SQLException
    {
        log.debug("Executing: %s", sql);
        statement.execute(sql);
    }

    @Override
    public void close()
            throws IOException
    {
        server.close();
    }

    public String getUser()
    {
        return user;
    }

    public String getDatabase()
    {
        return database;
    }

    public int getPort()
    {
        return port;
    }

    public String getJdbcUrl()
    {
        return server.getJdbcUrl(user, database);
    }
}
