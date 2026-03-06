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
package com.facebook.presto.plugin.jdbc.procedure;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.procedure.Procedure;
import com.facebook.presto.spi.procedure.Procedure.Argument;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Objects.requireNonNull;

public class ExecuteProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle EXECUTE = methodHandle(
            ExecuteProcedure.class,
            "execute",
            ConnectorSession.class,
            String.class);

    private final JdbcClient jdbcClient;

    @Inject
    public ExecuteProcedure(JdbcClient jdbcClient)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    public void execute(ConnectorSession session, String query)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doExecute(session, query);
        }
    }

    private void doExecute(ConnectorSession session, String query)
    {
        try (Connection connection = jdbcClient.getConnection(JdbcIdentity.from(session))) {
            connection.setReadOnly(false);
            try (Statement statement = connection.createStatement()) {
                //noinspection SqlSourceToSinkFlow
                statement.executeUpdate(query);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Failed to execute query. " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "execute",
                ImmutableList.of(new Argument("QUERY", VARCHAR)),
                EXECUTE.bindTo(this));
    }
}
