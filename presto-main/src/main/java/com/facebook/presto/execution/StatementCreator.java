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

package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Statement;
import com.google.inject.Inject;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static java.util.Objects.requireNonNull;

public class StatementCreator
{
    private final SqlParser sqlParser;

    @Inject
    public StatementCreator(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public Statement createStatement(String query, Session session)
    {
        Statement statement = sqlParser.createStatement(query);
        if (!(statement instanceof Execute)) {
            return statement;
        }

        // For execute queries, get the statement referenced in the query
        Execute execute = (Execute) statement;
        String name = execute.getName();
        String sqlString = session.getPreparedStatements().get(name);
        if (sqlString == null) {
            throw new PrestoException(NOT_FOUND, "Prepared statement not found: " + name);
        }

        statement = sqlParser.createStatement(sqlString);
        return statement;
    }
}
