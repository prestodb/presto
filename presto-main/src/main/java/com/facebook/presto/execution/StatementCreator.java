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
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstExpressionRewriter;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.ParameterCollector;
import com.facebook.presto.sql.tree.Statement;
import com.google.inject.Inject;

import java.util.List;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INCORRECT_NUMBER_OF_PARAMETERS;
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
        String sqlString = session.getPreparedStatement(name);

        statement = sqlParser.createStatement(sqlString);

        // validate that we have the right number of parameters
        validateParameters(statement, execute.getParameters());

        // replace Parameter expressions with the supplied values.
        ParameterRewriter parameterRewriter = new ParameterRewriter(execute.getParameters());
        AstExpressionRewriter expressionRewriter = new AstExpressionRewriter(parameterRewriter);
        return (Statement) expressionRewriter.process(statement, null);
    }

    public static void validateParameters(Node node, List<Literal> parameterValues)
    {
        ParameterCollector collector = new ParameterCollector();
        collector.process(node, null);
        if (parameterValues.size() != collector.getParameterCount()) {
            throw new SemanticException(INCORRECT_NUMBER_OF_PARAMETERS, node, "Incorrect number of parameters: expected " + collector.getParameterCount() + " but found " + parameterValues.size());
        }
    }
}
