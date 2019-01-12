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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.Execute;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Statement;
import io.prestosql.util.StatementUtils;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static io.prestosql.execution.ParameterExtractor.getParameterCount;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.ParsingUtil.createParsingOptions;
import static io.prestosql.sql.analyzer.ConstantExpressionVerifier.verifyExpressionIsConstant;
import static io.prestosql.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

public class QueryPreparer
{
    private final SqlParser sqlParser;

    @Inject
    public QueryPreparer(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public PreparedQuery prepareQuery(Session session, String query)
            throws ParsingException, PrestoException, SemanticException
    {
        Statement wrappedStatement = sqlParser.createStatement(query, createParsingOptions(session));
        return prepareQuery(session, wrappedStatement);
    }

    public PreparedQuery prepareQuery(Session session, Statement wrappedStatement)
            throws ParsingException, PrestoException, SemanticException
    {
        Statement statement = unwrapExecuteStatement(wrappedStatement, sqlParser, session);
        if (statement instanceof Explain && ((Explain) statement).isAnalyze()) {
            Statement innerStatement = ((Explain) statement).getStatement();
            Optional<QueryType> innerQueryType = StatementUtils.getQueryType(innerStatement.getClass());
            if (!innerQueryType.isPresent() || innerQueryType.get() == QueryType.DATA_DEFINITION) {
                throw new PrestoException(NOT_SUPPORTED, "EXPLAIN ANALYZE doesn't support statement type: " + innerStatement.getClass().getSimpleName());
            }
        }
        List<Expression> parameters = ImmutableList.of();
        if (wrappedStatement instanceof Execute) {
            parameters = ((Execute) wrappedStatement).getParameters();
        }
        validateParameters(statement, parameters);
        return new PreparedQuery(statement, parameters);
    }

    private static Statement unwrapExecuteStatement(Statement statement, SqlParser sqlParser, Session session)
    {
        if (!(statement instanceof Execute)) {
            return statement;
        }

        String sql = session.getPreparedStatementFromExecute((Execute) statement);
        return sqlParser.createStatement(sql, createParsingOptions(session));
    }

    private static void validateParameters(Statement node, List<Expression> parameterValues)
    {
        int parameterCount = getParameterCount(node);
        if (parameterValues.size() != parameterCount) {
            throw new SemanticException(INVALID_PARAMETER_USAGE, node, "Incorrect number of parameters: expected %s but found %s", parameterCount, parameterValues.size());
        }
        for (Expression expression : parameterValues) {
            verifyExpressionIsConstant(emptySet(), expression);
        }
    }

    public static class PreparedQuery
    {
        private final Statement statement;
        private final List<Expression> parameters;

        public PreparedQuery(Statement statement, List<Expression> parameters)
        {
            this.statement = requireNonNull(statement, "statement is null");
            this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
        }

        public Statement getStatement()
        {
            return statement;
        }

        public List<Expression> getParameters()
        {
            return parameters;
        }
    }
}
