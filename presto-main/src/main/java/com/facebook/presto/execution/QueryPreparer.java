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
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.util.StatementUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getWarningHandlingLevel;
import static com.facebook.presto.SystemSessionProperties.isLogFormattedQueryEnabled;
import static com.facebook.presto.execution.ParameterExtractor.getParameterCount;
import static com.facebook.presto.execution.warnings.WarningHandlingLevel.AS_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.WARNING_AS_ERROR;
import static com.facebook.presto.sql.ParsingUtil.createParsingOptions;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.analyzer.ConstantExpressionVerifier.verifyExpressionIsConstant;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class QueryPreparer
{
    private final SqlParser sqlParser;

    @Inject
    public QueryPreparer(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public PreparedQuery prepareQuery(Session session, String query, WarningCollector warningCollector)
            throws ParsingException, PrestoException, SemanticException
    {
        Statement wrappedStatement = sqlParser.createStatement(query, createParsingOptions(session, warningCollector));
        if (warningCollector.hasWarnings() && getWarningHandlingLevel(session) == AS_ERROR) {
            throw new PrestoException(WARNING_AS_ERROR, format("Warning handling level set to AS_ERROR. Warnings: %n %s",
                    warningCollector.getWarnings().stream()
                            .map(PrestoWarning::getMessage)
                            .collect(joining(System.lineSeparator()))));
        }
        return prepareQuery(session, wrappedStatement, warningCollector);
    }

    public PreparedQuery prepareQuery(Session session, Statement wrappedStatement, WarningCollector warningCollector)
            throws ParsingException, PrestoException, SemanticException
    {
        Statement statement = unwrapExecuteStatement(wrappedStatement, sqlParser, session, warningCollector);
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
        Optional<String> formattedQuery = Optional.empty();
        if (isLogFormattedQueryEnabled(session)) {
            formattedQuery = Optional.of(getFormattedQuery(statement, parameters));
        }
        return new PreparedQuery(statement, parameters, formattedQuery);
    }

    private static String getFormattedQuery(Statement statement, List<Expression> parameters)
    {
        String formattedQuery = formatSql(
                statement,
                parameters.isEmpty() ? Optional.empty() : Optional.of(parameters));
        return format("-- Formatted Query:\n%s", formattedQuery);
    }

    private static Statement unwrapExecuteStatement(Statement statement, SqlParser sqlParser, Session session, WarningCollector warningCollector)
    {
        if (!(statement instanceof Execute)) {
            return statement;
        }

        String sql = session.getPreparedStatementFromExecute((Execute) statement);
        return sqlParser.createStatement(sql, createParsingOptions(session, warningCollector));
    }

    private static void validateParameters(Statement node, List<Expression> parameterValues)
    {
        int parameterCount = getParameterCount(node);
        if (parameterValues.size() != parameterCount) {
            throw new SemanticException(INVALID_PARAMETER_USAGE, node, "Incorrect number of parameters: expected %s but found %s", parameterCount, parameterValues.size());
        }
        for (Expression expression : parameterValues) {
            verifyExpressionIsConstant(ImmutableSet.of(), expression);
        }
    }

    public static class PreparedQuery
    {
        private final Statement statement;
        private final List<Expression> parameters;
        private final Optional<String> formattedQuery;

        public PreparedQuery(Statement statement, List<Expression> parameters, Optional<String> formattedQuery)
        {
            this.statement = requireNonNull(statement, "statement is null");
            this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
            this.formattedQuery = requireNonNull(formattedQuery, "formattedQuery is null");
        }

        public Statement getStatement()
        {
            return statement;
        }

        public List<Expression> getParameters()
        {
            return parameters;
        }

        public Optional<String> getFormattedQuery()
        {
            return formattedQuery;
        }
    }
}
