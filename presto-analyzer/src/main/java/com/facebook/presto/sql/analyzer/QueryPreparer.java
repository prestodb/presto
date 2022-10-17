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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.analyzer.utils.StatementUtils;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Execute;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.WarningHandlingLevel.AS_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.WARNING_AS_ERROR;
import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.sql.analyzer.ConstantExpressionVerifier.verifyExpressionIsConstant;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static com.facebook.presto.sql.analyzer.utils.ParameterExtractor.getParameterCount;
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

    public PreparedQuery prepareQuery(AnalyzerOptions analyzerOptions, String query, Map<String, String> preparedStatements, WarningCollector warningCollector)
            throws ParsingException, PrestoException, SemanticException
    {
        Statement wrappedStatement = sqlParser.createStatement(query, analyzerOptions.getParsingOptions());
        if (warningCollector.hasWarnings() && analyzerOptions.getWarningHandlingLevel() == AS_ERROR) {
            throw new PrestoException(WARNING_AS_ERROR, format("Warning handling level set to AS_ERROR. Warnings: %n %s",
                    warningCollector.getWarnings().stream()
                            .map(PrestoWarning::getMessage)
                            .collect(joining(System.lineSeparator()))));
        }
        return prepareQuery(analyzerOptions, wrappedStatement, preparedStatements);
    }

    public PreparedQuery prepareQuery(AnalyzerOptions analyzerOptions, Statement wrappedStatement, Map<String, String> preparedStatements)
            throws ParsingException, PrestoException, SemanticException
    {
        Statement statement = wrappedStatement;
        Optional<String> prepareSql = Optional.empty();
        if (statement instanceof Execute) {
            String preparedStatementName = ((Execute) statement).getName().getValue();
            prepareSql = Optional.ofNullable(preparedStatements.get(preparedStatementName));
            String query = prepareSql.orElseThrow(() -> new PrestoException(NOT_FOUND, "Prepared statement not found: " + preparedStatementName));
            statement = sqlParser.createStatement(query, analyzerOptions.getParsingOptions());
        }

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
        if (analyzerOptions.isLogFormattedQueryEnabled()) {
            formattedQuery = Optional.of(getFormattedQuery(statement, parameters));
        }
        return new PreparedQuery(wrappedStatement, statement, parameters, formattedQuery, prepareSql);
    }

    private static String getFormattedQuery(Statement statement, List<Expression> parameters)
    {
        String formattedQuery = formatSql(
                statement,
                parameters.isEmpty() ? Optional.empty() : Optional.of(parameters));
        return format("-- Formatted Query:%n%s", formattedQuery);
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
        private final Statement wrappedStatement;
        private final List<Expression> parameters;
        private final Optional<String> formattedQuery;
        private final Optional<String> prepareSql;

        public PreparedQuery(Statement wrappedStatement, Statement statement, List<Expression> parameters, Optional<String> formattedQuery, Optional<String> prepareSql)
        {
            this.wrappedStatement = requireNonNull(wrappedStatement, "wrappedStatement is null");
            this.statement = requireNonNull(statement, "statement is null");
            this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
            this.formattedQuery = requireNonNull(formattedQuery, "formattedQuery is null");
            this.prepareSql = requireNonNull(prepareSql, "prepareSql is null");
        }

        public Statement getStatement()
        {
            return statement;
        }

        public Statement getWrappedStatement()
        {
            return wrappedStatement;
        }

        public List<Expression> getParameters()
        {
            return parameters;
        }

        public Optional<String> getFormattedQuery()
        {
            return formattedQuery;
        }

        public Optional<String> getPrepareSql()
        {
            return prepareSql;
        }
    }
}
