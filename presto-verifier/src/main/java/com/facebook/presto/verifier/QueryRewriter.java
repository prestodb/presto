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
package com.facebook.presto.verifier;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import io.airlift.units.Duration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.sql.SqlFormatter.formatSql;
import static com.facebook.presto.verifier.PrestoVerifier.statementToQueryType;
import static com.facebook.presto.verifier.QueryType.READ;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class QueryRewriter
{
    private static final Set<Integer> APPROXIMATE_TYPES = ImmutableSet.of(Types.REAL, Types.FLOAT, Types.DOUBLE);

    private final SqlParser parser;
    private final String gatewayUrl;
    private final QualifiedName rewritePrefix;
    private final Optional<String> catalogOverride;
    private final Optional<String> schemaOverride;
    private final Optional<String> usernameOverride;
    private final Optional<String> passwordOverride;
    private final int doublePrecision;
    private final Duration timeout;

    public QueryRewriter(SqlParser parser, String gatewayUrl, QualifiedName rewritePrefix, Optional<String> catalogOverride, Optional<String> schemaOverride, Optional<String> usernameOverride, Optional<String> passwordOverride, int doublePrecision, Duration timeout)
    {
        this.parser = requireNonNull(parser, "parser is null");
        this.gatewayUrl = requireNonNull(gatewayUrl, "gatewayUrl is null");
        this.rewritePrefix = requireNonNull(rewritePrefix, "rewritePrefix is null");
        this.catalogOverride = requireNonNull(catalogOverride, "catalogOverride is null");
        this.schemaOverride = requireNonNull(schemaOverride, "schemaOverride is null");
        this.usernameOverride = requireNonNull(usernameOverride, "usernameOverride is null");
        this.passwordOverride = requireNonNull(passwordOverride, "passwordOverride is null");
        this.doublePrecision = doublePrecision;
        this.timeout = requireNonNull(timeout, "timeout is null");
    }

    public Query shadowQuery(Query query)
            throws QueryRewriteException, SQLException
    {
        if (statementToQueryType(parser, query.getQuery()) == READ) {
            return query;
        }
        if (!query.getPreQueries().isEmpty()) {
            throw new QueryRewriteException("Cannot rewrite queries that use pre-queries");
        }
        if (!query.getPostQueries().isEmpty()) {
            throw new QueryRewriteException("Cannot rewrite queries that use post-queries");
        }

        Statement statement = parser.createStatement(query.getQuery());
        try (Connection connection = DriverManager.getConnection(gatewayUrl, usernameOverride.orElse(query.getUsername()), passwordOverride.orElse(query.getPassword()))) {
            trySetConnectionProperties(query, connection);
            if (statement instanceof CreateTableAsSelect) {
                return rewriteCreateTableAsSelect(connection, query, (CreateTableAsSelect) statement);
            }
        }

        throw new QueryRewriteException("Unsupported query type: " + statement.getClass());
    }

    private Query rewriteCreateTableAsSelect(Connection connection, Query query, CreateTableAsSelect statement)
            throws SQLException
    {
        List<String> parts = new ArrayList<>();
        int originalSize = statement.getName().getOriginalParts().size();
        int prefixSize = rewritePrefix.getOriginalParts().size();
        if (originalSize > prefixSize) {
            parts.addAll(statement.getName().getOriginalParts().subList(0, originalSize - prefixSize));
        }
        parts.addAll(rewritePrefix.getOriginalParts());
        parts.set(parts.size() - 1, createTemporaryTableName());
        QualifiedName temporaryTableName = QualifiedName.of(parts);
        Statement rewritten = new CreateTableAsSelect(temporaryTableName, statement.getQuery(), statement.isNotExists(), statement.getProperties(), statement.isWithData());
        String createTableAsSql = formatSql(rewritten);
        String checksumSql = checksumSql(getColumns(connection, statement), temporaryTableName);
        String dropTableSql = dropTableSql(temporaryTableName);
        return new Query(query.getCatalog(), query.getSchema(), ImmutableList.of(createTableAsSql), checksumSql, ImmutableList.of(dropTableSql), query.getUsername(), query.getPassword(), query.getSessionProperties());
    }

    private void trySetConnectionProperties(Query query, Connection connection)
            throws SQLException
    {
        // Required for jdbc drivers that do not implement all/some of these functions (eg. impala jdbc driver)
        // For these drivers, set the database default values in the query database
        try {
            connection.setClientInfo("ApplicationName", "verifier-rewrite");
            connection.setCatalog(catalogOverride.orElse(query.getCatalog()));
            connection.setSchema(schemaOverride.orElse(query.getSchema()));
        }
        catch (SQLClientInfoException ignored) {
            // Do nothing
        }
    }

    private String createTemporaryTableName()
    {
        return rewritePrefix.getSuffix() + UUID.randomUUID().toString().replace("-", "");
    }

    private List<Column> getColumns(Connection connection, CreateTableAsSelect createTableAsSelect)
            throws SQLException
    {
        com.facebook.presto.sql.tree.Query createSelectClause = createTableAsSelect.getQuery();

        // Rewrite the query to select zero rows, so that we can get the column names and types
        QueryBody innerQuery = createSelectClause.getQueryBody();
        com.facebook.presto.sql.tree.Query zeroRowsQuery;
        if (innerQuery instanceof QuerySpecification) {
            QuerySpecification querySpecification = (QuerySpecification) innerQuery;
            innerQuery = new QuerySpecification(
                    querySpecification.getSelect(),
                    querySpecification.getFrom(),
                    querySpecification.getWhere(),
                    querySpecification.getGroupBy(),
                    querySpecification.getHaving(),
                    querySpecification.getOrderBy(),
                    Optional.of("0"));

            zeroRowsQuery = new com.facebook.presto.sql.tree.Query(createSelectClause.getWith(), innerQuery, ImmutableList.of(), Optional.empty(), createSelectClause.getApproximate());
        }
        else {
            zeroRowsQuery = new com.facebook.presto.sql.tree.Query(createSelectClause.getWith(), innerQuery, ImmutableList.of(), Optional.of("0"), createSelectClause.getApproximate());
        }

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        try (java.sql.Statement jdbcStatement = connection.createStatement()) {
            TimeLimiter limiter = new SimpleTimeLimiter();
            java.sql.Statement limitedStatement = limiter.newProxy(jdbcStatement, java.sql.Statement.class, timeout.toMillis(), TimeUnit.MILLISECONDS);
            try (ResultSet resultSet = limitedStatement.executeQuery(formatSql(zeroRowsQuery))) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String name = metaData.getColumnName(i);
                    int type = metaData.getColumnType(i);
                    columns.add(new Column(name, APPROXIMATE_TYPES.contains(type)));
                }
            }
        }

        return columns.build();
    }

    private String checksumSql(List<Column> columns, QualifiedName table)
            throws SQLException
    {
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builder();
        for (Column column : columns) {
            Expression expression = new QualifiedNameReference(QualifiedName.of(column.getName()));
            if (column.isApproximateType()) {
                expression = new FunctionCall(QualifiedName.of("round"), ImmutableList.of(expression, new LongLiteral(Integer.toString(doublePrecision))));
            }
            selectItems.add(new SingleColumn(new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(expression))));
        }

        Select select = new Select(false, selectItems.build());
        return formatSql(new QuerySpecification(select, Optional.of(new Table(table)), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of(), Optional.empty()));
    }

    private static String dropTableSql(QualifiedName table)
    {
        return formatSql(new DropTable(table, true));
    }

    public static class QueryRewriteException
            extends Exception
    {
        public QueryRewriteException(String messageFormat, Object...args)
        {
            super(format(messageFormat, args));
        }
    }

    private static class Column
    {
        private final String name;
        private final boolean approximateType;

        private Column(String name, boolean approximateType)
        {
            this.name = name;
            this.approximateType = approximateType;
        }

        public String getName()
        {
            return name;
        }

        public boolean isApproximateType()
        {
            return approximateType;
        }
    }
}
