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
package io.prestosql.verifier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.units.Duration;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.DropTable;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.Insert;
import io.prestosql.sql.tree.LikeClause;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.QueryBody;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Select;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.Table;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static io.prestosql.sql.SqlFormatter.formatSql;
import static io.prestosql.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static io.prestosql.sql.tree.LikeClause.PropertiesOption.INCLUDING;
import static io.prestosql.verifier.QueryType.READ;
import static io.prestosql.verifier.VerifyCommand.statementToQueryType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

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

        Statement statement = parser.createStatement(query.getQuery(), new ParsingOptions(AS_DOUBLE /* anything */));
        try (Connection connection = DriverManager.getConnection(gatewayUrl, usernameOverride.orElse(query.getUsername()), passwordOverride.orElse(query.getPassword()))) {
            trySetConnectionProperties(query, connection);
            if (statement instanceof CreateTableAsSelect) {
                return rewriteCreateTableAsSelect(connection, query, (CreateTableAsSelect) statement);
            }
            else if (statement instanceof Insert) {
                return rewriteInsertQuery(connection, query, (Insert) statement);
            }
        }

        throw new QueryRewriteException("Unsupported query type: " + statement.getClass());
    }

    private Query rewriteCreateTableAsSelect(Connection connection, Query query, CreateTableAsSelect statement)
            throws SQLException, QueryRewriteException
    {
        QualifiedName temporaryTableName = generateTemporaryTableName(statement.getName());
        Statement rewritten = new CreateTableAsSelect(temporaryTableName, statement.getQuery(), statement.isNotExists(), statement.getProperties(), statement.isWithData(), statement.getColumnAliases(), Optional.empty());
        String createTableAsSql = formatSql(rewritten, Optional.empty());
        String checksumSql = checksumSql(getColumns(connection, statement), temporaryTableName);
        String dropTableSql = dropTableSql(temporaryTableName);
        return new Query(query.getCatalog(), query.getSchema(), ImmutableList.of(createTableAsSql), checksumSql, ImmutableList.of(dropTableSql), query.getUsername(), query.getPassword(), query.getSessionProperties());
    }

    private Query rewriteInsertQuery(Connection connection, Query query, Insert statement)
            throws SQLException, QueryRewriteException
    {
        QualifiedName temporaryTableName = generateTemporaryTableName(statement.getTarget());
        Statement createTemporaryTable = new CreateTable(temporaryTableName, ImmutableList.of(new LikeClause(statement.getTarget(), Optional.of(INCLUDING))), true, ImmutableList.of(), Optional.empty());
        String createTemporaryTableSql = formatSql(createTemporaryTable, Optional.empty());
        String insertSql = formatSql(new Insert(temporaryTableName, statement.getColumns(), statement.getQuery()), Optional.empty());
        String checksumSql = checksumSql(getColumnsForTable(connection, query.getCatalog(), query.getSchema(), statement.getTarget().toString()), temporaryTableName);
        String dropTableSql = dropTableSql(temporaryTableName);
        return new Query(query.getCatalog(), query.getSchema(), ImmutableList.of(createTemporaryTableSql, insertSql), checksumSql, ImmutableList.of(dropTableSql), query.getUsername(), query.getPassword(), query.getSessionProperties());
    }

    private QualifiedName generateTemporaryTableName(QualifiedName originalName)
    {
        List<String> parts = new ArrayList<>();
        int originalSize = originalName.getOriginalParts().size();
        int prefixSize = rewritePrefix.getOriginalParts().size();
        if (originalSize > prefixSize) {
            parts.addAll(originalName.getOriginalParts().subList(0, originalSize - prefixSize));
        }
        parts.addAll(rewritePrefix.getOriginalParts());
        parts.set(parts.size() - 1, createTemporaryTableName());
        return QualifiedName.of(parts);
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

    private List<Column> getColumnsForTable(Connection connection, String catalog, String schema, String table)
            throws SQLException
    {
        ResultSet columns = connection.getMetaData().getColumns(catalog, escapeLikeExpression(connection, schema), escapeLikeExpression(connection, table), null);
        ImmutableList.Builder<Column> columnBuilder = new ImmutableList.Builder<>();
        while (columns.next()) {
            String name = columns.getString("COLUMN_NAME");
            int type = columns.getInt("DATA_TYPE");
            columnBuilder.add(new Column(name, APPROXIMATE_TYPES.contains(type)));
        }
        return columnBuilder.build();
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

            zeroRowsQuery = new com.facebook.presto.sql.tree.Query(createSelectClause.getWith(), innerQuery, Optional.empty(), Optional.empty());
        }
        else {
            zeroRowsQuery = new com.facebook.presto.sql.tree.Query(createSelectClause.getWith(), innerQuery, Optional.empty(), Optional.of("0"));
        }

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        try (java.sql.Statement jdbcStatement = connection.createStatement()) {
            ExecutorService executor = newSingleThreadExecutor();
            TimeLimiter limiter = SimpleTimeLimiter.create(executor);
            java.sql.Statement limitedStatement = limiter.newProxy(jdbcStatement, java.sql.Statement.class, timeout.toMillis(), TimeUnit.MILLISECONDS);
            try (ResultSet resultSet = limitedStatement.executeQuery(formatSql(zeroRowsQuery, Optional.empty()))) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String name = metaData.getColumnName(i);
                    int type = metaData.getColumnType(i);
                    columns.add(new Column(name, APPROXIMATE_TYPES.contains(type)));
                }
            }
            catch (UncheckedTimeoutException e) {
                throw new SQLException("SQL statement execution timed out", e);
            }
            finally {
                executor.shutdownNow();
            }
        }

        return columns.build();
    }

    private String checksumSql(List<Column> columns, QualifiedName table)
            throws QueryRewriteException
    {
        if (columns.isEmpty()) {
            throw new QueryRewriteException("Table " + table + " has no columns");
        }
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builder();
        for (Column column : columns) {
            Expression expression = new Identifier(column.getName());
            if (column.isApproximateType()) {
                expression = new FunctionCall(QualifiedName.of("round"), ImmutableList.of(expression, new LongLiteral(Integer.toString(doublePrecision))));
            }
            selectItems.add(new SingleColumn(new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(expression))));
        }

        Select select = new Select(false, selectItems.build());
        return formatSql(new QuerySpecification(select, Optional.of(new Table(table)), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()), Optional.empty());
    }

    private static String dropTableSql(QualifiedName table)
    {
        return formatSql(new DropTable(table, true), Optional.empty());
    }

    private static String escapeLikeExpression(Connection connection, String value)
            throws SQLException
    {
        String escapeString = connection.getMetaData().getSearchStringEscape();
        return value.replace(escapeString, escapeString + escapeString).replace("_", escapeString + "_").replace("%", escapeString + "%");
    }

    public static class QueryRewriteException
            extends Exception
    {
        public QueryRewriteException(String messageFormat, Object... args)
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
