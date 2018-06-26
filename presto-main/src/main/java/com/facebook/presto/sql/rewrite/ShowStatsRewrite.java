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
package com.facebook.presto.sql.rewrite;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.RangeColumnStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.FunctionInvoker;
import com.facebook.presto.sql.QueryUtil;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Values;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.sql.QueryUtil.aliased;
import static com.facebook.presto.sql.QueryUtil.selectAll;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class ShowStatsRewrite
        implements StatementRewrite.Rewrite
{
    private static final List<Class<? extends Expression>> ALLOWED_SHOW_STATS_WHERE_EXPRESSION_TYPES = ImmutableList.of(
            Literal.class, Identifier.class, ComparisonExpression.class, LogicalBinaryExpression.class, NotExpression.class, IsNullPredicate.class, IsNotNullPredicate.class);

    private static final Expression NULL_DOUBLE = new Cast(new NullLiteral(), DOUBLE);
    private static final Expression NULL_VARCHAR = new Cast(new NullLiteral(), VARCHAR);
    private static final int MAX_LOW_HIGH_LENGTH = 32;

    @Override
    public Statement rewrite(Session session, Metadata metadata, SqlParser parser, Optional<QueryExplainer> queryExplainer, Statement node, List<Expression> parameters, AccessControl accessControl)
    {
        return (Statement) new Visitor(metadata, session, parameters, queryExplainer).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final List<Expression> parameters;
        private final Optional<QueryExplainer> queryExplainer;

        public Visitor(Metadata metadata, Session session, List<Expression> parameters, Optional<QueryExplainer> queryExplainer)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.parameters = requireNonNull(parameters, "parameters is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        }

        @Override
        protected Node visitShowStats(ShowStats node, Void context)
        {
            validateShowStats(node);
            checkState(queryExplainer.isPresent(), "Query explainer must be provided for SHOW STATS SELECT");

            if (node.getRelation() instanceof TableSubquery) {
                QuerySpecification specification = (QuerySpecification) ((TableSubquery) node.getRelation()).getQuery().getQueryBody();
                Table table = (Table) specification.getFrom().get();
                Constraint<ColumnHandle> constraint = getConstraint(specification);
                return rewriteShowStats(node, table, constraint);
            }
            else if (node.getRelation() instanceof Table) {
                Table table = (Table) node.getRelation();
                return rewriteShowStats(node, table, Constraint.alwaysTrue());
            }
            else {
                throw new IllegalArgumentException("Expected either TableSubquery or Table as relation");
            }
        }

        private void validateShowStats(ShowStats node)
        {
            // The following properties of SELECT subquery are required:
            //  - only one relation in FROM
            //  - only plain columns in projection
            //  - only plain columns and constants in WHERE
            //  - no group by
            //  - no having
            //  - no set quantifier
            if (!(node.getRelation() instanceof Table || node.getRelation() instanceof TableSubquery)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Only table and simple table subquery can be passed as argument to SHOW STATS clause");
            }

            if (node.getRelation() instanceof TableSubquery) {
                Query query = ((TableSubquery) node.getRelation()).getQuery();
                check(query.getQueryBody() instanceof QuerySpecification, node, "Only table and simple table subquery can be passed as argument to SHOW STATS clause");
                QuerySpecification querySpecification = (QuerySpecification) query.getQueryBody();

                check(querySpecification.getFrom().isPresent(), node, "There must be exactly one table in query passed to SHOW STATS SELECT clause");
                check(querySpecification.getFrom().get() instanceof Table, node, "There must be exactly one table in query passed to SHOW STATS SELECT clause");
                check(!query.getWith().isPresent(), node, "WITH is not supported by SHOW STATS SELECT clause");
                check(!querySpecification.getOrderBy().isPresent(), node, "ORDER BY is not supported in SHOW STATS SELECT clause");
                check(!querySpecification.getLimit().isPresent(), node, "LIMIT is not supported by SHOW STATS SELECT clause");
                check(!querySpecification.getHaving().isPresent(), node, "HAVING is not supported in SHOW STATS SELECT clause");
                check(!querySpecification.getGroupBy().isPresent(), node, "GROUP BY is not supported in SHOW STATS SELECT clause");
                check(!querySpecification.getSelect().isDistinct(), node, "DISTINCT is not supported by SHOW STATS SELECT clause");
                for (SelectItem selectItem : querySpecification.getSelect().getSelectItems()) {
                    if (selectItem instanceof AllColumns) {
                        continue;
                    }
                    check(selectItem instanceof SingleColumn, node, "Only * and column references are supported by SHOW STATS SELECT clause");
                    SingleColumn columnSelect = (SingleColumn) selectItem;
                    check(columnSelect.getExpression() instanceof Identifier, node, "Only * and column references are supported by SHOW STATS SELECT clause");
                }

                querySpecification.getWhere().ifPresent((expression) -> validateShowStatsWhereExpression(expression, node));
            }
        }

        private void validateShowStatsWhereExpression(Expression expression, ShowStats node)
        {
            check(ALLOWED_SHOW_STATS_WHERE_EXPRESSION_TYPES.stream().anyMatch(clazz -> clazz.isInstance(expression)), node, "Only literals, column references, comparators, is (not) null and logical operators are allowed in WHERE of SHOW STATS SELECT clause");

            if (expression instanceof NotExpression) {
                validateShowStatsWhereExpression(((NotExpression) expression).getValue(), node);
            }
            else if (expression instanceof LogicalBinaryExpression) {
                validateShowStatsWhereExpression(((LogicalBinaryExpression) expression).getLeft(), node);
                validateShowStatsWhereExpression(((LogicalBinaryExpression) expression).getRight(), node);
            }
            else if (expression instanceof ComparisonExpression) {
                validateShowStatsWhereExpression(((ComparisonExpression) expression).getLeft(), node);
                validateShowStatsWhereExpression(((ComparisonExpression) expression).getRight(), node);
            }
            else if (expression instanceof IsNullPredicate) {
                validateShowStatsWhereExpression(((IsNullPredicate) expression).getValue(), node);
            }
            else if (expression instanceof IsNotNullPredicate) {
                validateShowStatsWhereExpression(((IsNotNullPredicate) expression).getValue(), node);
            }
        }

        private Node rewriteShowStats(ShowStats node, Table table, Constraint<ColumnHandle> constraint)
        {
            TableHandle tableHandle = getTableHandle(node, table.getName());
            TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, constraint);
            List<String> statsColumnNames = buildColumnsNames();
            List<SelectItem> selectItems = buildSelectItems(statsColumnNames);
            Map<ColumnHandle, String> tableColumnNames = getStatisticsColumnNames(tableStatistics, tableHandle);
            Map<ColumnHandle, Type> tableColumnTypes = getStatisticsColumnTypes(tableStatistics, tableHandle);
            List<Expression> resultRows = buildStatisticsRows(tableStatistics, tableColumnNames, tableColumnTypes);

            return simpleQuery(selectAll(selectItems),
                    aliased(new Values(resultRows),
                            "table_stats_for_" + table.getName(),
                            statsColumnNames));
        }

        private static void check(boolean condition, ShowStats node, String message)
        {
            if (!condition) {
                throw new SemanticException(NOT_SUPPORTED, node, message);
            }
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        private Constraint<ColumnHandle> getConstraint(QuerySpecification specification)
        {
            if (!specification.getWhere().isPresent()) {
                return Constraint.alwaysTrue();
            }

            Plan plan = queryExplainer.get().getLogicalPlan(session, new Query(Optional.empty(), specification, Optional.empty(), Optional.empty()), parameters);

            Optional<TableScanNode> scanNode = searchFrom(plan.getRoot())
                    .where(TableScanNode.class::isInstance)
                    .findSingle();

            if (!scanNode.isPresent()) {
                return new Constraint<>(TupleDomain.none(), bindings -> true);
            }

            return new Constraint<>(scanNode.get().getCurrentConstraint(), bindings -> true);
        }

        private Map<ColumnHandle, String> getStatisticsColumnNames(TableStatistics statistics, TableHandle tableHandle)
        {
            return statistics.getColumnStatistics()
                    .keySet().stream()
                    .collect(toMap(identity(), column -> metadata.getColumnMetadata(session, tableHandle, column).getName()));
        }

        private Map<ColumnHandle, Type> getStatisticsColumnTypes(TableStatistics statistics, TableHandle tableHandle)
        {
            return statistics.getColumnStatistics()
                    .keySet().stream()
                    .collect(toMap(identity(), column -> metadata.getColumnMetadata(session, tableHandle, column).getType()));
        }

        private TableHandle getTableHandle(ShowStats node, QualifiedName table)
        {
            QualifiedObjectName qualifiedTableName = createQualifiedObjectName(session, node, table);
            return metadata.getTableHandle(session, qualifiedTableName)
                    .orElseThrow(() -> new SemanticException(MISSING_TABLE, node, "Table %s not found", table));
        }

        private static List<String> buildColumnsNames()
        {
            return ImmutableList.<String>builder()
                    .add("column_name")
                    .add("data_size")
                    .add("distinct_values_count")
                    .add("nulls_fraction")
                    .add("row_count")
                    .add("low_value")
                    .add("high_value")
                    .build();
        }

        private static List<SelectItem> buildSelectItems(List<String> columnNames)
        {
            return columnNames.stream()
                    .map(QueryUtil::unaliasedName)
                    .collect(toImmutableList());
        }

        private List<Expression> buildStatisticsRows(TableStatistics tableStatistics, Map<ColumnHandle, String> columnNames, Map<ColumnHandle, Type> columnTypes)
        {
            ImmutableList.Builder<Expression> rowsBuilder = ImmutableList.builder();

            // Stats for columns
            for (Map.Entry<ColumnHandle, ColumnStatistics> columnStats : tableStatistics.getColumnStatistics().entrySet()) {
                ColumnHandle columnHandle = columnStats.getKey();
                rowsBuilder.add(createColumnStatsRow(columnNames.get(columnHandle), columnTypes.get(columnHandle), columnStats.getValue()));
            }

            // Stats for whole table
            rowsBuilder.add(createTableStatsRow(tableStatistics));

            return rowsBuilder.build();
        }

        private Row createColumnStatsRow(String columnName, Type type, ColumnStatistics columnStatistics)
        {
            RangeColumnStatistics onlyRangeColumnStatistics = columnStatistics.getOnlyRangeColumnStatistics();

            ImmutableList.Builder<Expression> rowValues = ImmutableList.builder();
            rowValues.add(new StringLiteral(columnName));
            rowValues.add(createStatisticValueOrNull(onlyRangeColumnStatistics.getDataSize()));
            rowValues.add(createStatisticValueOrNull(onlyRangeColumnStatistics.getDistinctValuesCount()));
            rowValues.add(createStatisticValueOrNull(columnStatistics.getNullsFraction()));
            rowValues.add(NULL_DOUBLE);
            rowValues.add(lowHighAsLiteral(type, onlyRangeColumnStatistics.getLowValue()));
            rowValues.add(lowHighAsLiteral(type, onlyRangeColumnStatistics.getHighValue()));
            return new Row(rowValues.build());
        }

        private static Row createTableStatsRow(TableStatistics tableStatistics)
        {
            ImmutableList.Builder<Expression> rowValues = ImmutableList.builder();
            rowValues.add(NULL_VARCHAR);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(createStatisticValueOrNull(tableStatistics.getRowCount()));
            rowValues.add(NULL_VARCHAR);
            rowValues.add(NULL_VARCHAR);
            return new Row(rowValues.build());
        }

        private Expression lowHighAsLiteral(Type valueType, Optional<Object> value)
        {
            if (!value.isPresent()) {
                return new Cast(new NullLiteral(), VARCHAR);
            }
            FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
            FunctionInvoker functionInvoker = new FunctionInvoker(functionRegistry);
            Signature castSignature = functionRegistry.getCoercion(valueType, VarcharType.createUnboundedVarcharType());
            Slice varcharValue = (Slice) functionInvoker.invoke(castSignature, session.toConnectorSession(), singletonList(value.get()));
            String stringValue = varcharValue.toStringUtf8();
            if (stringValue.length() > MAX_LOW_HIGH_LENGTH) {
                stringValue = stringValue.substring(0, MAX_LOW_HIGH_LENGTH) + "...";
            }
            return new StringLiteral(stringValue);
        }

        private static Expression createStatisticValueOrNull(Estimate estimate)
        {
            if (estimate.isValueUnknown()) {
                return NULL_DOUBLE;
            }
            return new DoubleLiteral(Double.toString(estimate.getValue()));
        }
    }
}
