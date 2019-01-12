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
package io.prestosql.sql.rewrite;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.QueryUtil;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.AllColumns;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SelectItem;
import io.prestosql.sql.tree.ShowStats;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.TableSubquery;
import io.prestosql.sql.tree.Values;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.sql.QueryUtil.aliased;
import static io.prestosql.sql.QueryUtil.selectAll;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static io.prestosql.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.lang.Math.round;
import static java.util.Objects.requireNonNull;

public class ShowStatsRewrite
        implements StatementRewrite.Rewrite
{
    private static final Expression NULL_DOUBLE = new Cast(new NullLiteral(), DOUBLE);
    private static final Expression NULL_VARCHAR = new Cast(new NullLiteral(), VARCHAR);

    @Override
    public Statement rewrite(Session session, Metadata metadata, SqlParser parser, Optional<QueryExplainer> queryExplainer, Statement node, List<Expression> parameters, AccessControl accessControl, WarningCollector warningCollector)
    {
        return (Statement) new Visitor(metadata, session, parameters, queryExplainer, warningCollector).process(node, null);
    }

    private static class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final List<Expression> parameters;
        private final Optional<QueryExplainer> queryExplainer;
        private final WarningCollector warningCollector;

        public Visitor(Metadata metadata, Session session, List<Expression> parameters, Optional<QueryExplainer> queryExplainer, WarningCollector warningCollector)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.parameters = requireNonNull(parameters, "parameters is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        protected Node visitShowStats(ShowStats node, Void context)
        {
            checkState(queryExplainer.isPresent(), "Query explainer must be provided for SHOW STATS SELECT");

            if (node.getRelation() instanceof TableSubquery) {
                Query query = ((TableSubquery) node.getRelation()).getQuery();
                QuerySpecification specification = (QuerySpecification) query.getQueryBody();
                Plan plan = queryExplainer.get().getLogicalPlan(session, new Query(Optional.empty(), specification, Optional.empty(), Optional.empty()), parameters, warningCollector);
                validateShowStatsSubquery(node, query, specification, plan);
                Table table = (Table) specification.getFrom().get();
                Constraint<ColumnHandle> constraint = getConstraint(plan);
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

        private void validateShowStatsSubquery(ShowStats node, Query query, QuerySpecification querySpecification, Plan plan)
        {
            // The following properties of SELECT subquery are required:
            //  - only one relation in FROM
            //  - only predicates that can be pushed down can be in the where clause
            //  - no group by
            //  - no having
            //  - no set quantifier

            Optional<FilterNode> filterNode = searchFrom(plan.getRoot())
                    .where(FilterNode.class::isInstance)
                    .findSingle();

            check(!filterNode.isPresent(), node, "Only predicates that can be pushed down are supported in the SHOW STATS WHERE clause");
            check(querySpecification.getFrom().isPresent(), node, "There must be exactly one table in query passed to SHOW STATS SELECT clause");
            check(querySpecification.getFrom().get() instanceof Table, node, "There must be exactly one table in query passed to SHOW STATS SELECT clause");
            check(!query.getWith().isPresent(), node, "WITH is not supported by SHOW STATS SELECT clause");
            check(!querySpecification.getOrderBy().isPresent(), node, "ORDER BY is not supported in SHOW STATS SELECT clause");
            check(!querySpecification.getLimit().isPresent(), node, "LIMIT is not supported by SHOW STATS SELECT clause");
            check(!querySpecification.getHaving().isPresent(), node, "HAVING is not supported in SHOW STATS SELECT clause");
            check(!querySpecification.getGroupBy().isPresent(), node, "GROUP BY is not supported in SHOW STATS SELECT clause");
            check(!querySpecification.getSelect().isDistinct(), node, "DISTINCT is not supported by SHOW STATS SELECT clause");

            List<SelectItem> selectItems = querySpecification.getSelect().getSelectItems();
            check(selectItems.size() == 1 && selectItems.get(0) instanceof AllColumns, node, "Only SELECT * is supported in SHOW STATS SELECT clause");
        }

        private Node rewriteShowStats(ShowStats node, Table table, Constraint<ColumnHandle> constraint)
        {
            TableHandle tableHandle = getTableHandle(node, table.getName());
            TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, constraint);
            List<String> statsColumnNames = buildColumnsNames();
            List<SelectItem> selectItems = buildSelectItems(statsColumnNames);
            TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
            List<Expression> resultRows = buildStatisticsRows(tableMetadata, columnHandles, tableStatistics);

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

        private Constraint<ColumnHandle> getConstraint(Plan plan)
        {
            Optional<TableScanNode> scanNode = searchFrom(plan.getRoot())
                    .where(TableScanNode.class::isInstance)
                    .findSingle();

            if (!scanNode.isPresent()) {
                return Constraint.alwaysFalse();
            }

            return new Constraint<>(scanNode.get().getCurrentConstraint());
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

        private List<Expression> buildStatisticsRows(TableMetadata tableMetadata, Map<String, ColumnHandle> columnHandles, TableStatistics tableStatistics)
        {
            ImmutableList.Builder<Expression> rowsBuilder = ImmutableList.builder();
            for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                if (columnMetadata.isHidden()) {
                    continue;
                }
                String columnName = columnMetadata.getName();
                Type columnType = columnMetadata.getType();
                ColumnHandle columnHandle = columnHandles.get(columnName);
                ColumnStatistics columnStatistics = tableStatistics.getColumnStatistics().get(columnHandle);
                if (columnStatistics != null) {
                    rowsBuilder.add(createColumnStatsRow(columnName, columnType, columnStatistics));
                }
                else {
                    rowsBuilder.add(createEmptyColumnStatsRow(columnName));
                }
            }
            // Stats for whole table
            rowsBuilder.add(createTableStatsRow(tableStatistics));
            return rowsBuilder.build();
        }

        private Row createColumnStatsRow(String columnName, Type type, ColumnStatistics columnStatistics)
        {
            ImmutableList.Builder<Expression> rowValues = ImmutableList.builder();
            rowValues.add(new StringLiteral(columnName));
            rowValues.add(createEstimateRepresentation(columnStatistics.getDataSize()));
            rowValues.add(createEstimateRepresentation(columnStatistics.getDistinctValuesCount()));
            rowValues.add(createEstimateRepresentation(columnStatistics.getNullsFraction()));
            rowValues.add(NULL_DOUBLE);
            rowValues.add(toStringLiteral(type, columnStatistics.getRange().map(DoubleRange::getMin)));
            rowValues.add(toStringLiteral(type, columnStatistics.getRange().map(DoubleRange::getMax)));
            return new Row(rowValues.build());
        }

        private Expression createEmptyColumnStatsRow(String columnName)
        {
            ImmutableList.Builder<Expression> rowValues = ImmutableList.builder();
            rowValues.add(new StringLiteral(columnName));
            rowValues.add(NULL_DOUBLE);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(NULL_VARCHAR);
            rowValues.add(NULL_VARCHAR);
            return new Row(rowValues.build());
        }

        private static Row createTableStatsRow(TableStatistics tableStatistics)
        {
            ImmutableList.Builder<Expression> rowValues = ImmutableList.builder();
            rowValues.add(NULL_VARCHAR);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(NULL_DOUBLE);
            rowValues.add(createEstimateRepresentation(tableStatistics.getRowCount()));
            rowValues.add(NULL_VARCHAR);
            rowValues.add(NULL_VARCHAR);
            return new Row(rowValues.build());
        }

        private static Expression createEstimateRepresentation(Estimate estimate)
        {
            if (estimate.isUnknown()) {
                return NULL_DOUBLE;
            }
            return new DoubleLiteral(Double.toString(estimate.getValue()));
        }

        private static Expression toStringLiteral(Type type, Optional<Double> optionalValue)
        {
            return optionalValue.map(value -> toStringLiteral(type, value)).orElse(NULL_VARCHAR);
        }

        private static Expression toStringLiteral(Type type, double value)
        {
            if (type.equals(BigintType.BIGINT) || type.equals(IntegerType.INTEGER) || type.equals(SmallintType.SMALLINT) || type.equals(TinyintType.TINYINT)) {
                return new StringLiteral(Long.toString(round(value)));
            }
            if (type.equals(DoubleType.DOUBLE) || type instanceof DecimalType) {
                return new StringLiteral(Double.toString(value));
            }
            if (type.equals(RealType.REAL)) {
                return new StringLiteral(Float.toString((float) value));
            }
            if (type.equals(DATE)) {
                return new StringLiteral(LocalDate.ofEpochDay(round(value)).toString());
            }
            throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }
}
