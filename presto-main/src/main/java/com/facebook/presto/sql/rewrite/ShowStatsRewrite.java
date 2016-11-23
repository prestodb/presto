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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DomainTranslator;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.Values;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;

import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.sql.QueryUtil.aliased;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.rewrite.ShowColumnStatsRewriteResultBuilder.buildColumnsNames;
import static com.facebook.presto.sql.rewrite.ShowColumnStatsRewriteResultBuilder.buildSelectItems;
import static com.facebook.presto.sql.rewrite.ShowColumnStatsRewriteResultBuilder.buildStatisticsRows;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ShowStatsRewrite
        implements StatementRewrite.Rewrite
{
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
            checkArgument(node.getQuery().getQueryBody() instanceof QuerySpecification);
            checkState(queryExplainer.isPresent(), "Query explainer must be provided for SHOW STATS SELECT");
            QuerySpecification specification = (QuerySpecification) node.getQuery().getQueryBody();

            checkArgument(specification.getFrom().isPresent());
            checkArgument(specification.getFrom().get() instanceof Table);
            Table table = (Table) specification.getFrom().get();
            TableHandle tableHandle = getTableHandle(node, table.getName());
            Constraint<ColumnHandle> constraint = getConstraint(node, tableHandle);

            TableStatistics tableStatistics = metadata.getTableStatistics(session, tableHandle, constraint);

            List<String> statisticsNames = findUniqueStatisticsNames(tableStatistics);

            List<String> resultColumnNames = buildColumnsNames(statisticsNames);
            SelectItem[] selectItems = buildSelectItems(resultColumnNames);

            Map<ColumnHandle, String> columnNames = getStatisticsColumnNames(tableStatistics, node, table.getName());
            List<Expression> resultRows = buildStatisticsRows(tableStatistics, columnNames, statisticsNames);

            return simpleQuery(selectList(selectItems),
                    aliased(new Values(resultRows),
                            "table_stats_for_" + table.getName(),
                            resultColumnNames));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        private Constraint<ColumnHandle> getConstraint(ShowStats node, TableHandle tableHandle)
        {
            checkArgument(node.getQuery().getQueryBody() instanceof QuerySpecification);
            QuerySpecification specification = (QuerySpecification) node.getQuery().getQueryBody();

            if (specification.getWhere().isPresent()) {
                return getConstraintWithWhere(node);
            }
            return Constraint.alwaysTrue();
        }

        private Constraint<ColumnHandle> getConstraintWithWhere(ShowStats node)
        {
            checkArgument(node.getQuery().getQueryBody() instanceof QuerySpecification);
            QuerySpecification specification = (QuerySpecification) node.getQuery().getQueryBody();

            Plan plan = queryExplainer.get().getLogicalPlan(session, new Query(Optional.empty(), specification, Optional.empty(), Optional.empty()), parameters);
            Map<Symbol, Type> types = plan.getTypes();

            Optional<TableScanNode> scanNode = findTableScanNode(plan);
            if (!scanNode.isPresent()) {
                return new Constraint<>(TupleDomain.none(), bindings -> true);
            }

            Expression filterNodePredicate = findFilterExpression(plan);

            return getConstraint(scanNode.get(), filterNodePredicate, types);
        }

        private Constraint<ColumnHandle> getConstraint(TableScanNode node, Expression predicate, Map<Symbol, Type> types)
        {
            DomainTranslator.ExtractionResult decomposedPredicate = DomainTranslator.fromPredicate(
                    metadata,
                    session,
                    predicate,
                    types);

            TupleDomain<ColumnHandle> simplifiedConstraint = decomposedPredicate.getTupleDomain()
                    .transform(node.getAssignments()::get)
                    .intersect(node.getCurrentConstraint());

            return new Constraint<>(simplifiedConstraint, bindings -> true);
        }

        private Map<ColumnHandle, String> getStatisticsColumnNames(TableStatistics statistics, ShowStats node, QualifiedName tableName)
        {
            ImmutableMap.Builder<ColumnHandle, String> resultBuilder = ImmutableMap.builder();
            TableHandle tableHandle = getTableHandle(node, tableName);

            for (ColumnHandle column : statistics.getColumnStatistics().keySet()) {
                resultBuilder.put(column, metadata.getColumnMetadata(session, tableHandle, column).getName());
            }

            return resultBuilder.build();
        }

        private TableHandle getTableHandle(ShowStats node, QualifiedName table)
        {
            QualifiedObjectName qualifiedTableName = createQualifiedObjectName(session, node, table);
            return metadata.getTableHandle(session, qualifiedTableName)
                    .orElseThrow(() -> new SemanticException(MISSING_TABLE, node, "Table %s not found", table));
        }

        private static List<String> findUniqueStatisticsNames(TableStatistics tableStatistics)
        {
            TreeSet<String> statisticsKeys = new TreeSet<>();
            statisticsKeys.addAll(tableStatistics.getTableStatistics().keySet());
            for (ColumnStatistics columnStats : tableStatistics.getColumnStatistics().values()) {
                statisticsKeys.addAll(columnStats.getStatistics().keySet());
            }
            return unmodifiableList(new ArrayList(statisticsKeys));
        }

        private static Optional<TableScanNode> findTableScanNode(Plan plan)
        {
            TableScanNode scanNode = searchFrom(plan.getRoot()).where(TableScanNode.class::isInstance).findOnlyElement(null);
            return Optional.ofNullable(scanNode);
        }

        private static Expression findFilterExpression(Plan plan)
        {
            FilterNode filterNode = searchFrom(plan.getRoot()).where(FilterNode.class::isInstance).findOnlyElement(null);
            return Optional.ofNullable(filterNode).map(FilterNode::getPredicate).orElse(TRUE_LITERAL);
        }
    }
}
