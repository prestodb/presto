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
package com.facebook.presto.lance;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static java.lang.Float.intBitsToFloat;

/**
 * Recognizes a vector ANN query expressed as {@code ORDER BY <distance-fn>(col, q) ... LIMIT k}
 * sitting directly above a Lance table scan and rewrites it into an index search pushed down to
 * the dataset's pre-built vector index. The matched plan shape is
 * {@code TopN -> Project(score := fn(col, q)) -> TableScan(lance)} where {@code q} is a constant
 * vector and the sort direction is the "nearest" direction for the function's metric:
 *
 * <ul>
 *   <li>{@code cosine_similarity(col, q) DESC} -&gt; cosine</li>
 *   <li>{@code dot_product(col, q) DESC} -&gt; dot</li>
 *   <li>{@code l2_distance(col, q) ASC} -&gt; l2</li>
 * </ul>
 *
 * <p>The TopN and Project are kept so the engine still produces the exact global top-k (partial
 * TopN per worker + final TopN merge) and the score column. Only the table scan handle gains the
 * {@link LanceVectorSearch} parameters, which the split manager and page source consume.
 *
 * <p>Anything that does not match exactly (no index, non-constant vector, wrong sort direction,
 * a Filter between the TopN and the scan, multiple sort keys, or pushdown disabled) is left
 * unchanged and runs as a brute-force scan + TopN, which is always correct.
 */
public class LanceVectorSearchPushdown
        implements ConnectorPlanOptimizer
{
    @Override
    public PlanNode optimize(
            PlanNode maxSubplan,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        // session is null in unit tests; treat that as "enabled, no tuning overrides".
        if (session != null && !LanceSessionProperties.isVectorSearchPushdownEnabled(session)) {
            return maxSubplan;
        }
        Optional<Integer> nprobes = session == null ? Optional.empty() : LanceSessionProperties.getVectorSearchNprobes(session);
        Optional<Integer> ef = session == null ? Optional.empty() : LanceSessionProperties.getVectorSearchEf(session);
        Optional<Integer> refineFactor = session == null ? Optional.empty() : LanceSessionProperties.getVectorSearchRefineFactor(session);
        return rewriteWith(new Rewriter(nprobes, ef, refineFactor), maxSubplan);
    }

    private static final class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final Optional<Integer> nprobes;
        private final Optional<Integer> ef;
        private final Optional<Integer> refineFactor;

        private Rewriter(Optional<Integer> nprobes, Optional<Integer> ef, Optional<Integer> refineFactor)
        {
            this.nprobes = nprobes;
            this.ef = ef;
            this.refineFactor = refineFactor;
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Void> context)
        {
            // Match TopN -> Project -> TableScan(lance). A Filter between the Project and the scan
            // (a WHERE clause not foldable into the layout) means we do not push down, keeping
            // filtered results exact via the brute-force fallback.
            if (!(node.getSource() instanceof ProjectNode)) {
                return visitPlan(node, context);
            }
            ProjectNode project = (ProjectNode) node.getSource();
            if (!(project.getSource() instanceof TableScanNode)) {
                return visitPlan(node, context);
            }
            TableScanNode tableScan = (TableScanNode) project.getSource();

            TableHandle handle = tableScan.getTable();
            if (!(handle.getConnectorHandle() instanceof LanceTableHandle)) {
                return visitPlan(node, context);
            }
            LanceTableHandle lanceTable = (LanceTableHandle) handle.getConnectorHandle();
            if (lanceTable.hasVectorSearch()) {
                return node;
            }

            OrderingScheme orderingScheme = node.getOrderingScheme();
            if (orderingScheme.getOrderBy().size() != 1) {
                return visitPlan(node, context);
            }
            Ordering ordering = orderingScheme.getOrderBy().get(0);
            RowExpression sortExpression = project.getAssignments().getMap().get(ordering.getVariable());
            if (sortExpression == null) {
                return visitPlan(node, context);
            }

            long count = node.getCount();
            if (count <= 0 || count > Integer.MAX_VALUE) {
                return visitPlan(node, context);
            }

            Optional<LanceVectorSearch> vectorSearch = tryBuildVectorSearch(
                    sortExpression,
                    ordering.getSortOrder().isAscending(),
                    (int) count,
                    tableScan);
            if (!vectorSearch.isPresent()) {
                return visitPlan(node, context);
            }

            LanceTableHandle newLanceTable = lanceTable.withVectorSearch(vectorSearch.get());
            Optional<ConnectorTableLayoutHandle> newLayout = handle.getLayout()
                    .map(LanceTableLayoutHandle.class::cast)
                    .map(layout -> new LanceTableLayoutHandle(newLanceTable, layout.getTupleDomain()));
            TableHandle newHandle = new TableHandle(
                    handle.getConnectorId(),
                    newLanceTable,
                    handle.getTransaction(),
                    newLayout);
            TableScanNode newTableScan = new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    newHandle,
                    tableScan.getOutputVariables(),
                    tableScan.getAssignments(),
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint(),
                    tableScan.getCteMaterializationInfo());

            // Keep Project and TopN: the engine still computes the score column and performs the
            // exact global top-k merge over the per-segment candidates returned by the scan.
            ProjectNode newProject = (ProjectNode) project.replaceChildren(ImmutableList.of(newTableScan));
            return node.replaceChildren(ImmutableList.of(newProject));
        }

        private Optional<LanceVectorSearch> tryBuildVectorSearch(
                RowExpression sortExpression,
                boolean ascending,
                int k,
                TableScanNode tableScan)
        {
            if (!(sortExpression instanceof CallExpression)) {
                return Optional.empty();
            }
            CallExpression call = (CallExpression) sortExpression;
            String function = baseName(call.getDisplayName());

            String metric;
            boolean nearestIsAscending;
            switch (function) {
                case "cosine_similarity":
                    metric = LanceVectorSearch.METRIC_COSINE;
                    nearestIsAscending = false;
                    break;
                case "dot_product":
                    metric = LanceVectorSearch.METRIC_DOT;
                    nearestIsAscending = false;
                    break;
                case "l2_distance":
                    metric = LanceVectorSearch.METRIC_L2;
                    nearestIsAscending = true;
                    break;
                default:
                    return Optional.empty();
            }
            if (ascending != nearestIsAscending) {
                return Optional.empty();
            }

            List<RowExpression> arguments = call.getArguments();
            if (arguments.size() != 2) {
                return Optional.empty();
            }

            Optional<String> column = Optional.empty();
            Optional<List<Double>> queryVector = Optional.empty();
            for (RowExpression argument : arguments) {
                RowExpression unwrapped = unwrapCast(argument);
                Optional<String> resolvedColumn = resolveColumn(unwrapped, tableScan);
                if (resolvedColumn.isPresent()) {
                    column = resolvedColumn;
                    continue;
                }
                Optional<List<Double>> resolvedVector = resolveConstantVector(unwrapped);
                if (resolvedVector.isPresent()) {
                    queryVector = resolvedVector;
                }
            }
            if (!column.isPresent() || !queryVector.isPresent()) {
                return Optional.empty();
            }

            return Optional.of(new LanceVectorSearch(column.get(), queryVector.get(), k, metric, nprobes, ef, refineFactor));
        }

        private static Optional<String> resolveColumn(RowExpression expression, TableScanNode tableScan)
        {
            if (!(expression instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }
            Object columnHandle = tableScan.getAssignments().get(expression);
            if (columnHandle instanceof LanceColumnHandle) {
                return Optional.of(((LanceColumnHandle) columnHandle).getColumnName());
            }
            return Optional.empty();
        }

        private static Optional<List<Double>> resolveConstantVector(RowExpression expression)
        {
            if (!(expression instanceof ConstantExpression)) {
                return Optional.empty();
            }
            ConstantExpression constant = (ConstantExpression) expression;
            if (!(constant.getType() instanceof ArrayType)) {
                return Optional.empty();
            }
            Type elementType = ((ArrayType) constant.getType()).getElementType();
            Object value = constant.getValue();
            if (!(value instanceof Block)) {
                return Optional.empty();
            }
            Block block = (Block) value;
            ImmutableList.Builder<Double> vector = ImmutableList.builder();
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    return Optional.empty();
                }
                if (elementType.equals(DOUBLE)) {
                    vector.add(DOUBLE.getDouble(block, i));
                }
                else if (elementType.equals(REAL)) {
                    vector.add((double) intBitsToFloat(block.getInt(i)));
                }
                else {
                    return Optional.empty();
                }
            }
            List<Double> result = vector.build();
            return result.isEmpty() ? Optional.empty() : Optional.of(result);
        }

        private static RowExpression unwrapCast(RowExpression expression)
        {
            RowExpression current = expression;
            while (current instanceof CallExpression) {
                CallExpression call = (CallExpression) current;
                if (!"cast".equals(baseName(call.getDisplayName())) || call.getArguments().size() != 1) {
                    break;
                }
                current = call.getArguments().get(0);
            }
            return current;
        }

        private static String baseName(String displayName)
        {
            String name = displayName.toLowerCase(Locale.ENGLISH);
            int lastDot = name.lastIndexOf('.');
            return lastDot >= 0 ? name.substring(lastDot + 1) : name;
        }
    }
}
