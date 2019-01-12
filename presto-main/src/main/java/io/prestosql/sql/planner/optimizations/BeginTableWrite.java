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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.metadata.TableLayoutResult;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.UnionNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.prestosql.metadata.TableLayoutResult.computeEnforced;
import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static java.util.stream.Collectors.toSet;

/*
 * Major HACK alert!!!
 *
 * This logic should be invoked on query start, not during planning. At that point, the token
 * returned by beginCreate/beginInsert should be handed down to tasks in a mapping separate
 * from the plan that links plan nodes to the corresponding token.
 */
public class BeginTableWrite
        implements PlanOptimizer
{
    private final Metadata metadata;

    public BeginTableWrite(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(session), plan, new Context());
    }

    private class Rewriter
            extends SimplePlanRewriter<Context>
    {
        private final Session session;

        public Rewriter(Session session)
        {
            this.session = session;
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Context> context)
        {
            // Part of the plan should be an Optional<StateChangeListener<QueryState>> and this
            // callback can create the table and abort the table creation if the query fails.

            TableWriterNode.WriterTarget writerTarget = context.get().getMaterializedHandle(node.getTarget()).get();
            return new TableWriterNode(
                    node.getId(),
                    node.getSource().accept(this, context),
                    writerTarget,
                    node.getRowCountSymbol(),
                    node.getFragmentSymbol(),
                    node.getColumns(),
                    node.getColumnNames(),
                    node.getPartitioningScheme(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Context> context)
        {
            TableWriterNode.DeleteHandle deleteHandle = (TableWriterNode.DeleteHandle) context.get().getMaterializedHandle(node.getTarget()).get();
            return new DeleteNode(
                    node.getId(),
                    rewriteDeleteTableScan(node.getSource(), deleteHandle.getHandle()),
                    deleteHandle,
                    node.getRowId(),
                    node.getOutputSymbols());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<Context> context)
        {
            PlanNode child = node.getSource();

            TableWriterNode.WriterTarget originalTarget = getTarget(child);
            TableWriterNode.WriterTarget newTarget = createWriterTarget(originalTarget);

            context.get().addMaterializedHandle(originalTarget, newTarget);
            child = child.accept(this, context);

            return new TableFinishNode(
                    node.getId(),
                    child,
                    newTarget,
                    node.getRowCountSymbol(),
                    node.getStatisticsAggregation(),
                    node.getStatisticsAggregationDescriptor());
        }

        public TableWriterNode.WriterTarget getTarget(PlanNode node)
        {
            if (node instanceof TableWriterNode) {
                return ((TableWriterNode) node).getTarget();
            }
            if (node instanceof DeleteNode) {
                return ((DeleteNode) node).getTarget();
            }
            if (node instanceof ExchangeNode || node instanceof UnionNode) {
                Set<TableWriterNode.WriterTarget> writerTargets = node.getSources().stream()
                        .map(this::getTarget)
                        .collect(toSet());
                return Iterables.getOnlyElement(writerTargets);
            }
            throw new IllegalArgumentException("Invalid child for TableCommitNode: " + node.getClass().getSimpleName());
        }

        private TableWriterNode.WriterTarget createWriterTarget(TableWriterNode.WriterTarget target)
        {
            // TODO: begin these operations in pre-execution step, not here
            // TODO: we shouldn't need to store the schemaTableName in the handles, but there isn't a good way to pass this around with the current architecture
            if (target instanceof TableWriterNode.CreateName) {
                TableWriterNode.CreateName create = (TableWriterNode.CreateName) target;
                return new TableWriterNode.CreateHandle(metadata.beginCreateTable(session, create.getCatalog(), create.getTableMetadata(), create.getLayout()), create.getTableMetadata().getTable());
            }
            if (target instanceof TableWriterNode.InsertReference) {
                TableWriterNode.InsertReference insert = (TableWriterNode.InsertReference) target;
                return new TableWriterNode.InsertHandle(metadata.beginInsert(session, insert.getHandle()), metadata.getTableMetadata(session, insert.getHandle()).getTable());
            }
            if (target instanceof TableWriterNode.DeleteHandle) {
                TableWriterNode.DeleteHandle delete = (TableWriterNode.DeleteHandle) target;
                return new TableWriterNode.DeleteHandle(metadata.beginDelete(session, delete.getHandle()), delete.getSchemaTableName());
            }
            throw new IllegalArgumentException("Unhandled target type: " + target.getClass().getSimpleName());
        }

        private PlanNode rewriteDeleteTableScan(PlanNode node, TableHandle handle)
        {
            if (node instanceof TableScanNode) {
                TableScanNode scan = (TableScanNode) node;
                TupleDomain<ColumnHandle> originalEnforcedConstraint = scan.getEnforcedConstraint();

                List<TableLayoutResult> layouts = metadata.getLayouts(
                        session,
                        handle,
                        new Constraint<>(originalEnforcedConstraint),
                        Optional.of(ImmutableSet.copyOf(scan.getAssignments().values())));
                verify(layouts.size() == 1, "Expected exactly one layout for delete");
                TableLayoutResult layoutResult = Iterables.getOnlyElement(layouts);

                return new TableScanNode(
                        scan.getId(),
                        handle,
                        scan.getOutputSymbols(),
                        scan.getAssignments(),
                        Optional.of(layoutResult.getLayout().getHandle()),
                        layoutResult.getLayout().getPredicate(),
                        computeEnforced(originalEnforcedConstraint, layoutResult.getUnenforcedConstraint()));
            }

            if (node instanceof FilterNode) {
                PlanNode source = rewriteDeleteTableScan(((FilterNode) node).getSource(), handle);
                return replaceChildren(node, ImmutableList.of(source));
            }
            if (node instanceof ProjectNode) {
                PlanNode source = rewriteDeleteTableScan(((ProjectNode) node).getSource(), handle);
                return replaceChildren(node, ImmutableList.of(source));
            }
            if (node instanceof SemiJoinNode) {
                PlanNode source = rewriteDeleteTableScan(((SemiJoinNode) node).getSource(), handle);
                return replaceChildren(node, ImmutableList.of(source, ((SemiJoinNode) node).getFilteringSource()));
            }
            if (node instanceof JoinNode) {
                JoinNode joinNode = (JoinNode) node;
                if (joinNode.getType() == JoinNode.Type.INNER && isAtMostScalar(joinNode.getRight())) {
                    PlanNode source = rewriteDeleteTableScan(joinNode.getLeft(), handle);
                    return replaceChildren(node, ImmutableList.of(source, joinNode.getRight()));
                }
            }
            throw new IllegalArgumentException("Invalid descendant for DeleteNode: " + node.getClass().getName());
        }
    }

    public static class Context
    {
        private Optional<TableWriterNode.WriterTarget> handle = Optional.empty();
        private Optional<TableWriterNode.WriterTarget> materializedHandle = Optional.empty();

        public void addMaterializedHandle(TableWriterNode.WriterTarget handle, TableWriterNode.WriterTarget materializedHandle)
        {
            checkState(!this.handle.isPresent(), "can only have one WriterTarget in a subtree");
            this.handle = Optional.of(handle);
            this.materializedHandle = Optional.of(materializedHandle);
        }

        public Optional<TableWriterNode.WriterTarget> getMaterializedHandle(TableWriterNode.WriterTarget handle)
        {
            checkState(this.handle.get().equals(handle), "can't find materialized handle for WriterTarget");
            return materializedHandle;
        }
    }
}
