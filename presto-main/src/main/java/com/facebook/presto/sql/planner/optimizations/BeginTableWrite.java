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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode.DeleteHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;

/*
 * Major HACK alert!!!
 *
 * This logic should be invoked on query start, not during planning. At that point, the token
 * returned by beginCreate/beginInsert should be handed down to tasks in a mapping separate
 * from the plan that links plan nodes to the corresponding token.
 */
public class BeginTableWrite
        extends PlanOptimizer
{
    private final Metadata metadata;

    public BeginTableWrite(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return PlanRewriter.rewriteWith(new Rewriter(session), plan);
    }

    private class Rewriter
            extends PlanRewriter<Void>
    {
        private final Session session;

        public Rewriter(Session session)
        {
            this.session = session;
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Void> context)
        {
            // TODO: begin create table or insert in pre-execution step, not here
            // Part of the plan should be an Optional<StateChangeListener<QueryState>> and this
            // callback can create the table and abort the table creation if the query fails.
            return new TableWriterNode(
                    node.getId(),
                    node.getSource().accept(this, context),
                    createWriterTarget(node.getTarget()),
                    node.getColumns(),
                    node.getColumnNames(),
                    node.getOutputSymbols(),
                    node.getSampleWeightSymbol());
        }

        @Override
        public PlanNode visitDelete(DeleteNode node, RewriteContext<Void> context)
        {
            // TODO: begin delete in pre-execution step, not here
            TableHandle handle = metadata.beginDelete(session, node.getTarget().getHandle());
            return new DeleteNode(
                    node.getId(),
                    rewriteDeleteTableScan(node.getSource(), handle, context),
                    new DeleteHandle(handle),
                    node.getRowId(),
                    node.getOutputSymbols());
        }

        @Override
        public PlanNode visitTableCommit(TableCommitNode node, RewriteContext<Void> context)
        {
            PlanNode child = node.getSource().accept(this, context);

            TableWriterNode.WriterTarget target = getTarget(child);

            return new TableCommitNode(node.getId(), child, target, node.getOutputSymbols());
        }

        public TableWriterNode.WriterTarget getTarget(PlanNode node)
        {
            if (node instanceof TableWriterNode) {
                return ((TableWriterNode) node).getTarget();
            }
            if (node instanceof DeleteNode) {
                return ((DeleteNode) node).getTarget();
            }
            if (node instanceof ExchangeNode) {
                PlanNode source = Iterables.getOnlyElement(node.getSources());
                return getTarget(source);
            }
            throw new IllegalArgumentException("Invalid child for TableCommitNode: " + node.getClass().getSimpleName());
        }

        private TableWriterNode.WriterTarget createWriterTarget(TableWriterNode.WriterTarget target)
        {
            if (target instanceof TableWriterNode.CreateName) {
                TableWriterNode.CreateName create = (TableWriterNode.CreateName) target;
                return new TableWriterNode.CreateHandle(metadata.beginCreateTable(session, create.getCatalog(), create.getTableMetadata()));
            }
            if (target instanceof TableWriterNode.InsertReference) {
                TableWriterNode.InsertReference insert = (TableWriterNode.InsertReference) target;
                return new TableWriterNode.InsertHandle(metadata.beginInsert(session, insert.getHandle()));
            }
            throw new IllegalArgumentException("Unhandled target type: " + target.getClass().getSimpleName());
        }

        private PlanNode rewriteDeleteTableScan(PlanNode node, TableHandle handle, RewriteContext<Void> context)
        {
            if (node instanceof TableScanNode) {
                TableScanNode scan = (TableScanNode) node;

                List<TableLayoutResult> layouts = metadata.getLayouts(
                        session,
                        handle,
                        new Constraint<>(scan.getCurrentConstraint(), bindings -> true),
                        Optional.of(ImmutableSet.copyOf(scan.getAssignments().values())));
                verify(layouts.size() == 1, "Expected exactly one layout for delete");
                TableLayoutHandle layout = Iterables.getOnlyElement(layouts).getLayout().getHandle();

                return new TableScanNode(
                        scan.getId(),
                        handle,
                        scan.getOutputSymbols(),
                        scan.getAssignments(),
                        Optional.of(layout),
                        scan.getCurrentConstraint(),
                        scan.getOriginalConstraint());
            }

            if (node instanceof FilterNode) {
                PlanNode source = rewriteDeleteTableScan(((FilterNode) node).getSource(), handle, context);
                return context.replaceChildren(node, ImmutableList.of(source));
            }
            if (node instanceof ProjectNode) {
                PlanNode source = rewriteDeleteTableScan(((ProjectNode) node).getSource(), handle, context);
                return context.replaceChildren(node, ImmutableList.of(source));
            }
            if (node instanceof SemiJoinNode) {
                PlanNode source = rewriteDeleteTableScan(((SemiJoinNode) node).getSource(), handle, context);
                return context.replaceChildren(node, ImmutableList.of(source, ((SemiJoinNode) node).getFilteringSource()));
            }
            throw new IllegalArgumentException("Invalid descendant for DeleteNode: " + node.getClass().getName());
        }
    }
}
