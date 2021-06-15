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

package com.facebook.presto.execution.scheduler;

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.AnalyzeTableHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode.WriterTarget;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static com.google.common.graph.Traverser.forTree;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TableWriteInfo
{
    private final Optional<ExecutionWriterTarget> writerTarget;
    private final Optional<AnalyzeTableHandle> analyzeTableHandle;
    private final Optional<DeleteScanInfo> deleteScanInfo;

    @JsonCreator
    public TableWriteInfo(
            @JsonProperty("writerTarget") Optional<ExecutionWriterTarget> writerTarget,
            @JsonProperty("analyzeTableHandle") Optional<AnalyzeTableHandle> analyzeTableHandle,
            @JsonProperty("deleteScanInfo") Optional<DeleteScanInfo> deleteScanInfo)
    {
        this.writerTarget = requireNonNull(writerTarget, "writerTarget is null");
        this.analyzeTableHandle = requireNonNull(analyzeTableHandle, "analyzeTableHandle is null");
        this.deleteScanInfo = requireNonNull(deleteScanInfo, "deleteScanInfo is null");
        checkArgument(!analyzeTableHandle.isPresent() || !writerTarget.isPresent() && !deleteScanInfo.isPresent(), "analyzeTableHandle is present, so no other fields should be present");
        checkArgument(!deleteScanInfo.isPresent() || writerTarget.isPresent(), "deleteScanInfo is present, but writerTarget is not present");
    }

    public static TableWriteInfo createTableWriteInfo(StreamingSubPlan plan, Metadata metadata, Session session)
    {
        Optional<ExecutionWriterTarget> writerTarget = createWriterTarget(plan, metadata, session);
        Optional<AnalyzeTableHandle> analyzeTableHandle = createAnalyzeTableHandle(plan, metadata, session);
        Optional<DeleteScanInfo> deleteScanInfo = createDeleteScanInfo(plan, writerTarget, metadata, session);
        return new TableWriteInfo(writerTarget, analyzeTableHandle, deleteScanInfo);
    }

    private static Optional<ExecutionWriterTarget> createWriterTarget(StreamingSubPlan plan, Metadata metadata, Session session)
    {
        Optional<TableFinishNode> tableFinishNode = findSinglePlanNode(plan, TableFinishNode.class);
        if (tableFinishNode.isPresent()) {
            WriterTarget target = tableFinishNode.get().getTarget().orElseThrow(() -> new VerifyException("target is absent"));
            if (target instanceof TableWriterNode.CreateName) {
                TableWriterNode.CreateName create = (TableWriterNode.CreateName) target;
                return Optional.of(new ExecutionWriterTarget.CreateHandle(metadata.beginCreateTable(session, create.getConnectorId().getCatalogName(), create.getTableMetadata(), create.getLayout()), create.getSchemaTableName()));
            }
            if (target instanceof TableWriterNode.InsertReference) {
                TableWriterNode.InsertReference insert = (TableWriterNode.InsertReference) target;
                return Optional.of(new ExecutionWriterTarget.InsertHandle(metadata.beginInsert(session, insert.getHandle()), insert.getSchemaTableName()));
            }
            if (target instanceof TableWriterNode.DeleteHandle) {
                TableWriterNode.DeleteHandle delete = (TableWriterNode.DeleteHandle) target;
                return Optional.of(new ExecutionWriterTarget.DeleteHandle(metadata.beginDelete(session, delete.getHandle()), delete.getSchemaTableName()));
            }
            if (target instanceof TableWriterNode.RefreshMaterializedViewReference) {
                TableWriterNode.RefreshMaterializedViewReference refresh = (TableWriterNode.RefreshMaterializedViewReference) target;
                return Optional.of(new ExecutionWriterTarget.RefreshMaterializedViewHandle(metadata.beginRefreshMaterializedView(session, refresh.getHandle()), refresh.getSchemaTableName()));
            }
            throw new IllegalArgumentException("Unhandled target type: " + target.getClass().getSimpleName());
        }

        return Optional.empty();
    }

    private static Optional<AnalyzeTableHandle> createAnalyzeTableHandle(StreamingSubPlan plan, Metadata metadata, Session session)
    {
        Optional<StatisticsWriterNode> node = findSinglePlanNode(plan, StatisticsWriterNode.class);
        if (node.isPresent()) {
            return Optional.of(metadata.beginStatisticsCollection(session, node.get().getTableHandle()));
        }
        return Optional.empty();
    }

    private static Optional<DeleteScanInfo> createDeleteScanInfo(StreamingSubPlan plan, Optional<ExecutionWriterTarget> writerTarget, Metadata metadata, Session session)
    {
        if (writerTarget.isPresent() && writerTarget.get() instanceof ExecutionWriterTarget.DeleteHandle) {
            TableHandle tableHandle = ((ExecutionWriterTarget.DeleteHandle) writerTarget.get()).getHandle();
            DeleteNode delete = getOnlyElement(findPlanNodes(plan, DeleteNode.class));
            TableScanNode tableScan = getDeleteTableScan(delete);
            TupleDomain<ColumnHandle> originalEnforcedConstraint = tableScan.getEnforcedConstraint();
            TableLayoutResult layoutResult = metadata.getLayout(
                    session,
                    tableHandle,
                    new Constraint<>(originalEnforcedConstraint),
                    Optional.of(ImmutableSet.copyOf(tableScan.getAssignments().values())));

            return Optional.of(new DeleteScanInfo(tableScan.getId(), layoutResult.getLayout().getNewTableHandle()));
        }
        return Optional.empty();
    }

    private static <T extends PlanNode> Optional<T> findSinglePlanNode(StreamingSubPlan plan, Class<T> clazz)
    {
        List<T> allMatches = findPlanNodes(plan, clazz);
        switch (allMatches.size()) {
            case 0:
                return Optional.empty();
            case 1:
                return Optional.of(getOnlyElement(allMatches));
            default:
                throw new IllegalArgumentException(format("Multiple matches found for class %s", clazz));
        }
    }

    private static <T extends PlanNode> List<T> findPlanNodes(StreamingSubPlan plan, Class<T> clazz)
    {
        return stream(forTree(StreamingSubPlan::getChildren).depthFirstPreOrder(plan))
                .map(subPlan -> PlanNodeSearcher.searchFrom(subPlan.getFragment().getRoot())
                        .where(clazz::isInstance)
                        .<T>findAll())
                .flatMap(Collection::stream)
                .collect(toImmutableList());
    }

    private static TableScanNode getDeleteTableScan(PlanNode node)
    {
        if (node instanceof TableScanNode) {
            return (TableScanNode) node;
        }
        if (node instanceof DeleteNode) {
            return getDeleteTableScan(((DeleteNode) node).getSource());
        }
        if (node instanceof FilterNode) {
            return getDeleteTableScan(((FilterNode) node).getSource());
        }
        if (node instanceof ProjectNode) {
            return getDeleteTableScan(((ProjectNode) node).getSource());
        }
        if (node instanceof SemiJoinNode) {
            return getDeleteTableScan(((SemiJoinNode) node).getSource());
        }
        if (node instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) node;
            return getDeleteTableScan(joinNode.getLeft());
        }
        throw new IllegalArgumentException("Invalid descendant for DeleteNode: " + node.getClass().getName());
    }

    @JsonProperty
    public Optional<ExecutionWriterTarget> getWriterTarget()
    {
        return writerTarget;
    }

    @JsonProperty
    public Optional<AnalyzeTableHandle> getAnalyzeTableHandle()
    {
        return analyzeTableHandle;
    }

    @JsonProperty
    public Optional<DeleteScanInfo> getDeleteScanInfo()
    {
        return deleteScanInfo;
    }

    public static class DeleteScanInfo
    {
        private final PlanNodeId id;
        private final TableHandle tableHandle;

        @JsonCreator
        public DeleteScanInfo(@JsonProperty("id") PlanNodeId id, @JsonProperty("tableHandle") TableHandle tableHandle)
        {
            this.id = id;
            this.tableHandle = tableHandle;
        }

        @JsonProperty
        public PlanNodeId getId()
        {
            return id;
        }

        @JsonProperty
        public TableHandle getTableHandle()
        {
            return tableHandle;
        }
    }
}
