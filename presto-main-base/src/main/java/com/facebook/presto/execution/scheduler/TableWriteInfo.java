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
import com.facebook.presto.metadata.AnalyzeTableHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.VerifyException;

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

    @JsonCreator
    public TableWriteInfo(
            @JsonProperty("writerTarget") Optional<ExecutionWriterTarget> writerTarget,
            @JsonProperty("analyzeTableHandle") Optional<AnalyzeTableHandle> analyzeTableHandle)
    {
        this.writerTarget = requireNonNull(writerTarget, "writerTarget is null");
        this.analyzeTableHandle = requireNonNull(analyzeTableHandle, "analyzeTableHandle is null");
        checkArgument(!analyzeTableHandle.isPresent() || !writerTarget.isPresent(), "analyzeTableHandle is present, so no other fields should be present");
    }

    public static TableWriteInfo createTableWriteInfo(StreamingSubPlan plan, Metadata metadata, Session session)
    {
        Optional<ExecutionWriterTarget> writerTarget = createWriterTarget(plan, metadata, session);
        Optional<AnalyzeTableHandle> analyzeTableHandle = createAnalyzeTableHandle(plan, metadata, session);
        return new TableWriteInfo(writerTarget, analyzeTableHandle);
    }

    public static TableWriteInfo createTableWriteInfo(PlanNode planNode, Metadata metadata, Session session)
    {
        Optional<ExecutionWriterTarget> writerTarget = createWriterTarget(planNode, metadata, session);
        Optional<AnalyzeTableHandle> analyzeTableHandle = createAnalyzeTableHandle(planNode, metadata, session);
        return new TableWriteInfo(writerTarget, analyzeTableHandle);
    }

    private static Optional<ExecutionWriterTarget> createWriterTarget(Optional<TableFinishNode> finishNodeOptional, Metadata metadata, Session session)
    {
        if (finishNodeOptional.isPresent()) {
            TableWriterNode.WriterTarget target = finishNodeOptional.get().getTarget().orElseThrow(() -> new VerifyException("target is absent"));
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
            if (target instanceof TableWriterNode.UpdateTarget) {
                TableWriterNode.UpdateTarget update = (TableWriterNode.UpdateTarget) target;
                return Optional.of(new ExecutionWriterTarget.UpdateHandle(update.getHandle(), update.getSchemaTableName()));
            }
            if (target instanceof TableWriterNode.MergeTarget) {
                TableWriterNode.MergeTarget mergeTarget = (TableWriterNode.MergeTarget) target;

                // TODO #20578: WIP - The ".get()." method call fails.
                // Caused by: java.util.NoSuchElementException: No value present
                // at java.util.Optional.get(Optional.java:135)
                // at com.facebook.presto.execution.scheduler.TableWriteInfo.createWriterTarget(TableWriteInfo.java:117)
                // at com.facebook.presto.execution.scheduler.TableWriteInfo.createWriterTarget(TableWriteInfo.java:127)
                // at com.facebook.presto.execution.scheduler.TableWriteInfo.createTableWriteInfo(TableWriteInfo.java:75)
                // at com.facebook.presto.execution.scheduler.SectionExecutionFactory.createSectionExecutions(SectionExecutionFactory.java:178)
                // at com.facebook.presto.execution.scheduler.SqlQueryScheduler.createStageExecutions(SqlQueryScheduler.java:370)
                // at com.facebook.presto.execution.scheduler.SqlQueryScheduler.<init>(SqlQueryScheduler.java:248)
                // at com.facebook.presto.execution.scheduler.SqlQueryScheduler.createSqlQueryScheduler(SqlQueryScheduler.java:178)
                // at com.facebook.presto.execution.SqlQueryExecution.createQueryScheduler(SqlQueryExecution.java:671)
//                return Optional.of(new ExecutionWriterTarget.MergeHandle(mergeTarget.getHandle(), mergeTarget.getMergeHandle().get().getConnectorMergeHandle()/*, mergeTarget.getSchemaTableName()*/));

//                return Optional.of(new ExecutionWriterTarget.MergeHandle(metadata.beginMerge(session, mergeTarget.getHandle())));
                return Optional.of(new ExecutionWriterTarget.MergeHandle(mergeTarget.getMergeHandle().get()));
            }
            throw new IllegalArgumentException("Unhandled target type: " + target.getClass().getSimpleName());
        }

        return Optional.empty();
    }

    private static Optional<ExecutionWriterTarget> createWriterTarget(StreamingSubPlan plan, Metadata metadata, Session session)
    {
        return createWriterTarget(findSinglePlanNode(plan, TableFinishNode.class), metadata, session);
    }

    private static Optional<ExecutionWriterTarget> createWriterTarget(PlanNode planNode, Metadata metadata, Session session)
    {
        return createWriterTarget(findSinglePlanNode(planNode, TableFinishNode.class), metadata, session);
    }

    private static Optional<AnalyzeTableHandle> createAnalyzeTableHandle(StreamingSubPlan plan, Metadata metadata, Session session)
    {
        return createAnalyzeTableHandle(findSinglePlanNode(plan, StatisticsWriterNode.class), metadata, session);
    }

    private static Optional<AnalyzeTableHandle> createAnalyzeTableHandle(PlanNode planNode, Metadata metadata, Session session)
    {
        return createAnalyzeTableHandle(findSinglePlanNode(planNode, StatisticsWriterNode.class), metadata, session);
    }

    private static Optional<AnalyzeTableHandle> createAnalyzeTableHandle(Optional<StatisticsWriterNode> statisticsWriterNodeOptional, Metadata metadata, Session session)
    {
        return statisticsWriterNodeOptional.map(node -> metadata.beginStatisticsCollection(session, node.getTableHandle()));
    }

    private static <T extends PlanNode> Optional<T> findSinglePlanNode(PlanNode planNode, Class<T> clazz)
    {
        return PlanNodeSearcher
                .searchFrom(planNode)
                .where(clazz::isInstance)
                .findSingle();
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
}
