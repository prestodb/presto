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
package com.facebook.presto.spi.eventlistener;

import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.prestospark.PrestoSparkExecutionContext;
import com.facebook.presto.spi.statistics.PlanStatisticsWithSourceInfo;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class QueryCompletedEvent
{
    private final QueryMetadata metadata;
    private final QueryStatistics statistics;
    private final QueryContext context;
    private final QueryIOMetadata ioMetadata;
    private final Optional<QueryFailureInfo> failureInfo;
    private final List<PrestoWarning> warnings;
    private final Optional<QueryType> queryType;
    private final List<String> failedTasks;
    private final List<StageStatistics> stageStatistics;
    private final List<OperatorStatistics> operatorStatistics;
    private final List<PlanStatisticsWithSourceInfo> planStatisticsRead;
    private final List<PlanStatisticsWithSourceInfo> planStatisticsWritten;
    private final Map<PlanNodeId, Map<PlanCanonicalizationStrategy, String>> planNodeHash;
    private final Map<PlanCanonicalizationStrategy, String> canonicalPlan;
    private final Optional<String> statsEquivalentPlan;

    private final Instant createTime;
    private final Instant executionStartTime;
    private final Instant endTime;
    private final Optional<String> expandedQuery;
    private final List<PlanOptimizerInformation> optimizerInformation;
    private final List<CTEInformation> cteInformationList;
    private final Set<String> scalarFunctions;
    private final Set<String> aggregateFunctions;
    private final Set<String> windowFunctions;
    private final Optional<PrestoSparkExecutionContext> prestoSparkExecutionContext;
    private final Map<PlanCanonicalizationStrategy, String> hboPlanHash;
    private final Optional<Map<PlanNodeId, PlanNode>> planIdNodeMap;

    public QueryCompletedEvent(
            QueryMetadata metadata,
            QueryStatistics statistics,
            QueryContext context,
            QueryIOMetadata ioMetadata,
            Optional<QueryFailureInfo> failureInfo,
            List<PrestoWarning> warnings,
            Optional<QueryType> queryType,
            List<String> failedTasks,
            Instant createTime,
            Instant executionStartTime,
            Instant endTime,
            List<StageStatistics> stageStatistics,
            List<OperatorStatistics> operatorStatistics,
            List<PlanStatisticsWithSourceInfo> planStatisticsRead,
            List<PlanStatisticsWithSourceInfo> planStatisticsWritten,
            Map<PlanNodeId, Map<PlanCanonicalizationStrategy, String>> planNodeHash,
            Map<PlanCanonicalizationStrategy, String> canonicalPlan,
            Optional<String> statsEquivalentPlan,
            Optional<String> expandedQuery,
            List<PlanOptimizerInformation> optimizerInformation,
            List<CTEInformation> cteInformationList,
            Set<String> scalarFunctions,
            Set<String> aggregateFunctions,
            Set<String> windowFunctions,
            Optional<PrestoSparkExecutionContext> prestoSparkExecutionContext,
            Map<PlanCanonicalizationStrategy, String> hboPlanHash,
            Optional<Map<PlanNodeId, PlanNode>> planNodeIdMap)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.context = requireNonNull(context, "context is null");
        this.ioMetadata = requireNonNull(ioMetadata, "ioMetadata is null");
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
        this.warnings = requireNonNull(warnings, "queryWarnings is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.failedTasks = requireNonNull(failedTasks, "failedTasks is null");
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.executionStartTime = requireNonNull(executionStartTime, "executionStartTime is null");
        this.endTime = requireNonNull(endTime, "endTime is null");
        this.stageStatistics = requireNonNull(stageStatistics, "stageStatistics is null");
        this.operatorStatistics = requireNonNull(operatorStatistics, "operatorStatistics is null");
        this.planStatisticsRead = requireNonNull(planStatisticsRead, "planStatisticsRead is null");
        this.planNodeHash = requireNonNull(planNodeHash, "planNodeHash is null");
        this.canonicalPlan = requireNonNull(canonicalPlan, "canonicalPlan is null");
        this.statsEquivalentPlan = requireNonNull(statsEquivalentPlan, "statsEquivalentPlan is null");
        this.planStatisticsWritten = requireNonNull(planStatisticsWritten, "planStatisticsWritten is null");
        this.expandedQuery = requireNonNull(expandedQuery, "expandedQuery is null");
        this.optimizerInformation = requireNonNull(optimizerInformation, "optimizerInformation is null");
        this.cteInformationList = requireNonNull(cteInformationList, "cteInformationList is null");
        this.scalarFunctions = requireNonNull(scalarFunctions, "scalarFunctions is null");
        this.aggregateFunctions = requireNonNull(aggregateFunctions, "aggregateFunctions is null");
        this.windowFunctions = requireNonNull(windowFunctions, "windowFunctions is null");
        this.prestoSparkExecutionContext = requireNonNull(prestoSparkExecutionContext, "prestoSparkExecutionContext is null");
        this.hboPlanHash = requireNonNull(hboPlanHash, "planHash is null");
        this.planIdNodeMap = requireNonNull(planNodeIdMap, "planNodeIdMap is null");
    }

    public QueryMetadata getMetadata()
    {
        return metadata;
    }

    public QueryStatistics getStatistics()
    {
        return statistics;
    }

    public QueryContext getContext()
    {
        return context;
    }

    public QueryIOMetadata getIoMetadata()
    {
        return ioMetadata;
    }

    public Optional<QueryFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    public List<PrestoWarning> getWarnings()
    {
        return warnings;
    }

    public Optional<QueryType> getQueryType()
    {
        return queryType;
    }

    public List<String> getFailedTasks()
    {
        return failedTasks;
    }

    public Instant getCreateTime()
    {
        return createTime;
    }

    public Instant getExecutionStartTime()
    {
        return executionStartTime;
    }

    public Instant getEndTime()
    {
        return endTime;
    }

    public List<StageStatistics> getStageStatistics()
    {
        return stageStatistics;
    }

    public List<OperatorStatistics> getOperatorStatistics()
    {
        return operatorStatistics;
    }

    public List<PlanStatisticsWithSourceInfo> getPlanStatisticsRead()
    {
        return planStatisticsRead;
    }

    public List<PlanStatisticsWithSourceInfo> getPlanStatisticsWritten()
    {
        return planStatisticsWritten;
    }

    public Map<PlanNodeId, Map<PlanCanonicalizationStrategy, String>> getPlanNodeHash()
    {
        return planNodeHash;
    }

    public Map<PlanCanonicalizationStrategy, String> getCanonicalPlan()
    {
        return canonicalPlan;
    }

    public Optional<String> getStatsEquivalentPlan()
    {
        return statsEquivalentPlan;
    }

    public Optional<String> getExpandedQuery()
    {
        return expandedQuery;
    }

    public List<PlanOptimizerInformation> getOptimizerInformation()
    {
        return optimizerInformation;
    }

    public List<CTEInformation> getCteInformationList()
    {
        return cteInformationList;
    }

    public Set<String> getScalarFunctions()
    {
        return scalarFunctions;
    }

    public Set<String> getAggregateFunctions()
    {
        return aggregateFunctions;
    }

    public Set<String> getWindowFunctions()
    {
        return windowFunctions;
    }

    public Optional<PrestoSparkExecutionContext> getPrestoSparkExecutionContext()
    {
        return prestoSparkExecutionContext;
    }

    public Map<PlanCanonicalizationStrategy, String> getHboPlanHash()
    {
        return hboPlanHash;
    }

    public Optional<Map<PlanNodeId, PlanNode>> getPlanNodeIdMap()
    {
        return planIdNodeMap;
    }
}
