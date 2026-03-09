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
package com.facebook.presto.plugin.openlineage;

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.ResourceEstimates;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

public final class PrestoEventData
{
    public static final QueryIOMetadata queryIOMetadata;
    public static final QueryContext queryContext;
    public static final QueryMetadata queryMetadata;
    public static final QueryStatistics queryStatistics;
    public static final QueryCompletedEvent queryCompleteEvent;
    public static final QueryCreatedEvent queryCreatedEvent;

    private PrestoEventData()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    static {
        queryIOMetadata = new QueryIOMetadata(Collections.emptyList(), Optional.empty());

        queryContext = new QueryContext(
                "user",
                Optional.of("principal"),
                Optional.of("127.0.0.1"),
                Optional.of("Some-User-Agent"),
                Optional.of("Some client info"),
                new HashSet<>(), // clientTags
                Optional.of("some-presto-client"),
                Optional.of("catalog"),
                Optional.of("schema"),
                Optional.of(new ResourceGroupId("name")),
                new HashMap<>(), // sessionProperties
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                "serverAddress",
                "serverVersion",
                "environment",
                "worker"); // workerType

        queryMetadata = new QueryMetadata(
                "queryId",
                Optional.of("transactionId"),
                "create table b.c as select * from y.z",
                "queryHash",
                Optional.of("preparedQuery"),
                "COMPLETED",
                URI.create("http://localhost"),
                Optional.of("queryPlan"),
                Optional.empty(), // jsonPlan
                Optional.empty(), // graphvizPlan
                Optional.empty(), // payload
                List.of(), // runtimeOptimizedStages
                Optional.empty(), // tracingId
                Optional.of("updateType"));

        queryStatistics = new QueryStatistics(
                Duration.ofSeconds(1), // cpuTime
                Duration.ofSeconds(1), // retriedCpuTime
                Duration.ofSeconds(1), // wallTime
                Duration.ofSeconds(1), // totalScheduledTime
                Duration.ofSeconds(0), // waitingForPrerequisitesTime
                Duration.ofSeconds(0), // queuedTime
                Duration.ofSeconds(0), // waitingForResourcesTime
                Duration.ofSeconds(0), // semanticAnalyzingTime
                Duration.ofSeconds(0), // columnAccessPermissionCheckingTime
                Duration.ofSeconds(0), // dispatchingTime
                Duration.ofSeconds(0), // planningTime
                Optional.empty(), // analysisTime
                Duration.ofSeconds(1), // executionTime
                0, // peakRunningTasks
                0L, // peakUserMemoryBytes
                0L, // peakTotalNonRevocableMemoryBytes
                0L, // peakTaskUserMemory
                0L, // peakTaskTotalMemory
                0L, // peakNodeTotalMemory
                0L, // shuffledBytes
                0L, // shuffledRows
                0L, // totalBytes
                0L, // totalRows
                0L, // outputBytes
                0L, // outputRows
                0L, // writtenOutputBytes
                0L, // writtenOutputRows
                0L, // writtenIntermediateBytes
                0L, // spilledBytes
                0.0, // cumulativeMemory
                0.0, // cumulativeTotalMemory
                0, // completedSplits
                true, // complete
                new RuntimeStats());

        queryCompleteEvent = new QueryCompletedEvent(
                queryMetadata,
                queryStatistics,
                queryContext,
                queryIOMetadata,
                Optional.empty(), // failureInfo
                Collections.emptyList(), // warnings
                Optional.of(QueryType.INSERT), // queryType
                Collections.emptyList(), // failedTasks
                Instant.parse("2025-04-28T11:23:55.384424Z"), // createTime
                Instant.parse("2025-04-28T11:24:16.256207Z"), // executionStartTime
                Instant.parse("2025-04-28T11:24:26.993340Z"), // endTime
                Collections.emptyList(), // stageStatistics
                Collections.emptyList(), // operatorStatistics
                Collections.emptyList(), // planStatisticsRead
                Collections.emptyList(), // planStatisticsWritten
                Collections.emptyMap(), // planNodeHash
                Collections.emptyMap(), // canonicalPlan
                Optional.empty(), // statsEquivalentPlan
                Optional.empty(), // expandedQuery
                Collections.emptyList(), // optimizerInformation
                Collections.emptyList(), // cteInformationList
                Collections.emptySet(), // scalarFunctions
                Collections.emptySet(), // aggregateFunctions
                Collections.emptySet(), // windowFunctions
                Optional.empty(), // prestoSparkExecutionContext
                Collections.emptyMap(), // hboPlanHash
                Optional.empty(), // planNodeIdMap
                Optional.empty()); // qualifiedName

        queryCreatedEvent = new QueryCreatedEvent(
                Instant.parse("2025-04-28T11:23:55.384424Z"),
                queryContext,
                queryMetadata);
    }
}
