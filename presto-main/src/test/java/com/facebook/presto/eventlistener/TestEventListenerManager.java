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

package com.facebook.presto.eventlistener;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.eventlistener.CTEInformation;
import com.facebook.presto.spi.eventlistener.Column;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import com.facebook.presto.spi.eventlistener.OperatorStatistics;
import com.facebook.presto.spi.eventlistener.OutputColumnMetadata;
import com.facebook.presto.spi.eventlistener.PlanOptimizerInformation;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryFailureInfo;
import com.facebook.presto.spi.eventlistener.QueryIOMetadata;
import com.facebook.presto.spi.eventlistener.QueryInputMetadata;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import com.facebook.presto.spi.eventlistener.QueryOutputMetadata;
import com.facebook.presto.spi.eventlistener.QueryStatistics;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.facebook.presto.spi.eventlistener.SplitFailureInfo;
import com.facebook.presto.spi.eventlistener.SplitStatistics;
import com.facebook.presto.spi.eventlistener.StageStatistics;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.prestospark.PrestoSparkExecutionContext;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.spi.statistics.PlanStatisticsWithSourceInfo;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

@Test
public class TestEventListenerManager
{
    private static final Logger log = Logger.get(TestEventListenerManager.class);
    private final EventsCapture generatedEvents = new EventsCapture();

    @Test
    public void testMultipleEventListeners() throws IOException
    {
        Path tempFile1 = Files.createTempFile("listener1_", ".properties");
        Path tempFile2 = Files.createTempFile("listener2_", ".properties");
        Path tempFile3 = Files.createTempFile("listener3_", ".properties");

        writeProperties(tempFile1, "event-listener.name", "wxd-event-listener1");
        writeProperties(tempFile2, "event-listener.name", "wxd-event-listener2");
        writeProperties(tempFile3, "event-listener.name", "wxd-event-listener3");

        EventListenerConfig config = new EventListenerConfig()
                .setEventListenerFiles(tempFile1.toFile().getPath() + "," + tempFile2.toFile().getPath() + "," + tempFile3.toFile().getPath());
        EventListenerManager eventListenerManager = new EventListenerManager(config);
        TestingEventListener testingEventListener = new TestingEventListener(generatedEvents);
        eventListenerManager.addEventListenerFactory(new TestEventListenerFactory(testingEventListener, "wxd-event-listener1"));
        eventListenerManager.addEventListenerFactory(new TestEventListenerFactory(testingEventListener, "wxd-event-listener2"));
        eventListenerManager.addEventListenerFactory(new TestEventListenerFactory(testingEventListener, "wxd-event-listener3"));
        eventListenerManager.loadConfiguredEventListeners();

        QueryCreatedEvent queryCreatedEvent = createDummyQueryCreatedEvent();
        eventListenerManager.queryCreated(queryCreatedEvent);
        QueryCompletedEvent queryCompletedEvent = createDummyQueryCompletedEvent();
        eventListenerManager.queryCompleted(queryCompletedEvent);
        SplitCompletedEvent splitCompletedEvent = createDummySplitCompletedEvent();
        eventListenerManager.splitCompleted(splitCompletedEvent);

        assertEquals(generatedEvents.getQueryCreatedEvents().size(), 3);
        assertEquals(generatedEvents.getQueryCompletedEvents().size(), 3);
        assertEquals(generatedEvents.getSplitCompletedEvents().size(), 3);
        generatedEvents.getQueryCreatedEvents().forEach(event -> assertEquals(event, queryCreatedEvent));
        generatedEvents.getQueryCompletedEvents().forEach(event -> assertEquals(event, queryCompletedEvent));
        generatedEvents.getSplitCompletedEvents().forEach(event -> assertEquals(event, splitCompletedEvent));

        tryDeleteFile(tempFile1);
        tryDeleteFile(tempFile2);
        tryDeleteFile(tempFile3);
    }

    @Test
    public void testEventListenerNotRegistered() throws IOException
    {
        Path tempFile1 = Files.createTempFile("listener1_", ".properties");
        Path tempFile2 = Files.createTempFile("listener2_", ".properties");

        writeProperties(tempFile1, "event-listener.name", "wxd-event-listener1");
        writeProperties(tempFile2, "event-listener.name", "wxd-event-listener2");
        EventListenerConfig config = new EventListenerConfig().setEventListenerFiles(tempFile1.toFile().getPath() + "," + tempFile2.toFile().getPath());

        EventListenerManager eventListenerManager = new EventListenerManager(config);
        TestingEventListener testingEventListener = new TestingEventListener(generatedEvents);
        eventListenerManager.addEventListenerFactory(new TestEventListenerFactory(testingEventListener, "wxd-event-listener1"));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            eventListenerManager.loadConfiguredEventListeners();
        });

        String expectedMessage = "Event listener wxd-event-listener2 is not registered";
        assertEquals(exception.getMessage(), expectedMessage);
    }

    private void writeProperties(Path filePath, String key, String value)
            throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty(key, value);

        try (FileOutputStream outputStream = new FileOutputStream(filePath.toFile())) {
            properties.store(outputStream, "Test Properties");
        }
    }

    public static QueryCreatedEvent createDummyQueryCreatedEvent()
    {
        QueryMetadata metadata = createDummyQueryMetadata();
        QueryContext context = createDummyQueryContext();
        return new QueryCreatedEvent(Instant.now(), context, metadata);
    }

    public static QueryCompletedEvent createDummyQueryCompletedEvent()
    {
        QueryMetadata metadata = createDummyQueryMetadata();
        QueryStatistics statistics = createDummyQueryStatistics();
        QueryContext context = createDummyQueryContext();
        QueryIOMetadata ioMetadata = createDummyQueryIoMetadata();
        Optional<QueryFailureInfo> failureInfo = Optional.empty();
        List<PrestoWarning> warnings = new ArrayList<>();
        Optional<QueryType> queryType = Optional.empty();
        List<String> failedTasks = new ArrayList<>();
        Instant createTime = Instant.now();
        Instant executionStartTime = Instant.now().minusSeconds(10);
        Instant endTime = Instant.now().plusSeconds(10);
        List<StageStatistics> stageStatistics = new ArrayList<>();
        List<OperatorStatistics> operatorStatistics = new ArrayList<>();
        List<PlanStatisticsWithSourceInfo> planStatisticsRead = new ArrayList<>();
        List<PlanStatisticsWithSourceInfo> planStatisticsWritten = new ArrayList<>();
        Map<PlanNodeId, Map<PlanCanonicalizationStrategy, String>> planNodeHash = new HashMap<>();
        Map<PlanCanonicalizationStrategy, String> canonicalPlan = new HashMap<>();
        Optional<String> statsEquivalentPlan = Optional.empty();
        Optional<String> expandedQuery = Optional.empty();
        List<PlanOptimizerInformation> optimizerInformation = new ArrayList<>();
        List<CTEInformation> cteInformationList = new ArrayList<>();
        Set<String> scalarFunctions = new HashSet<>();
        Set<String> aggregateFunctions = new HashSet<>();
        Set<String> windowFunctions = new HashSet<>();
        Optional<PrestoSparkExecutionContext> prestoSparkExecutionContext = Optional.empty();
        Map<PlanCanonicalizationStrategy, String> hboPlanHash = new HashMap<>();
        Optional<Map<PlanNodeId, PlanNode>> planIdNodeMap = Optional.ofNullable(new HashMap<>());

        return new QueryCompletedEvent(
                metadata,
                statistics,
                context,
                ioMetadata,
                failureInfo,
                warnings,
                queryType,
                failedTasks,
                createTime,
                executionStartTime,
                endTime,
                stageStatistics,
                operatorStatistics,
                planStatisticsRead,
                planStatisticsWritten,
                planNodeHash,
                canonicalPlan,
                statsEquivalentPlan,
                expandedQuery,
                optimizerInformation,
                cteInformationList,
                scalarFunctions,
                aggregateFunctions,
                windowFunctions,
                prestoSparkExecutionContext,
                hboPlanHash,
                planIdNodeMap);
    }

    public static QueryStatistics createDummyQueryStatistics()
    {
        Duration cpuTime = Duration.ofMillis(1000);
        Duration retriedCpuTime = Duration.ofMillis(500);
        Duration wallTime = Duration.ofMillis(2000);
        Duration waitingForPrerequisitesTime = Duration.ofMillis(300);
        Duration queuedTime = Duration.ofMillis(1500);
        Duration waitingForResourcesTime = Duration.ofMillis(600);
        Duration semanticAnalyzingTime = Duration.ofMillis(700);
        Duration columnAccessPermissionCheckingTime = Duration.ofMillis(200);
        Duration dispatchingTime = Duration.ofMillis(1200);
        Duration planningTime = Duration.ofMillis(2500);
        Optional<Duration> analysisTime = Optional.of(Duration.ofMillis(1800));
        Duration executionTime = Duration.ofMillis(3500);

        int peakRunningTasks = 5;
        long peakUserMemoryBytes = 500000000L;
        long peakTotalNonRevocableMemoryBytes = 800000000L;
        long peakTaskUserMemory = 100000000L;
        long peakTaskTotalMemory = 200000000L;
        long peakNodeTotalMemory = 120000000L;
        long shuffledBytes = 10000000L;
        long shuffledRows = 200000L;
        long totalBytes = 30000000L;
        long totalRows = 400000L;
        long outputBytes = 5000000L;
        long outputRows = 60000L;
        long writtenOutputBytes = 7000000L;
        long writtenOutputRows = 80000L;
        long writtenIntermediateBytes = 9000000L;
        long spilledBytes = 1000000L;
        double cumulativeMemory = 150.5;
        double cumulativeTotalMemory = 200.5;
        int completedSplits = 100;
        boolean complete = true;
        RuntimeStats runtimeStats = new RuntimeStats();
        return new QueryStatistics(
                cpuTime,
                retriedCpuTime,
                wallTime,
                waitingForPrerequisitesTime,
                queuedTime,
                waitingForResourcesTime,
                semanticAnalyzingTime,
                columnAccessPermissionCheckingTime,
                dispatchingTime,
                planningTime,
                analysisTime,
                executionTime,
                peakRunningTasks,
                peakUserMemoryBytes,
                peakTotalNonRevocableMemoryBytes,
                peakTaskUserMemory,
                peakTaskTotalMemory,
                peakNodeTotalMemory,
                shuffledBytes,
                shuffledRows,
                totalBytes,
                totalRows,
                outputBytes,
                outputRows,
                writtenOutputBytes,
                writtenOutputRows,
                writtenIntermediateBytes,
                spilledBytes,
                cumulativeMemory,
                cumulativeTotalMemory,
                completedSplits,
                complete,
                runtimeStats);
    }

    private static QueryMetadata createDummyQueryMetadata()
    {
        String queryId = "20250216_173945_00000_9r4vt";
        Optional<String> transactionId = Optional.of("dummy-transaction-id");
        String query = "SELECT * FROM dummy_table";
        String queryHash = "dummy-query-hash";
        Optional<String> preparedQuery = Optional.of("PREPARE SELECT * FROM dummy_table");
        String queryState = "COMPLETED";
        URI uri = URI.create("http://localhost/query/dummy-query-id");
        Optional<String> plan = Optional.of("dummy-plan");
        Optional<String> jsonPlan = Optional.of("{\"plan\": \"dummy-plan\"}");
        Optional<String> graphvizPlan = Optional.of("digraph {node1 -> node2}");
        Optional<String> payload = Optional.of("dummy-payload");
        List<String> runtimeOptimizedStages = new ArrayList<>(Arrays.asList("stage1", "stage2"));
        Optional<String> tracingId = Optional.of("dummy-tracing-id");
        Optional<String> updateType = Optional.of("dummy-type");

        return new QueryMetadata(
                queryId,
                transactionId,
                query,
                queryHash,
                preparedQuery,
                queryState,
                uri,
                plan,
                jsonPlan,
                graphvizPlan,
                payload,
                runtimeOptimizedStages,
                tracingId,
                updateType);
    }

    private static QueryContext createDummyQueryContext()
    {
        String user = "dummyUser";
        String serverAddress = "127.0.0.1";
        String serverVersion = "testversion";
        String environment = "testing";
        String workerType = "worker-1";

        Optional<String> principal = Optional.of("dummyPrincipal");
        Optional<String> remoteClientAddress = Optional.of("192.168.1.100");
        Optional<String> userAgent = Optional.of("Mozilla/5.0");
        Optional<String> clientInfo = Optional.of("Dummy Client Info");
        Optional<String> source = Optional.empty();
        Optional<String> catalog = Optional.of("dummyCatalog");
        Optional<String> schema = Optional.of("dummySchema");
        Optional<ResourceGroupId> resourceGroupId = Optional.of(new ResourceGroupId("dummyGroupId"));

        Set<String> clientTags = new HashSet<>(Arrays.asList("tag1", "tag2", "tag3"));

        Map<String, String> sessionProperties = new HashMap<>();
        sessionProperties.put("property1", "value1");
        sessionProperties.put("property2", "value2");

        ResourceEstimates resourceEstimates = new ResourceEstimates(
                Optional.of(new com.facebook.airlift.units.Duration(1200, TimeUnit.SECONDS)),
                Optional.of(new com.facebook.airlift.units.Duration(1200, TimeUnit.SECONDS)),
                Optional.of(new com.facebook.airlift.units.DataSize(2, DataSize.Unit.GIGABYTE)),
                Optional.of(new com.facebook.airlift.units.DataSize(2, DataSize.Unit.GIGABYTE)));
        return new QueryContext(
                user,
                principal,
                remoteClientAddress,
                userAgent,
                clientInfo,
                clientTags,
                source,
                catalog,
                schema,
                resourceGroupId,
                sessionProperties,
                resourceEstimates,
                serverAddress,
                serverVersion,
                environment,
                workerType);
    }

    private static QueryIOMetadata createDummyQueryIoMetadata()
    {
        List<QueryInputMetadata> inputs = new ArrayList<>();
        QueryInputMetadata queryInputMetadata = getQueryInputMetadata();
        inputs.add(queryInputMetadata);
        OutputColumnMetadata column1 = new OutputColumnMetadata("column1", "int", new HashSet<>());
        OutputColumnMetadata column2 = new OutputColumnMetadata("column2", "varchar", new HashSet<>());
        OutputColumnMetadata column3 = new OutputColumnMetadata("column3", "varchar", new HashSet<>());
        List<OutputColumnMetadata> columns = new ArrayList<>();
        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        QueryOutputMetadata outputMetadata = new QueryOutputMetadata(
                "dummyCatalog",
                "dummySchema",
                "dummyTable",
                Optional.of("dummyConnectorMetadata"),
                Optional.of(true),
                "dummySerializedCommitOutput",
                Optional.of(columns));
        return new QueryIOMetadata(inputs, Optional.of(outputMetadata));
    }

    private static QueryInputMetadata getQueryInputMetadata()
    {
        String catalogName = "dummyCatalog";
        String schema = "dummySchema";
        String table = "dummyTable";
        String serializedCommitOutput = "commitOutputDummy";
        Column column1 = new Column("column1", "int");
        Column column2 = new Column("column2", "varchar");
        Column column3 = new Column("column3", "varchar");
        List<Column> columns = Arrays.asList(column1, column2, column3);
        Optional<Object> connectorInfo = Optional.of(new Object());
        return new QueryInputMetadata(
                catalogName,
                schema,
                table,
                columns,
                connectorInfo,
                Optional.empty(),
                serializedCommitOutput);
    }

    private static SplitCompletedEvent createDummySplitCompletedEvent()
    {
        Instant now = Instant.now();
        Instant startTimeDummy = now.minusSeconds(100);
        Instant endTimeDummy = now.minusSeconds(50);
        SplitStatistics stats = createDummySplitStatistics();
        SplitFailureInfo failureInfo = new SplitFailureInfo("Error", "Dummy failure message");
        return new SplitCompletedEvent(
                "query123",
                "stage456",
                "stageExec789",
                "task012",
                now,
                Optional.of(startTimeDummy),
                Optional.of(endTimeDummy),
                stats,
                Optional.of(failureInfo),
                "dummyPayload");
    }

    private static SplitStatistics createDummySplitStatistics()
    {
        Duration cpuTime = Duration.ofSeconds(500);
        Duration wallTime = Duration.ofSeconds(1000);
        Duration queuedTime = Duration.ofSeconds(120);
        Duration completedReadTime = Duration.ofSeconds(800);

        long completedPositions = 1500;
        long completedDataSizeBytes = 10000000L;

        Optional<Duration> timeToFirstByte = Optional.of(Duration.ofSeconds(10));
        Optional<Duration> timeToLastByte = Optional.empty();

        return new SplitStatistics(
                cpuTime,
                wallTime,
                queuedTime,
                completedReadTime,
                completedPositions,
                completedDataSizeBytes,
                timeToFirstByte,
                timeToLastByte);
    }

    private static void tryDeleteFile(Path path)
    {
        try {
            File file = new File(path.toUri());
            if (file.exists()) {
                Files.delete(file.toPath());
            }
        }
        catch (IOException e) {
            log.error(e, "Could not delete file found at [%s]", path);
        }
    }

    private static class TestEventListenerFactory
            implements EventListenerFactory
    {
        public static String name;
        private final TestingEventListener testingEventListener;

        public TestEventListenerFactory(TestingEventListener testingEventListener, String name)
        {
            this.testingEventListener = requireNonNull(testingEventListener, "testingEventListener is null");
            this.name = name;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public EventListener create(Map<String, String> config)
        {
            return testingEventListener;
        }
    }

    private static class TestingEventListener
            implements EventListener
    {
        private final EventsCapture eventsCapture;

        public TestingEventListener(EventsCapture eventsCapture)
        {
            this.eventsCapture = eventsCapture;
        }

        @Override
        public void queryCreated(QueryCreatedEvent queryCreatedEvent)
        {
            eventsCapture.addQueryCreated(queryCreatedEvent);
        }

        @Override
        public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
        {
            eventsCapture.addQueryCompleted(queryCompletedEvent);
        }

        @Override
        public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
        {
            eventsCapture.addSplitCompleted(splitCompletedEvent);
        }
    }

    private static class EventsCapture
    {
        private final ImmutableList.Builder<QueryCreatedEvent> queryCreatedEvents = ImmutableList.builder();
        private final ImmutableList.Builder<QueryCompletedEvent> queryCompletedEvents = ImmutableList.builder();
        private final ImmutableList.Builder<SplitCompletedEvent> splitCompletedEvents = ImmutableList.builder();

        public synchronized void addQueryCreated(QueryCreatedEvent event)
        {
            queryCreatedEvents.add(event);
        }

        public synchronized void addQueryCompleted(QueryCompletedEvent event)
        {
            queryCompletedEvents.add(event);
        }

        public synchronized void addSplitCompleted(SplitCompletedEvent event)
        {
            splitCompletedEvents.add(event);
        }

        public List<QueryCreatedEvent> getQueryCreatedEvents()
        {
            return queryCreatedEvents.build();
        }

        public List<QueryCompletedEvent> getQueryCompletedEvents()
        {
            return queryCompletedEvents.build();
        }

        public List<SplitCompletedEvent> getSplitCompletedEvents()
        {
            return splitCompletedEvents.build();
        }
    }
}
