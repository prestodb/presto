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
package com.facebook.presto.spark.execution;

import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.QueryStateTimer;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spark.ErrorClassifier;
import com.facebook.presto.spark.PrestoSparkMetadataStorage;
import com.facebook.presto.spark.PrestoSparkQueryData;
import com.facebook.presto.spark.PrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.PrestoSparkQueryStatusInfo;
import com.facebook.presto.spark.PrestoSparkServiceWaitTimeMetrics;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.RddAndMore;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutor;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskInputs;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.planner.PrestoSparkPlanFragmenter;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner.PlanAndMore;
import com.facebook.presto.spark.planner.PrestoSparkRddFactory;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.spark.PrestoSparkQueryExecutionFactory.createQueryInfo;
import static com.facebook.presto.spark.PrestoSparkQueryExecutionFactory.createStageInfo;
import static com.facebook.presto.spark.classloader_interface.ScalaUtils.collectScalaIterator;
import static com.facebook.presto.spark.classloader_interface.ScalaUtils.emptyScalaIterator;
import static com.facebook.presto.spark.util.PrestoSparkUtils.computeNextTimeout;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textDistributedPlan;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PrestoSparkStaticQueryExecution
        extends AbstractPrestoSparkQueryExecution
{
    private static final Logger log = Logger.get(PrestoSparkStaticQueryExecution.class);

    public PrestoSparkStaticQueryExecution(
            JavaSparkContext sparkContext,
            Session session,
            QueryMonitor queryMonitor,
            CollectionAccumulator<SerializedTaskInfo> taskInfoCollector,
            CollectionAccumulator<PrestoSparkShuffleStats> shuffleStatsCollector,
            PrestoSparkTaskExecutorFactory taskExecutorFactory,
            PrestoSparkTaskExecutorFactoryProvider taskExecutorFactoryProvider,
            QueryStateTimer queryStateTimer,
            WarningCollector warningCollector,
            String query,
            PlanAndMore planAndMore,
            Optional<String> sparkQueueName,
            Codec<TaskInfo> taskInfoCodec,
            JsonCodec<PrestoSparkTaskDescriptor> sparkTaskDescriptorJsonCodec,
            JsonCodec<PrestoSparkQueryStatusInfo> queryStatusInfoJsonCodec,
            JsonCodec<PrestoSparkQueryData> queryDataJsonCodec,
            PrestoSparkRddFactory rddFactory,
            TransactionManager transactionManager,
            PagesSerde pagesSerde,
            PrestoSparkExecutionExceptionFactory executionExceptionFactory,
            Duration queryTimeout,
            long queryCompletionDeadline,
            PrestoSparkMetadataStorage metadataStorage,
            Optional<String> queryStatusInfoOutputLocation,
            Optional<String> queryDataOutputLocation,
            TempStorage tempStorage,
            NodeMemoryConfig nodeMemoryConfig,
            Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics,
            Optional<ErrorClassifier> errorClassifier,
            PrestoSparkPlanFragmenter planFragmenter,
            Metadata metadata,
            PartitioningProviderManager partitioningProviderManager)
    {
        super(
                sparkContext,
                session,
                queryMonitor,
                taskInfoCollector,
                shuffleStatsCollector,
                taskExecutorFactory,
                taskExecutorFactoryProvider,
                queryStateTimer,
                warningCollector,
                query,
                planAndMore,
                sparkQueueName,
                taskInfoCodec,
                sparkTaskDescriptorJsonCodec,
                queryStatusInfoJsonCodec,
                queryDataJsonCodec,
                rddFactory,
                transactionManager,
                pagesSerde,
                executionExceptionFactory,
                queryTimeout,
                queryCompletionDeadline,
                metadataStorage,
                queryStatusInfoOutputLocation,
                queryDataOutputLocation,
                tempStorage,
                nodeMemoryConfig,
                waitTimeMetrics,
                errorClassifier,
                planFragmenter,
                metadata,
                partitioningProviderManager);
    }

    @Override
    protected List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>> doExecute()
            throws SparkException, TimeoutException
    {
        SubPlan rootFragmentedPlan = createFragmentedPlan();
        setFinalFragmentedPlan(rootFragmentedPlan);
        TableWriteInfo tableWriteInfo = getTableWriteInfo(session, rootFragmentedPlan);
        PlanFragment rootFragment = rootFragmentedPlan.getFragment();

        queryStateTimer.beginRunning();
        if (rootFragment.getPartitioning().equals(COORDINATOR_DISTRIBUTION)) {
            PrestoSparkTaskDescriptor taskDescriptor = new PrestoSparkTaskDescriptor(
                    session.toSessionRepresentation(),
                    session.getIdentity().getExtraCredentials(),
                    rootFragment,
                    tableWriteInfo);
            SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor = new SerializedPrestoSparkTaskDescriptor(sparkTaskDescriptorJsonCodec.toJsonBytes(taskDescriptor));

            Map<PlanFragmentId, RddAndMore<PrestoSparkSerializedPage>> inputRdds = new HashMap<>();
            for (SubPlan child : rootFragmentedPlan.getChildren()) {
                inputRdds.put(child.getFragment().getId(), createRdd(child, PrestoSparkSerializedPage.class, tableWriteInfo));
            }

            Map<String, JavaFutureAction<List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>>>> inputFutures = inputRdds.entrySet().stream()
                    .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().getRdd().collectAsync()));

            PrestoSparkQueryExecutionFactory.waitForActionsCompletionWithTimeout(inputFutures.values(), computeNextTimeout(queryCompletionDeadline), MILLISECONDS, waitTimeMetrics);

            // release memory retained by the RDDs (splits and dependencies)
            inputRdds = null;

            ImmutableMap.Builder<String, List<PrestoSparkSerializedPage>> inputs = ImmutableMap.builder();
            long totalNumberOfPagesReceived = 0;
            long totalCompressedSizeInBytes = 0;
            long totalUncompressedSizeInBytes = 0;
            for (Map.Entry<String, JavaFutureAction<List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>>>> inputFuture : inputFutures.entrySet()) {
                // Use a mutable list to allow memory release on per page basis
                List<PrestoSparkSerializedPage> pages = new ArrayList<>();
                List<Tuple2<MutablePartitionId, PrestoSparkSerializedPage>> tuples = getUnchecked(inputFuture.getValue());
                long currentFragmentOutputCompressedSizeInBytes = 0;
                long currentFragmentOutputUncompressedSizeInBytes = 0;
                for (Tuple2<MutablePartitionId, PrestoSparkSerializedPage> tuple : tuples) {
                    PrestoSparkSerializedPage page = tuple._2;
                    currentFragmentOutputCompressedSizeInBytes += page.getSize();
                    currentFragmentOutputUncompressedSizeInBytes += page.getUncompressedSizeInBytes();
                    pages.add(page);
                }
                log.info(
                        "Received %s pages from fragment %s. Compressed size: %s. Uncompressed size: %s.",
                        pages.size(),
                        inputFuture.getKey(),
                        DataSize.succinctBytes(currentFragmentOutputCompressedSizeInBytes),
                        DataSize.succinctBytes(currentFragmentOutputUncompressedSizeInBytes));
                totalNumberOfPagesReceived += pages.size();
                totalCompressedSizeInBytes += currentFragmentOutputCompressedSizeInBytes;
                totalUncompressedSizeInBytes += currentFragmentOutputUncompressedSizeInBytes;
                inputs.put(inputFuture.getKey(), pages);
            }

            log.info(
                    "Received %s pages in total. Compressed size: %s. Uncompressed size: %s.",
                    totalNumberOfPagesReceived,
                    DataSize.succinctBytes(totalCompressedSizeInBytes),
                    DataSize.succinctBytes(totalUncompressedSizeInBytes));

            IPrestoSparkTaskExecutor<PrestoSparkSerializedPage> prestoSparkTaskExecutor = taskExecutorFactory.create(
                    0,
                    0,
                    serializedTaskDescriptor,
                    emptyScalaIterator(),
                    new PrestoSparkTaskInputs(ImmutableMap.of(), ImmutableMap.of(), inputs.build()),
                    taskInfoCollector,
                    shuffleStatsCollector,
                    PrestoSparkSerializedPage.class);
            return collectScalaIterator(prestoSparkTaskExecutor);
        }

        RddAndMore<PrestoSparkSerializedPage> rootRdd = createRdd(rootFragmentedPlan, PrestoSparkSerializedPage.class, tableWriteInfo);
        return rootRdd.collectAndDestroyDependenciesWithTimeout(computeNextTimeout(queryCompletionDeadline), MILLISECONDS, waitTimeMetrics);
    }

    private SubPlan createFragmentedPlan()
    {
        SubPlan rootFragmentedPlan = planFragmenter.fragmentQueryPlan(session, planAndMore.getPlan(), warningCollector);
        queryMonitor.queryUpdatedEvent(
                createQueryInfo(
                        session,
                        query,
                        PLANNING,
                        Optional.of(planAndMore),
                        sparkQueueName,
                        Optional.empty(),
                        queryStateTimer,
                        Optional.of(createStageInfo(session.getQueryId(), rootFragmentedPlan, ImmutableList.of())),
                        warningCollector));

        log.info(textDistributedPlan(rootFragmentedPlan, metadata.getFunctionAndTypeManager(), session, true));
        int hashPartitionCount = getHashPartitionCount(sparkContext.sc(), session.getQueryId(), session, planAndMore);
        rootFragmentedPlan = configureOutputPartitioning(session, rootFragmentedPlan, hashPartitionCount);
        return rootFragmentedPlan;
    }
}
