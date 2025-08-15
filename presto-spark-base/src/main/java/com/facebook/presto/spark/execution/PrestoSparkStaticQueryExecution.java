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
import com.facebook.presto.cost.HistoryBasedPlanStatisticsTracker;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.QueryStateTimer;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spark.ErrorClassifier;
import com.facebook.presto.spark.PrestoSparkMetadataStorage;
import com.facebook.presto.spark.PrestoSparkQueryData;
import com.facebook.presto.spark.PrestoSparkQueryStatusInfo;
import com.facebook.presto.spark.PrestoSparkServiceWaitTimeMetrics;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.RddAndMore;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.PrestoSparkShuffleStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.execution.task.PrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.planner.PrestoSparkPlanFragmenter;
import com.facebook.presto.spark.planner.PrestoSparkQueryPlanner.PlanAndMore;
import com.facebook.presto.spark.planner.PrestoSparkRddFactory;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.execution.QueryState.PLANNING;
import static com.facebook.presto.spark.PrestoSparkQueryExecutionFactory.createQueryInfo;
import static com.facebook.presto.spark.PrestoSparkQueryExecutionFactory.createStageInfo;
import static com.facebook.presto.spark.util.PrestoSparkUtils.computeNextTimeout;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textDistributedPlan;
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
            FeaturesConfig featuresConfig,
            QueryManagerConfig queryManagerConfig,
            Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics,
            Optional<ErrorClassifier> errorClassifier,
            PrestoSparkPlanFragmenter planFragmenter,
            Metadata metadata,
            PartitioningProviderManager partitioningProviderManager,
            HistoryBasedPlanStatisticsTracker historyBasedPlanStatisticsTracker,
            Optional<CollectionAccumulator<Map<String, Long>>> bootstrapMetricsCollector)
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
                featuresConfig,
                queryManagerConfig,
                waitTimeMetrics,
                errorClassifier,
                planFragmenter,
                metadata,
                partitioningProviderManager,
                historyBasedPlanStatisticsTracker,
                bootstrapMetricsCollector);
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
            Map<PlanFragmentId, RddAndMore<PrestoSparkSerializedPage>> inputRdds = new HashMap<>();
            for (SubPlan child : rootFragmentedPlan.getChildren()) {
                inputRdds.put(child.getFragment().getId(), createRdd(child, PrestoSparkSerializedPage.class, tableWriteInfo));
            }
            return collectPages(tableWriteInfo, rootFragment, inputRdds);
        }

        RddAndMore<PrestoSparkSerializedPage> rootRdd = createRdd(rootFragmentedPlan, PrestoSparkSerializedPage.class, tableWriteInfo);
        return rootRdd.collectAndDestroyDependenciesWithTimeout(computeNextTimeout(queryCompletionDeadline), MILLISECONDS, waitTimeMetrics);
    }

    @VisibleForTesting
    public SubPlan createFragmentedPlan()
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
        int hashPartitionCount = planAndMore.getPhysicalResourceSettings().getHashPartitionCount();
        rootFragmentedPlan = configureOutputPartitioning(session, rootFragmentedPlan, hashPartitionCount);
        return rootFragmentedPlan;
    }
}
