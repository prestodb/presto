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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.connector.system.GlobalSystemConnector;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import static com.facebook.presto.SystemSessionProperties.getExchangeMaterializationStrategy;
import static com.facebook.presto.SystemSessionProperties.getPartitioningProviderCatalog;
import static com.facebook.presto.SystemSessionProperties.isDistributedSortEnabled;
import static com.facebook.presto.SystemSessionProperties.isForceSingleNodeOutput;
import static com.facebook.presto.SystemSessionProperties.isGroupedExecutionEnabled;
import static com.facebook.presto.SystemSessionProperties.isRecoverableGroupedExecutionEnabled;
import static com.facebook.presto.SystemSessionProperties.isRedistributeWrites;
import static com.facebook.presto.SystemSessionProperties.isScaleWriters;
import static com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy.NONE;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public class PrestoSparkSettingsRequirements
{
    public static final String SPARK_TASK_CPUS_PROPERTY = "spark.task.cpus";
    public static final String SPARK_EXECUTOR_CORES_PROPERTY = "spark.executor.cores";

    public void verify(SparkContext sparkContext, Session session)
    {
        // verify Presto configuration
        verify(!isDistributedSortEnabled(session), "distributed sort is not supported");
        verify(getExchangeMaterializationStrategy(session) == NONE, "exchange materialization is not supported");
        verify(getPartitioningProviderCatalog(session).equals(GlobalSystemConnector.NAME), "partitioning provider other that system is not supported");
        verify(!isRecoverableGroupedExecutionEnabled(session) && !isGroupedExecutionEnabled(session), "grouped execution is not supported");
        verify(!isRedistributeWrites(session), "redistribute writes is not supported");
        verify(!isScaleWriters(session), "scale writes is not supported");
        verify(!isForceSingleNodeOutput(session), "force single node output is expected to be disabled");

        // verify Spark configuration
        verifyExecutorConfiguration(sparkContext.conf());
    }

    private static void verifyExecutorConfiguration(SparkConf sparkConf)
    {
        String taskCpusString = sparkConf.get(SPARK_TASK_CPUS_PROPERTY, null);
        verify(taskCpusString != null, "%s must be set", SPARK_TASK_CPUS_PROPERTY);
        String executorCoresString = sparkConf.get(SPARK_EXECUTOR_CORES_PROPERTY, null);
        verify(executorCoresString != null, "%s must be set", SPARK_EXECUTOR_CORES_PROPERTY);
        int taskCpus = parseInt(taskCpusString);
        int executorCores = parseInt(executorCoresString);
        verify(
                taskCpus == executorCores,
                "%s (%s) must be equal to %s (%s)",
                SPARK_TASK_CPUS_PROPERTY,
                taskCpus,
                SPARK_EXECUTOR_CORES_PROPERTY,
                executorCores);
    }

    private static void verify(boolean condition, String message, Object... args)
    {
        if (!condition) {
            throw new PrestoException(NOT_SUPPORTED, format(message, args));
        }
    }

    public static void setDefaults(FeaturesConfig config)
    {
        config.setDistributedSortEnabled(false);
        config.setGroupedExecutionEnabled(false);
        config.setRecoverableGroupedExecutionEnabled(false);
        config.setColocatedJoinsEnabled(true);
        config.setRedistributeWrites(false);
        config.setScaleWriters(false);
        config.setPreferDistributedUnion(true);
        config.setForceSingleNodeOutput(false);
        config.setInlineSqlFunctions(true);
        config.setEnforceFixedDistributionForOutputOperator(true);
        config.setPrestoSparkAssignBucketToPartitionForPartitionedTableWriteEnabled(true);
    }

    public static void setDefaults(QueryManagerConfig config)
    {
        config.setExchangeMaterializationStrategy(NONE);
        config.setPartitioningProviderCatalog(GlobalSystemConnector.NAME);
    }
}
