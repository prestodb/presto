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
package com.facebook.presto.spark.planner;

import com.facebook.presto.Session;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.spark.PrestoSparkQueryRunner;
import com.facebook.presto.spark.classloader_interface.MutablePartitionId;
import com.facebook.presto.spark.classloader_interface.PrestoSparkMutableRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionModule;
import com.facebook.presto.spark.execution.property.NativeExecutionConfigModule;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spiller.LocalTempStorage;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitionFunction.HASH;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SystemPartitioning.FIXED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.createSystemPartitioning;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.createTempDirectory;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestPrestoSparkRddFactory
{
    private PrestoSparkQueryRunner queryRunner;
    private JavaSparkContext sparkContext;
    private LocalTempStorage storage;
    private Path temporaryDirectory;

    @BeforeClass
    public void setUp()
            throws IOException
    {
        temporaryDirectory = createTempDirectory("test-spark-rdd-factory");
        queryRunner = new PrestoSparkQueryRunner(
                "tpch",
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of("spark.ui.enabled", "false"),
                Optional.of(temporaryDirectory),
                ImmutableList.of(
                        new NativeExecutionModule(),
                        new NativeExecutionConfigModule(ImmutableMap.of(), ImmutableMap.of())),
                4);
        sparkContext = JavaSparkContext.fromSparkContext(queryRunner.getSparkContext());
        storage = new LocalTempStorage(ImmutableList.of(temporaryDirectory.resolve("storage")), 1.0);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        try {
            if (queryRunner != null) {
                queryRunner.close();
            }
        }
        finally {
            sparkContext = null;
            queryRunner = null;
            if (temporaryDirectory != null) {
                deleteRecursively(temporaryDirectory, ALLOW_INSECURE);
            }
        }
    }

    @DataProvider
    public Object[][] countedHashPartitions()
    {
        return new Object[][] {{1}, {2}, {8}, {64}};
    }

    @Test(dataProvider = "countedHashPartitions")
    public void testConstructsRddForCountedHashFragment(int partitionCount)
    {
        PartitioningHandle partitioning = createSystemPartitioning(FIXED, HASH, partitionCount);
        PlanFragmentId inputFragmentId = new PlanFragmentId(0);
        VariableReferenceExpression input = new VariableReferenceExpression(Optional.empty(), "input", BIGINT);
        RemoteSourceNode remoteSource = new RemoteSourceNode(Optional.empty(), new PlanNodeId("source"), inputFragmentId,
                ImmutableList.of(input), false, Optional.empty(), ExchangeNode.Type.REPARTITION);
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId(1),
                remoteSource,
                ImmutableSet.of(input),
                partitioning,
                ImmutableList.of(),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(input)),
                Optional.empty(),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.empty(),
                Optional.empty());
        Session session = Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty("native_execution_enabled", "true")
                .build();
        JavaPairRDD<MutablePartitionId, PrestoSparkMutableRow> inputRdd = sparkContext.parallelizePairs(
                ImmutableList.<Tuple2<MutablePartitionId, PrestoSparkMutableRow>>of(), partitionCount);
        JavaPairRDD<MutablePartitionId, PrestoSparkSerializedPage> outputRdd = queryRunner.getRddFactory().createSparkRdd(
                sparkContext,
                session,
                fragment,
                ImmutableMap.of(inputFragmentId, inputRdd),
                ImmutableMap.of(),
                queryRunner.getTaskExecutorFactoryProvider(),
                sparkContext.sc().collectionAccumulator("task-info"),
                sparkContext.sc().collectionAccumulator("shuffle-stats"),
                new TableWriteInfo(Optional.empty(), Optional.empty()),
                PrestoSparkSerializedPage.class,
                storage);
        assertEquals(outputRdd.getNumPartitions(), partitionCount);
    }
}
