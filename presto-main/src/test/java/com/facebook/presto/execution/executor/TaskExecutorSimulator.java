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
package com.facebook.presto.execution.executor;

import com.facebook.presto.execution.executor.SimulationController.TaskSpecification;
import com.facebook.presto.execution.executor.SplitGenerators.AggregatedLeafSplitGenerator;
import com.facebook.presto.execution.executor.SplitGenerators.FastLeafSplitGenerator;
import com.facebook.presto.execution.executor.SplitGenerators.IntermediateSplitGenerator;
import com.facebook.presto.execution.executor.SplitGenerators.L4LeafSplitGenerator;
import com.facebook.presto.execution.executor.SplitGenerators.QuantaExceedingSplitGenerator;
import com.facebook.presto.execution.executor.SplitGenerators.SimpleLeafSplitGenerator;
import com.facebook.presto.execution.executor.SplitGenerators.SlowLeafSplitGenerator;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.presto.execution.TaskManagerConfig.TaskPriorityTracking.TASK_FAIR;
import static com.facebook.presto.execution.executor.Histogram.fromContinuous;
import static com.facebook.presto.execution.executor.Histogram.fromDiscrete;
import static com.facebook.presto.execution.executor.SimulationController.TaskSpecification.Type.INTERMEDIATE;
import static com.facebook.presto.execution.executor.SimulationController.TaskSpecification.Type.LEAF;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.units.Duration.nanosSince;
import static io.airlift.units.Duration.succinctNanos;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;

public class TaskExecutorSimulator
        implements Closeable
{
    public static void main(String[] args)
            throws Exception
    {
        try (TaskExecutorSimulator simulator = new TaskExecutorSimulator()) {
            simulator.run();
        }
    }

    private final ListeningExecutorService submissionExecutor = listeningDecorator(newCachedThreadPool(threadsNamed(getClass().getSimpleName() + "-%s")));
    private final ScheduledExecutorService overallStatusPrintExecutor = newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService runningSplitsPrintExecutor = newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService wakeupExecutor = newScheduledThreadPool(32);

    private final TaskExecutor taskExecutor;
    private final MultilevelSplitQueue splitQueue;

    private TaskExecutorSimulator()
    {
        splitQueue = new MultilevelSplitQueue(2);
        taskExecutor = new TaskExecutor(36, 72, 3, 8, TASK_FAIR, splitQueue, Ticker.systemTicker());
        taskExecutor.start();
    }

    @Override
    public void close()
    {
        submissionExecutor.shutdownNow();
        overallStatusPrintExecutor.shutdownNow();
        runningSplitsPrintExecutor.shutdownNow();
        wakeupExecutor.shutdownNow();
        taskExecutor.stop();
    }

    public void run()
            throws Exception
    {
        long start = System.nanoTime();
        scheduleStatusPrinter(start);

        SimulationController controller = new SimulationController(taskExecutor, TaskExecutorSimulator::printSummaryStats);

        // Uncomment one of these:
        // runExperimentOverloadedCluster(controller);
        // runExperimentMisbehavingQuanta(controller);
        // runExperimentStarveSlowSplits(controller);
        runExperimentWithinLevelFairness(controller);

        System.out.println("Stopped scheduling new tasks. Ending simulation..");
        controller.stop();
        close();

        SECONDS.sleep(5);

        System.out.println();
        System.out.println("Simulation finished at " + DateTime.now() + ". Runtime: " + nanosSince(start));
        System.out.println();

        printSummaryStats(controller, taskExecutor);
    }

    private void runExperimentOverloadedCluster(SimulationController controller)
            throws InterruptedException
    {
        /*
        Designed to simulate a somewhat overloaded Hive cluster.
        The following data is a point-in-time snapshot representative production cluster:
            - 60 running queries => 45 queries/node
            - 80 tasks/node
            - 600 splits scheduled/node (80% intermediate => ~480, 20% leaf => 120)
            - Only 60% intermediate splits will ever get data (~300)

        Desired result:
        This experiment should demonstrate the trade-offs that will be made during periods when a
        node is under heavy load. Ideally, the different classes of tasks should each accumulate
        scheduled time, and not spend disproportionately long waiting.
        */

        System.out.println("Overload experiment started.");
        TaskSpecification leafSpec = new TaskSpecification(LEAF, "leaf", OptionalInt.empty(), 16, 30, new AggregatedLeafSplitGenerator());
        controller.addTaskSpecification(leafSpec);

        TaskSpecification slowLeafSpec = new TaskSpecification(LEAF, "slow_leaf", OptionalInt.empty(), 16, 10, new SlowLeafSplitGenerator());
        controller.addTaskSpecification(slowLeafSpec);

        TaskSpecification intermediateSpec = new TaskSpecification(INTERMEDIATE, "intermediate", OptionalInt.empty(), 8, 40, new IntermediateSplitGenerator(wakeupExecutor));
        controller.addTaskSpecification(intermediateSpec);

        controller.enableSpecification(leafSpec);
        controller.enableSpecification(slowLeafSpec);
        controller.enableSpecification(intermediateSpec);
        controller.run();

        SECONDS.sleep(30);

        // this gets the executor into a more realistic point-in-time state, where long running tasks start to make progress
        for (int i = 0; i < 20; i++) {
            controller.clearPendingQueue();
            MINUTES.sleep(1);
        }

        System.out.println("Overload experiment completed.");
    }

    private void runExperimentStarveSlowSplits(SimulationController controller)
            throws InterruptedException
    {
        /*
        Designed to simulate how higher level admission control affects short-term scheduling decisions.
        A fixed, large number of tasks (120) are submitted at approximately the same time.

        Desired result:
        Presto is designed to prioritize fast, short tasks at the expense of longer slower tasks.
        This experiment allows us to quantify exactly how this preference manifests itself. It is
        expected that shorter tasks will complete faster, however, longer tasks should not starve
        for more than a couple of minutes at a time.
        */

        System.out.println("Starvation experiment started.");
        TaskSpecification slowLeafSpec = new TaskSpecification(LEAF, "slow_leaf", OptionalInt.of(600), 40, 4, new SlowLeafSplitGenerator());
        controller.addTaskSpecification(slowLeafSpec);

        TaskSpecification intermediateSpec = new TaskSpecification(INTERMEDIATE, "intermediate", OptionalInt.of(400), 40, 8, new IntermediateSplitGenerator(wakeupExecutor));
        controller.addTaskSpecification(intermediateSpec);

        TaskSpecification fastLeafSpec = new TaskSpecification(LEAF, "fast_leaf", OptionalInt.of(600), 40, 4, new FastLeafSplitGenerator());
        controller.addTaskSpecification(fastLeafSpec);

        controller.enableSpecification(slowLeafSpec);
        controller.enableSpecification(fastLeafSpec);
        controller.enableSpecification(intermediateSpec);

        controller.run();

        for (int i = 0; i < 60; i++) {
            SECONDS.sleep(20);
            controller.clearPendingQueue();
        }

        System.out.println("Starvation experiment completed.");
    }

    private void runExperimentMisbehavingQuanta(SimulationController controller)
            throws InterruptedException
    {
        /*
        Designed to simulate how Presto allocates resources in scenarios where there is variance in
        quanta run-time between tasks.

        Desired result:
        Variance in quanta run time should not affect total accrued scheduled time. It is
        acceptable, however, to penalize tasks that use extremely short quanta, as each quanta
        incurs scheduling overhead.
        */

        System.out.println("Misbehaving quanta experiment started.");

        TaskSpecification slowLeafSpec = new TaskSpecification(LEAF, "good_leaf", OptionalInt.empty(), 16, 4, new L4LeafSplitGenerator());
        controller.addTaskSpecification(slowLeafSpec);

        TaskSpecification misbehavingLeafSpec = new TaskSpecification(LEAF, "bad_leaf", OptionalInt.empty(), 16, 4, new QuantaExceedingSplitGenerator());
        controller.addTaskSpecification(misbehavingLeafSpec);

        controller.enableSpecification(slowLeafSpec);
        controller.enableSpecification(misbehavingLeafSpec);

        controller.run();

        for (int i = 0; i < 120; i++) {
            controller.clearPendingQueue();
            SECONDS.sleep(20);
        }

        System.out.println("Misbehaving quanta experiment completed.");
    }

    private void runExperimentWithinLevelFairness(SimulationController controller)
            throws InterruptedException
    {
        /*
        Designed to simulate how Presto allocates resources to tasks at the same level of the
        feedback queue when there is large variance in accrued scheduled time.

        Desired result:
        Scheduling within levels should be fair - total accrued time should not affect what
        fraction of resources tasks are allocated as long as they are in the same level.
        */

        System.out.println("Level fairness experiment started.");

        TaskSpecification longLeafSpec = new TaskSpecification(INTERMEDIATE, "l4_long", OptionalInt.empty(), 2, 16, new SimpleLeafSplitGenerator(MINUTES.toNanos(4), SECONDS.toNanos(1)));
        controller.addTaskSpecification(longLeafSpec);

        TaskSpecification shortLeafSpec = new TaskSpecification(INTERMEDIATE, "l4_short", OptionalInt.empty(), 2, 16, new SimpleLeafSplitGenerator(MINUTES.toNanos(2), SECONDS.toNanos(1)));
        controller.addTaskSpecification(shortLeafSpec);

        controller.enableSpecification(longLeafSpec);
        controller.run();

        // wait until long tasks are all well into L4
        MINUTES.sleep(1);
        controller.runCallback();

        // start short leaf tasks
        controller.enableSpecification(shortLeafSpec);

        // wait until short tasks hit L4
        SECONDS.sleep(25);
        controller.runCallback();

        // now watch for L4 fairness at this point
        MINUTES.sleep(2);

        System.out.println("Level fairness experiment completed.");
    }

    private void scheduleStatusPrinter(long start)
    {
        overallStatusPrintExecutor.scheduleAtFixedRate(() -> {
            try {
                System.out.printf(
                        "%6s -- %4s splits (R: %2s  L: %3s  I: %3s  B: %3s  W: %3s  C: %5s)  |  %3s tasks (%3s %3s %3s %3s %3s)  |  Selections: %4s %4s %4s %4s %3s\n",
                        nanosSince(start),
                        taskExecutor.getTotalSplits(),
                        taskExecutor.getRunningSplits(),
                        taskExecutor.getTotalSplits() - taskExecutor.getIntermediateSplits(),
                        taskExecutor.getIntermediateSplits(),
                        taskExecutor.getBlockedSplits(),
                        taskExecutor.getWaitingSplits(),
                        taskExecutor.getCompletedSplitsLevel0() + taskExecutor.getCompletedSplitsLevel1() + taskExecutor.getCompletedSplitsLevel2() + taskExecutor.getCompletedSplitsLevel3() + taskExecutor.getCompletedSplitsLevel4(),
                        taskExecutor.getTasks(),
                        taskExecutor.getRunningTasksLevel0(),
                        taskExecutor.getRunningTasksLevel1(),
                        taskExecutor.getRunningTasksLevel2(),
                        taskExecutor.getRunningTasksLevel3(),
                        taskExecutor.getRunningTasksLevel4(),
                        (int) splitQueue.getSelectedCountLevel0().getOneMinute().getRate(),
                        (int) splitQueue.getSelectedCountLevel1().getOneMinute().getRate(),
                        (int) splitQueue.getSelectedCountLevel2().getOneMinute().getRate(),
                        (int) splitQueue.getSelectedCountLevel3().getOneMinute().getRate(),
                        (int) splitQueue.getSelectedCountLevel4().getOneMinute().getRate());
            }
            catch (Exception ignored) {
            }
        }, 1, 1, SECONDS);
    }

    private static void printSummaryStats(SimulationController controller, TaskExecutor taskExecutor)
    {
        Map<TaskSpecification, Boolean> specEnabled = controller.getSpecificationEnabled();

        ListMultimap<TaskSpecification, SimulationTask> completedTasks = controller.getCompletedTasks();
        ListMultimap<TaskSpecification, SimulationTask> runningTasks = controller.getRunningTasks();
        Set<SimulationTask> allTasks = ImmutableSet.<SimulationTask>builder().addAll(completedTasks.values()).addAll(runningTasks.values()).build();

        long completedSplits = completedTasks.values().stream().mapToInt(t -> t.getCompletedSplits().size()).sum();
        long runningSplits = runningTasks.values().stream().mapToInt(t -> t.getCompletedSplits().size()).sum();

        System.out.println("Completed tasks : " + completedTasks.size());
        System.out.println("Remaining tasks : " + runningTasks.size());
        System.out.println("Completed splits: " + completedSplits);
        System.out.println("Remaining splits: " + runningSplits);
        System.out.println();
        System.out.println("Completed tasks  L0: " + taskExecutor.getCompletedTasksLevel0());
        System.out.println("Completed tasks  L1: " + taskExecutor.getCompletedTasksLevel1());
        System.out.println("Completed tasks  L2: " + taskExecutor.getCompletedTasksLevel2());
        System.out.println("Completed tasks  L3: " + taskExecutor.getCompletedTasksLevel3());
        System.out.println("Completed tasks  L4: " + taskExecutor.getCompletedTasksLevel4());
        System.out.println();
        System.out.println("Completed splits L0: " + taskExecutor.getCompletedSplitsLevel0());
        System.out.println("Completed splits L1: " + taskExecutor.getCompletedSplitsLevel1());
        System.out.println("Completed splits L2: " + taskExecutor.getCompletedSplitsLevel2());
        System.out.println("Completed splits L3: " + taskExecutor.getCompletedSplitsLevel3());
        System.out.println("Completed splits L4: " + taskExecutor.getCompletedSplitsLevel4());

        Histogram<Long> levelsHistogram = fromContinuous(ImmutableList.of(
                MILLISECONDS.toNanos(0L),
                MILLISECONDS.toNanos(1_000),
                MILLISECONDS.toNanos(10_000L),
                MILLISECONDS.toNanos(60_000L),
                MILLISECONDS.toNanos(300_000L),
                HOURS.toNanos(1),
                DAYS.toNanos(1)));

        System.out.println();
        System.out.println("Levels - Completed Task Processed Time");
        levelsHistogram.printDistribution(
                completedTasks.values().stream().filter(t -> t.getSpecification().getType() == LEAF).collect(Collectors.toList()),
                SimulationTask::getScheduledTimeNanos,
                SimulationTask::getProcessedTimeNanos,
                Duration::succinctNanos,
                TaskExecutorSimulator::formatNanos);

        System.out.println();
        System.out.println("Levels - Running Task Processed Time");
        levelsHistogram.printDistribution(
                runningTasks.values().stream().filter(t -> t.getSpecification().getType() == LEAF).collect(Collectors.toList()),
                SimulationTask::getScheduledTimeNanos,
                SimulationTask::getProcessedTimeNanos,
                Duration::succinctNanos,
                TaskExecutorSimulator::formatNanos);

        System.out.println();
        System.out.println("Levels - All Task Wait Time");
        levelsHistogram.printDistribution(
                runningTasks.values().stream().filter(t -> t.getSpecification().getType() == LEAF).collect(Collectors.toList()),
                SimulationTask::getScheduledTimeNanos,
                SimulationTask::getTotalWaitTimeNanos,
                Duration::succinctNanos,
                TaskExecutorSimulator::formatNanos);

        System.out.println();
        System.out.println("Specification - Processed time");
        Set<String> specifications = runningTasks.values().stream().map(t -> t.getSpecification().getName()).collect(Collectors.toSet());
        fromDiscrete(specifications).printDistribution(
                allTasks,
                t -> t.getSpecification().getName(),
                SimulationTask::getProcessedTimeNanos,
                identity(),
                TaskExecutorSimulator::formatNanos);

        System.out.println();
        System.out.println("Specification - Wait time");
        fromDiscrete(specifications).printDistribution(
                allTasks,
                t -> t.getSpecification().getName(),
                SimulationTask::getTotalWaitTimeNanos,
                identity(),
                TaskExecutorSimulator::formatNanos);

        System.out.println();
        System.out.println("Breakdown by specification");
        System.out.println("##########################");
        for (TaskSpecification specification : specEnabled.keySet()) {
            List<SimulationTask> allSpecificationTasks = ImmutableList.<SimulationTask>builder()
                    .addAll(completedTasks.get(specification))
                    .addAll(runningTasks.get(specification))
                    .build();

            System.out.println(specification.getName());
            System.out.println("=============================");
            System.out.println("Completed tasks           : " + completedTasks.get(specification).size());
            System.out.println("In-progress tasks         : " + runningTasks.get(specification).size());
            System.out.println("Total tasks               : " + specification.getTotalTasks());
            System.out.println("Splits/task               : " + specification.getNumSplitsPerTask());
            System.out.println("Current required time     : " + succinctNanos(allSpecificationTasks.stream().mapToLong(SimulationTask::getScheduledTimeNanos).sum()));
            System.out.println("Completed scheduled time  : " + succinctNanos(allSpecificationTasks.stream().mapToLong(SimulationTask::getProcessedTimeNanos).sum()));
            System.out.println("Total wait time           : " + succinctNanos(allSpecificationTasks.stream().mapToLong(SimulationTask::getTotalWaitTimeNanos).sum()));

            System.out.println();
            System.out.println("All Tasks by Scheduled time - Processed Time");
            levelsHistogram.printDistribution(
                    allSpecificationTasks,
                    SimulationTask::getScheduledTimeNanos,
                    SimulationTask::getProcessedTimeNanos,
                    Duration::succinctNanos,
                    TaskExecutorSimulator::formatNanos);

            System.out.println();
            System.out.println("All Tasks by Scheduled time - Wait Time");
            levelsHistogram.printDistribution(
                    allSpecificationTasks,
                    SimulationTask::getScheduledTimeNanos,
                    SimulationTask::getTotalWaitTimeNanos,
                    Duration::succinctNanos,
                    TaskExecutorSimulator::formatNanos);

            System.out.println();
            System.out.println("Complete Tasks by Scheduled time - Wait Time");
            levelsHistogram.printDistribution(
                    completedTasks.get(specification),
                    SimulationTask::getScheduledTimeNanos,
                    SimulationTask::getTotalWaitTimeNanos,
                    Duration::succinctNanos,
                    TaskExecutorSimulator::formatNanos);
        }
    }

    private static String formatNanos(List<Long> list)
    {
        LongSummaryStatistics stats = list.stream().mapToLong(Long::new).summaryStatistics();
        return String.format("Min: %8s  Max: %8s  Avg: %8s  Sum: %8s",
                succinctNanos(stats.getMin() == Long.MAX_VALUE ? 0 : stats.getMin()),
                succinctNanos(stats.getMax() == Long.MIN_VALUE ? 0 : stats.getMax()),
                succinctNanos((long) stats.getAverage()),
                succinctNanos(stats.getSum()));
    }
}
