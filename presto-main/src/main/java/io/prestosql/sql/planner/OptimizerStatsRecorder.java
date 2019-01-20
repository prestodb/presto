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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.optimizations.OptimizerStats;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import org.weakref.jmx.MBeanExport;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OptimizerStatsRecorder
{
    private final Map<Class<?>, OptimizerStats> stats = new HashMap<>();

    @GuardedBy("this")
    private final List<MBeanExport> mbeanExports = new ArrayList<>();

    public void register(PlanOptimizer optimizer)
    {
        requireNonNull(optimizer, "optimizer is null");
        checkArgument(!optimizer.getClass().isAnonymousClass());
        stats.put(optimizer.getClass(), new OptimizerStats());
    }

    public void record(PlanOptimizer optimizer, long nanos)
    {
        requireNonNull(optimizer, "optimizer is null");
        OptimizerStats optimizerStats = requireNonNull(stats.get(optimizer.getClass()), "optimizer is not registered");
        optimizerStats.record(nanos);
    }

    public void recordFailure(PlanOptimizer optimizer)
    {
        requireNonNull(optimizer, "optimizer is null");
        OptimizerStats optimizerStats = requireNonNull(stats.get(optimizer.getClass()), "optimizer is not registered");
        optimizerStats.recordFailure();
    }

    synchronized void export(MBeanExporter exporter)
    {
        checkState(mbeanExports.isEmpty(), "MBeans already exported");
        for (Map.Entry<Class<?>, OptimizerStats> entry : stats.entrySet()) {
            verify(!entry.getKey().getSimpleName().isEmpty());
            try {
                mbeanExports.add(exporter.exportWithGeneratedName(entry.getValue(), PlanOptimizer.class, ImmutableMap.<String, String>builder()
                        .put("name", PlanOptimizer.class.getSimpleName())
                        .put("optimizer", entry.getKey().getSimpleName())
                        .build()));
            }
            catch (RuntimeException e) {
                throw new RuntimeException(format("Failed to export MBean with name '%s'", getName(entry.getKey())), e);
            }
        }
    }

    synchronized void unexport(MBeanExporter exporter)
    {
        for (MBeanExport mbeanExport : mbeanExports) {
            mbeanExport.unexport();
        }
        mbeanExports.clear();
    }

    private String getName(Class<?> key)
    {
        return ObjectNames.builder(PlanOptimizer.class)
                .withProperty("optimizer", key.getSimpleName())
                .build();
    }
}
