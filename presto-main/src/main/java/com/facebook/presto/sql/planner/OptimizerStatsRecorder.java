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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.optimizations.OptimizerStats;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OptimizerStatsRecorder
{
    private final Map<Class<?>, OptimizerStats> stats = new HashMap<>();

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

    void export(MBeanExporter exporter)
    {
        for (Map.Entry<Class<?>, OptimizerStats> entry : stats.entrySet()) {
            verify(!entry.getKey().getSimpleName().isEmpty());
            try {
                exporter.export(getName(entry.getKey()), entry.getValue());
            }
            catch (RuntimeException e) {
                throw new RuntimeException(format("Failed to export MBean with name '%s'", getName(entry.getKey())), e);
            }
        }
    }

    void unexport(MBeanExporter exporter)
    {
        for (Class<?> rule : stats.keySet()) {
            exporter.unexport(getName(rule));
        }
    }

    private String getName(Class<?> key)
    {
        return ObjectNames.builder(PlanOptimizer.class)
                .withProperty("optimizer", key.getSimpleName())
                .build();
    }
}
