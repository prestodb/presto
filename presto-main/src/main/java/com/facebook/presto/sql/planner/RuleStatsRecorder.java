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

import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.RuleStats;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

public class RuleStatsRecorder
{
    private final Map<Class<?>, RuleStats> stats = new HashMap<>();

    public void registerAll(Collection<Rule<?>> rules)
    {
        for (Rule<?> rule : rules) {
            checkArgument(!rule.getClass().isAnonymousClass());
            stats.put(rule.getClass(), new RuleStats());
        }
    }

    public void record(Rule<?> rule, long nanos, boolean match)
    {
        stats.get(rule.getClass()).record(nanos, match);
    }

    public void recordFailure(Rule<?> rule)
    {
        stats.get(rule.getClass()).recordFailure();
    }

    void export(MBeanExporter exporter)
    {
        for (Map.Entry<Class<?>, RuleStats> entry : stats.entrySet()) {
            verify(!entry.getKey().getSimpleName().isEmpty());
            String name = ObjectNames.builder(IterativeOptimizer.class)
                    .withProperty("rule", entry.getKey().getSimpleName())
                    .build();

            try {
                exporter.export(name, entry.getValue());
            }
            catch (RuntimeException e) {
                throw new RuntimeException(format("Failed to export MBean with name '%s'", name), e);
            }
        }
    }

    void unexport(MBeanExporter exporter)
    {
        for (Class<?> rule : stats.keySet()) {
            String name = ObjectNames.builder(IterativeOptimizer.class)
                    .withProperty("rule", rule.getSimpleName())
                    .build();

            exporter.unexport(name);
        }
    }
}
