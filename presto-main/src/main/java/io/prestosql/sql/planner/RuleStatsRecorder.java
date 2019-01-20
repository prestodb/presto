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
import io.prestosql.sql.planner.iterative.IterativeOptimizer;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.iterative.RuleStats;
import org.weakref.jmx.MBeanExport;
import org.weakref.jmx.MBeanExporter;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

public class RuleStatsRecorder
{
    private final Map<Class<?>, RuleStats> stats = new HashMap<>();

    @GuardedBy("this")
    private final List<MBeanExport> mbeanExports = new ArrayList<>();

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

    synchronized void export(MBeanExporter exporter)
    {
        checkState(mbeanExports.isEmpty(), "MBeans already exported");
        for (Map.Entry<Class<?>, RuleStats> entry : stats.entrySet()) {
            verify(!entry.getKey().getSimpleName().isEmpty());
            try {
                mbeanExports.add(exporter.exportWithGeneratedName(entry.getValue(), IterativeOptimizer.class, ImmutableMap.<String, String>builder()
                        .put("name", IterativeOptimizer.class.getSimpleName())
                        .put("rule", entry.getKey().getSimpleName())
                        .build()));
            }
            catch (RuntimeException e) {
                throw new RuntimeException(format("Failed to export MBean with for rule '%s'", entry.getKey().getSimpleName()), e);
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
}
