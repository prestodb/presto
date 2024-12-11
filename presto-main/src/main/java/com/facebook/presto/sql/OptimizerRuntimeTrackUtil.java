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
package com.facebook.presto.sql;

import com.facebook.presto.Session;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.optimizations.StatsRecordingPlanOptimizer;
import com.google.common.base.Splitter;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.getOptimizersToEnableVerboseRuntimeStats;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class OptimizerRuntimeTrackUtil
{
    private OptimizerRuntimeTrackUtil()
    {
    }

    public static String getOptimizerNameForLog(Object optimizer)
    {
        checkArgument(optimizer instanceof PlanOptimizer || optimizer instanceof ConnectorPlanOptimizer);
        String optimizerName = optimizer.getClass().getSimpleName();
        if (optimizer instanceof StatsRecordingPlanOptimizer) {
            optimizerName = format("%s:%s", optimizerName, ((StatsRecordingPlanOptimizer) optimizer).getDelegate().getClass().getSimpleName());
        }
        return optimizerName;
    }

    public static boolean trackOptimizerRuntime(Session session, Object optimizer)
    {
        String optimizerString = getOptimizersToEnableVerboseRuntimeStats(session);
        if (optimizerString.isEmpty()) {
            return false;
        }
        List<String> optimizers = Splitter.on(",").trimResults().splitToList(optimizerString);
        return optimizers.contains(getOptimizerNameForLog(optimizer));
    }
}
