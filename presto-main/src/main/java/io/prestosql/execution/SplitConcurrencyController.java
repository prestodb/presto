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
package io.prestosql.execution;

import io.airlift.units.Duration;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.isFinite;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@NotThreadSafe
public class SplitConcurrencyController
{
    private static final double TARGET_UTILIZATION = 0.5;

    private final long adjustmentIntervalNanos;
    private int targetConcurrency;
    private long threadNanosSinceLastAdjustment;

    public SplitConcurrencyController(int initialConcurrency, Duration adjustmentInterval)
    {
        checkArgument(initialConcurrency > 0, "initial concurrency must be positive");
        this.targetConcurrency = initialConcurrency;
        this.adjustmentIntervalNanos = adjustmentInterval.roundTo(NANOSECONDS);
    }

    public void update(long nanos, double utilization, int currentConcurrency)
    {
        checkArgument(nanos >= 0, "nanos is negative");
        checkArgument(isFinite(utilization), "utilization must be finite");
        checkArgument(utilization >= 0, "utilization is negative");
        checkArgument(currentConcurrency >= 0, "currentConcurrency is negative");

        threadNanosSinceLastAdjustment += nanos;
        if (threadNanosSinceLastAdjustment >= adjustmentIntervalNanos && utilization < TARGET_UTILIZATION && currentConcurrency >= targetConcurrency) {
            threadNanosSinceLastAdjustment = 0;
            targetConcurrency++;
        }
    }

    public int getTargetConcurrency()
    {
        checkState(targetConcurrency > 0, "Possible deadlock detected. Target concurrency is zero");
        return targetConcurrency;
    }

    public void splitFinished(long splitThreadNanos, double utilization, int currentConcurrency)
    {
        checkArgument(splitThreadNanos >= 0, "nanos is negative");
        checkArgument(isFinite(utilization), "utilization must be finite");
        checkArgument(utilization >= 0, "utilization is negative");
        checkArgument(currentConcurrency >= 0, "currentConcurrency is negative");

        if (threadNanosSinceLastAdjustment >= adjustmentIntervalNanos || threadNanosSinceLastAdjustment >= splitThreadNanos) {
            if (utilization > TARGET_UTILIZATION && targetConcurrency > 1) {
                threadNanosSinceLastAdjustment = 0;
                targetConcurrency--;
            }
            else if (utilization < TARGET_UTILIZATION && currentConcurrency >= targetConcurrency) {
                threadNanosSinceLastAdjustment = 0;
                targetConcurrency++;
            }
        }
    }
}
