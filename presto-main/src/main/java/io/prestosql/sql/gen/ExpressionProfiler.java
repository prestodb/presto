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
package io.prestosql.sql.gen;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import io.airlift.units.Duration;

import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.base.Verify.verify;
import static io.prestosql.execution.executor.PrioritizedSplitRunner.SPLIT_RUN_QUANTA;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ExpressionProfiler
{
    private static final Duration EXPENSIVE_EXPRESSION_THRESHOLD = SPLIT_RUN_QUANTA;
    private static final int NOT_INITALIZED = -1;

    private final Ticker ticker;
    private final double expensiveExpressionThresholdNanos;
    private double totalExecutionTimeNanos;
    private int samples;
    private long previousTimestamp = NOT_INITALIZED;
    private boolean isExpressionExpensive = true;

    public ExpressionProfiler()
    {
        this(systemTicker(), EXPENSIVE_EXPRESSION_THRESHOLD);
    }

    @VisibleForTesting
    public ExpressionProfiler(Ticker ticker, Duration expensiveExpressionThreshold)
    {
        requireNonNull(ticker, "ticker is null");
        requireNonNull(expensiveExpressionThreshold, "expensiveExpressionThreshold is null");
        this.expensiveExpressionThresholdNanos = expensiveExpressionThreshold.getValue(NANOSECONDS);
        this.ticker = ticker;
    }

    public void start()
    {
        previousTimestamp = ticker.read();
    }

    public void stop(int batchSize)
    {
        verify(previousTimestamp != NOT_INITALIZED, "start() is not called");
        verify(batchSize > 0, "batchSize must be positive");

        long now = ticker.read();
        long delta = now - previousTimestamp;
        totalExecutionTimeNanos += delta;
        samples += batchSize;
        if ((totalExecutionTimeNanos / samples) < expensiveExpressionThresholdNanos) {
            isExpressionExpensive = false;
        }
        previousTimestamp = NOT_INITALIZED;
    }

    public boolean isExpressionExpensive()
    {
        return isExpressionExpensive;
    }
}
