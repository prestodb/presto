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
package com.facebook.presto.server.protocol;

import com.facebook.airlift.stats.DecayCounter;
import com.facebook.presto.execution.QueryManagerConfig;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RetryCircuitBreaker
{
    private final int retryLimit;
    private final DecayCounter counter;

    @Inject
    public RetryCircuitBreaker(QueryManagerConfig queryManagerConfig)
    {
        requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.retryLimit = queryManagerConfig.getGlobalQueryRetryFailureLimit();
        Duration globalRetryWindow = queryManagerConfig.getGlobalQueryRetryFailureWindow();
        checkArgument(requireNonNull(globalRetryWindow).toMillis() >= 1_000, "retry window should be at least 1 second");
        this.counter = new DecayCounter(1.0 / globalRetryWindow.roundTo(SECONDS));
    }

    public void incrementFailure()
    {
        counter.add(1);
    }

    public boolean isRetryAllowed()
    {
        return counter.getCount() < retryLimit;
    }

    @Managed
    public double getRetryCount()
    {
        return counter.getCount();
    }
}
