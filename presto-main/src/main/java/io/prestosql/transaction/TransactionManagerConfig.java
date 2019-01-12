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
package io.prestosql.transaction;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class TransactionManagerConfig
{
    private Duration idleCheckInterval = new Duration(1, TimeUnit.MINUTES);
    private Duration idleTimeout = new Duration(5, TimeUnit.MINUTES);
    private int maxFinishingConcurrency = 1;

    @MinDuration("1ms")
    @NotNull
    public Duration getIdleCheckInterval()
    {
        return idleCheckInterval;
    }

    @Config("transaction.idle-check-interval")
    @ConfigDescription("Time interval between idle transactions checks")
    public TransactionManagerConfig setIdleCheckInterval(Duration idleCheckInterval)
    {
        this.idleCheckInterval = idleCheckInterval;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getIdleTimeout()
    {
        return idleTimeout;
    }

    @Config("transaction.idle-timeout")
    @ConfigDescription("Amount of time before an inactive transaction is considered expired")
    public TransactionManagerConfig setIdleTimeout(Duration idleTimeout)
    {
        this.idleTimeout = idleTimeout;
        return this;
    }

    @Min(1)
    public int getMaxFinishingConcurrency()
    {
        return maxFinishingConcurrency;
    }

    @Config("transaction.max-finishing-concurrency")
    @ConfigDescription("Maximum parallelism for committing or aborting a transaction")
    public TransactionManagerConfig setMaxFinishingConcurrency(int maxFinishingConcurrency)
    {
        this.maxFinishingConcurrency = maxFinishingConcurrency;
        return this;
    }
}
