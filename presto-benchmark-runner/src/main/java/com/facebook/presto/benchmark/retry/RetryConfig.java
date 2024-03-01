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
package com.facebook.presto.benchmark.retry;

import com.facebook.airlift.configuration.Config;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class RetryConfig
{
    private int maxAttempts = 1;
    private Duration minBackoffDelay = new Duration(0, NANOSECONDS);
    private Duration maxBackoffDelay = new Duration(1, MINUTES);
    private double scaleFactor = 1.0;

    @Min(1)
    public int getMaxAttempts()
    {
        return maxAttempts;
    }

    @Config("max-attempts")
    public RetryConfig setMaxAttempts(int maxAttempts)
    {
        this.maxAttempts = maxAttempts;
        return this;
    }

    @MinDuration("0ns")
    public Duration getMinBackoffDelay()
    {
        return minBackoffDelay;
    }

    @Config("min-backoff-delay")
    public RetryConfig setMinBackoffDelay(Duration minBackoffDelay)
    {
        this.minBackoffDelay = minBackoffDelay;
        return this;
    }

    @MinDuration("0ns")
    public Duration getMaxBackoffDelay()
    {
        return maxBackoffDelay;
    }

    @Config("max-backoff-delay")
    public RetryConfig setMaxBackoffDelay(Duration maxBackoffDelay)
    {
        this.maxBackoffDelay = maxBackoffDelay;
        return this;
    }

    @Min(1)
    public double getScaleFactor()
    {
        return scaleFactor;
    }

    @Config("backoff-scale-factor")
    public RetryConfig setScaleFactor(double scaleFactor)
    {
        this.scaleFactor = scaleFactor;
        return this;
    }
}
