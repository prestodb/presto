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
package com.facebook.presto.dispatcher;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class DispatcherConfig
{
    private Duration remoteQueryInfoMaxErrorDuration = new Duration(5, TimeUnit.MINUTES);
    private Duration remoteQueryInfoRefreshMaxWait = new Duration(0, TimeUnit.SECONDS);
    private Duration remoteQueryInfoUpdateInterval = new Duration(3, TimeUnit.SECONDS);
    private int remoteQueryInfoMaxCallbackThreads = 1000;

    public Duration getRemoteQueryInfoMaxErrorDuration()
    {
        return remoteQueryInfoMaxErrorDuration;
    }

    @Config("dispatcher.remote-query-info.min-error-duration")
    public DispatcherConfig setRemoteQueryInfoMaxErrorDuration(Duration remoteQueryInfoMaxErrorDuration)
    {
        this.remoteQueryInfoMaxErrorDuration = remoteQueryInfoMaxErrorDuration;
        return this;
    }

    @NotNull
    public Duration getRemoteQueryInfoRefreshMaxWait()
    {
        return remoteQueryInfoRefreshMaxWait;
    }

    @Config("dispatcher.remote-query-info.refresh-max-wait")
    public DispatcherConfig setRemoteQueryInfoRefreshMaxWait(Duration remoteQueryInfoRefreshMaxWait)
    {
        this.remoteQueryInfoRefreshMaxWait = remoteQueryInfoRefreshMaxWait;
        return this;
    }

    @MinDuration("1ms")
    @MaxDuration("10s")
    @NotNull
    public Duration getRemoteQueryInfoUpdateInterval()
    {
        return remoteQueryInfoUpdateInterval;
    }

    @Config("dispatcher.remote-query-info.update-interval")
    @ConfigDescription("Interval between updating query data")
    public DispatcherConfig setRemoteQueryInfoUpdateInterval(Duration remoteQueryInfoUpdateInterval)
    {
        this.remoteQueryInfoUpdateInterval = remoteQueryInfoUpdateInterval;
        return this;
    }

    @Min(1)
    public int getRemoteQueryInfoMaxCallbackThreads()
    {
        return remoteQueryInfoMaxCallbackThreads;
    }

    @Config("dispatcher.remote-query.max-callback-threads")
    public DispatcherConfig setRemoteQueryInfoMaxCallbackThreads(int remoteQueryInfoMaxCallbackThreads)
    {
        this.remoteQueryInfoMaxCallbackThreads = remoteQueryInfoMaxCallbackThreads;
        return this;
    }
}
