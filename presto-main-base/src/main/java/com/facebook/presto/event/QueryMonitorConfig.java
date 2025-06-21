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
package com.facebook.presto.event;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.DataSize.Unit;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MaxDataSize;
import com.facebook.airlift.units.MinDataSize;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.MINUTES;

public class QueryMonitorConfig
{
    private DataSize maxOutputStageJsonSize = new DataSize(16, Unit.MEGABYTE);
    private Duration queryProgressPublishInterval = new Duration(0, MINUTES);

    @MinDataSize("1kB")
    @MaxDataSize("1GB")
    @NotNull
    public DataSize getMaxOutputStageJsonSize()
    {
        return maxOutputStageJsonSize;
    }

    @Config("event.max-output-stage-size")
    public QueryMonitorConfig setMaxOutputStageJsonSize(DataSize maxOutputStageJsonSize)
    {
        this.maxOutputStageJsonSize = maxOutputStageJsonSize;
        return this;
    }

    public Duration getQueryProgressPublishInterval()
    {
        return queryProgressPublishInterval;
    }

    @Config("event.query-progress-publish-interval")
    @ConfigDescription("How frequently to publish query progress events. 0 duration disables the publication of these events.")
    public QueryMonitorConfig setQueryProgressPublishInterval(Duration queryProgressPublishInterval)
    {
        this.queryProgressPublishInterval = queryProgressPublishInterval;
        return this;
    }
}
