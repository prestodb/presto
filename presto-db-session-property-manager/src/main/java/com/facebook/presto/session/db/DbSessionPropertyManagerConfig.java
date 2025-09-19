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
package com.facebook.presto.session.db;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import static java.util.concurrent.TimeUnit.SECONDS;

public class DbSessionPropertyManagerConfig
{
    private String configDbUrl;
    private Duration specsRefreshPeriod = new Duration(10, SECONDS);

    @NotNull
    public String getConfigDbUrl()
    {
        return configDbUrl;
    }

    @Config("session-property-manager.db.url")
    public DbSessionPropertyManagerConfig setConfigDbUrl(String configDbUrl)
    {
        this.configDbUrl = configDbUrl;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getSpecsRefreshPeriod()
    {
        return specsRefreshPeriod;
    }

    @Config("session-property-manager.db.refresh-period")
    public DbSessionPropertyManagerConfig setSpecsRefreshPeriod(Duration specsRefreshPeriod)
    {
        this.specsRefreshPeriod = specsRefreshPeriod;
        return this;
    }
}
