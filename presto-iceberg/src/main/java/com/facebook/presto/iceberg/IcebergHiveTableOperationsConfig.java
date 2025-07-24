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
package com.facebook.presto.iceberg;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.Duration;
import com.facebook.airlift.units.MinDuration;
import jakarta.validation.constraints.Min;

import static com.facebook.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class IcebergHiveTableOperationsConfig
{
    private Duration tableRefreshBackoffMinSleepTime = succinctDuration(100, MILLISECONDS);
    private Duration tableRefreshBackoffMaxSleepTime = succinctDuration(5, SECONDS);
    private Duration tableRefreshMaxRetryTime = succinctDuration(1, MINUTES);
    private double tableRefreshBackoffScaleFactor = 4.0;
    private int tableRefreshRetries = 20;
    private boolean lockingEnabled = true;

    @MinDuration("1ms")
    public Duration getTableRefreshBackoffMinSleepTime()
    {
        return tableRefreshBackoffMinSleepTime;
    }

    @Config("iceberg.hive.table-refresh.backoff-min-sleep-time")
    @ConfigDescription("The minimum amount of time to sleep between retries when refreshing table metadata")
    public IcebergHiveTableOperationsConfig setTableRefreshBackoffMinSleepTime(Duration tableRefreshBackoffMinSleepTime)
    {
        this.tableRefreshBackoffMinSleepTime = tableRefreshBackoffMinSleepTime;
        return this;
    }

    @MinDuration("1ms")
    public Duration getTableRefreshBackoffMaxSleepTime()
    {
        return tableRefreshBackoffMaxSleepTime;
    }

    @Config("iceberg.hive.table-refresh.backoff-max-sleep-time")
    @ConfigDescription("The maximum amount of time to sleep between retries when refreshing table metadata")
    public IcebergHiveTableOperationsConfig setTableRefreshBackoffMaxSleepTime(Duration tableRefreshBackoffMaxSleepTime)
    {
        this.tableRefreshBackoffMaxSleepTime = tableRefreshBackoffMaxSleepTime;
        return this;
    }

    @MinDuration("1ms")
    public Duration getTableRefreshMaxRetryTime()
    {
        return tableRefreshMaxRetryTime;
    }

    @Config("iceberg.hive.table-refresh.max-retry-time")
    @ConfigDescription("The maximum amount of time to take across all retries before failing a table metadata refresh operation")
    public IcebergHiveTableOperationsConfig setTableRefreshMaxRetryTime(Duration tableRefreshMaxRetryTime)
    {
        this.tableRefreshMaxRetryTime = tableRefreshMaxRetryTime;
        return this;
    }

    @Min(1)
    public double getTableRefreshBackoffScaleFactor()
    {
        return tableRefreshBackoffScaleFactor;
    }

    @Config("iceberg.hive.table-refresh.backoff-scale-factor")
    @ConfigDescription("The multiple used to scale subsequent wait time between retries")
    public IcebergHiveTableOperationsConfig setTableRefreshBackoffScaleFactor(double tableRefreshBackoffScaleFactor)
    {
        this.tableRefreshBackoffScaleFactor = tableRefreshBackoffScaleFactor;
        return this;
    }

    @Config("iceberg.hive.table-refresh.retries")
    @ConfigDescription("The number of times to retry after errors when refreshing table metadata using the Hive metastore")
    public IcebergHiveTableOperationsConfig setTableRefreshRetries(int tableRefreshRetries)
    {
        this.tableRefreshRetries = tableRefreshRetries;
        return this;
    }

    @Min(0)
    public int getTableRefreshRetries()
    {
        return tableRefreshRetries;
    }

    @Config("iceberg.engine.hive.lock-enabled")
    @ConfigDescription("Whether to use HMS locks to ensure atomicity of commits")
    public IcebergHiveTableOperationsConfig setLockingEnabled(boolean lockingEnabled)
    {
        this.lockingEnabled = lockingEnabled;
        return this;
    }

    public boolean getLockingEnabled()
    {
        return lockingEnabled;
    }
}
