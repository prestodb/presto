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
package com.facebook.presto.iceberg.hive;

import com.facebook.presto.iceberg.IcebergHiveTableOperationsConfig;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestHiveTableOperationsConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(IcebergHiveTableOperationsConfig.class)
                .setTableRefreshBackoffMinSleepTime(succinctDuration(100, MILLISECONDS))
                .setTableRefreshBackoffMaxSleepTime(succinctDuration(5, SECONDS))
                .setTableRefreshMaxRetryTime(succinctDuration(1, MINUTES))
                .setTableRefreshBackoffScaleFactor(4.0)
                .setTableRefreshRetries(20)
                .setLockingEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.hive.table-refresh.backoff-min-sleep-time", "10s")
                .put("iceberg.hive.table-refresh.backoff-max-sleep-time", "20s")
                .put("iceberg.hive.table-refresh.max-retry-time", "30s")
                .put("iceberg.hive.table-refresh.retries", "42")
                .put("iceberg.hive.table-refresh.backoff-scale-factor", "2.0")
                .put("iceberg.engine.hive.lock-enabled", "false")
                .build();

        IcebergHiveTableOperationsConfig expected = new IcebergHiveTableOperationsConfig()
                .setTableRefreshBackoffMinSleepTime(succinctDuration(10, SECONDS))
                .setTableRefreshBackoffMaxSleepTime(succinctDuration(20, SECONDS))
                .setTableRefreshMaxRetryTime(succinctDuration(30, SECONDS))
                .setTableRefreshBackoffScaleFactor(2.0)
                .setTableRefreshRetries(42)
                .setLockingEnabled(false);

        assertFullMapping(properties, expected);
    }
}
