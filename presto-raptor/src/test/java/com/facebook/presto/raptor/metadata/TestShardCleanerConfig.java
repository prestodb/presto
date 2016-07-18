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
package com.facebook.presto.raptor.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestShardCleanerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ShardCleanerConfig.class)
                .setMaxTransactionAge(new Duration(1, DAYS))
                .setTransactionCleanerInterval(new Duration(10, MINUTES))
                .setLocalCleanerInterval(new Duration(1, HOURS))
                .setLocalCleanTime(new Duration(4, HOURS))
                .setBackupCleanerInterval(new Duration(5, MINUTES))
                .setBackupCleanTime(new Duration(1, DAYS))
                .setBackupDeletionThreads(50));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("raptor.max-transaction-age", "42m")
                .put("raptor.transaction-cleaner-interval", "43m")
                .put("raptor.local-cleaner-interval", "31m")
                .put("raptor.local-clean-time", "32m")
                .put("raptor.backup-cleaner-interval", "34m")
                .put("raptor.backup-clean-time", "35m")
                .put("raptor.backup-deletion-threads", "37")
                .build();

        ShardCleanerConfig expected = new ShardCleanerConfig()
                .setMaxTransactionAge(new Duration(42, MINUTES))
                .setTransactionCleanerInterval(new Duration(43, MINUTES))
                .setLocalCleanerInterval(new Duration(31, MINUTES))
                .setLocalCleanTime(new Duration(32, MINUTES))
                .setBackupCleanerInterval(new Duration(34, MINUTES))
                .setBackupCleanTime(new Duration(35, MINUTES))
                .setBackupDeletionThreads(37);

        assertFullMapping(properties, expected);
    }
}
