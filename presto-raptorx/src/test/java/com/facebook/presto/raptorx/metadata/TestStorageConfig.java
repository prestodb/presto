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
package com.facebook.presto.raptorx.metadata;

import com.facebook.presto.raptorx.storage.StorageConfig;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestStorageConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(StorageConfig.class)
                .setDataDirectory(null)
                .setMinAvailableSpace(new DataSize(0, BYTE))
                .setOrcMaxMergeDistance(new DataSize(1, MEGABYTE))
                .setOrcMaxReadSize(new DataSize(8, MEGABYTE))
                .setOrcStreamBufferSize(new DataSize(8, MEGABYTE))
                .setOrcTinyStripeThreshold(new DataSize(8, MEGABYTE))
                .setDeletionThreads(max(1, getRuntime().availableProcessors() / 2))
                .setChunkRecoveryTimeout(new Duration(30, SECONDS))
                .setMaxChunkRows(1_000_000)
                .setMaxChunkSize(new DataSize(256, MEGABYTE))
                .setMaxBufferSize(new DataSize(256, MEGABYTE)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("storage.data-directory", "/data")
                .put("storage.min-available-space", "123GB")
                .put("storage.orc.max-merge-distance", "26kB")
                .put("storage.orc.max-read-size", "27kB")
                .put("storage.orc.stream-buffer-size", "28kB")
                .put("storage.orc.tiny-stripe-threshold", "29kB")
                .put("storage.max-deletion-threads", "999")
                .put("storage.chunk-recovery-timeout", "42s")
                .put("storage.max-chunk-rows", "99999")
                .put("storage.max-chunk-size", "99MB")
                .put("storage.max-buffer-size", "123MB")
                .build();

        StorageConfig expected = new StorageConfig()
                .setDataDirectory(new File("/data"))
                .setMinAvailableSpace(new DataSize(123, GIGABYTE))
                .setOrcMaxMergeDistance(new DataSize(26, KILOBYTE))
                .setOrcMaxReadSize(new DataSize(27, KILOBYTE))
                .setOrcStreamBufferSize(new DataSize(28, KILOBYTE))
                .setOrcTinyStripeThreshold(new DataSize(29, KILOBYTE))
                .setDeletionThreads(999)
                .setChunkRecoveryTimeout(new Duration(42, SECONDS))
                .setMaxChunkRows(99_999)
                .setMaxChunkSize(new DataSize(99, MEGABYTE))
                .setMaxBufferSize(new DataSize(123, MEGABYTE));

        assertFullMapping(properties, expected);
    }
}
