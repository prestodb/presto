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
package com.facebook.presto.spark.execution.nativeprocess;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.ServerInfo;
import okhttp3.OkHttpClient;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetadataSidecarProcess
{
    @Test
    public void testWritesTimestampWithTimezoneMode()
            throws Exception
    {
        assertTimestampWithTimezoneMode(true);
        assertTimestampWithTimezoneMode(false);
    }

    private static void assertTimestampWithTimezoneMode(boolean legacyTimestampWithTimezone)
            throws Exception
    {
        Path configBasePath = Files.createTempDirectory("metadata-sidecar-test");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        try {
            MetadataSidecarProcess process = new MetadataSidecarProcess(
                    "/bin/echo",
                    "",
                    new OkHttpClient(),
                    executor,
                    scheduledExecutor,
                    jsonCodec(ServerInfo.class),
                    new Duration(1, SECONDS),
                    "test-oncall",
                    "test-user",
                    "test-service",
                    legacyTimestampWithTimezone);

            process.writeConfigsForTest(configBasePath);

            List<String> configLines = Files.readAllLines(configBasePath.resolve("config.properties"));
            assertThat(configLines)
                    .filteredOn(line -> line.startsWith("legacy-timestamp-with-timezone="))
                    .containsExactly("legacy-timestamp-with-timezone=" + legacyTimestampWithTimezone);
        }
        finally {
            executor.shutdownNow();
            scheduledExecutor.shutdownNow();
            deleteRecursively(configBasePath, ALLOW_INSECURE);
        }
    }
}
