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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestRetryConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(RetryConfig.class)
                .setMaxAttempts(1)
                .setMinBackoffDelay(new Duration(0, NANOSECONDS))
                .setMaxBackoffDelay(new Duration(1, MINUTES))
                .setScaleFactor(1.0));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("max-attempts", "10")
                .put("min-backoff-delay", "500ms")
                .put("max-backoff-delay", "10s")
                .put("backoff-scale-factor", "2.0")
                .build();
        RetryConfig expected = new RetryConfig()
                .setMaxAttempts(10)
                .setMinBackoffDelay(new Duration(500, MILLISECONDS))
                .setMaxBackoffDelay(new Duration(10, SECONDS))
                .setScaleFactor(2.0);

        assertFullMapping(properties, expected);
    }
}
