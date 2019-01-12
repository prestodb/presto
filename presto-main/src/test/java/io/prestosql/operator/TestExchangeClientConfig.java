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
package io.prestosql.operator;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit;

public class TestExchangeClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ExchangeClientConfig.class)
                .setMaxBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setConcurrentRequestMultiplier(3)
                .setMinErrorDuration(new Duration(5, TimeUnit.MINUTES))
                .setMaxErrorDuration(new Duration(5, TimeUnit.MINUTES))
                .setMaxResponseSize(new HttpClientConfig().getMaxContentLength())
                .setPageBufferClientMaxCallbackThreads(25)
                .setClientThreads(25)
                .setAcknowledgePages(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("exchange.max-buffer-size", "1GB")
                .put("exchange.concurrent-request-multiplier", "13")
                .put("exchange.min-error-duration", "13s")
                .put("exchange.max-error-duration", "33s")
                .put("exchange.max-response-size", "1MB")
                .put("exchange.client-threads", "2")
                .put("exchange.page-buffer-client.max-callback-threads", "16")
                .put("exchange.acknowledge-pages", "false")
                .build();

        ExchangeClientConfig expected = new ExchangeClientConfig()
                .setMaxBufferSize(new DataSize(1, Unit.GIGABYTE))
                .setConcurrentRequestMultiplier(13)
                .setMinErrorDuration(new Duration(33, TimeUnit.SECONDS))
                .setMaxErrorDuration(new Duration(33, TimeUnit.SECONDS))
                .setMaxResponseSize(new DataSize(1, Unit.MEGABYTE))
                .setClientThreads(2)
                .setPageBufferClientMaxCallbackThreads(16)
                .setAcknowledgePages(false);

        assertFullMapping(properties, expected);
    }
}
