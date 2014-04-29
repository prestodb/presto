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
package com.facebook.presto.operator;

import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

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
                .setExchangeMaxBufferSize(new DataSize(32, Unit.MEGABYTE))
                .setExchangeConcurrentRequestMultiplier(3)
                .setExchangeMaxResponseSize(new HttpClientConfig().getMaxContentLength()));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("exchange.max-buffer-size", "1GB")
                .put("exchange.concurrent-request-multiplier", "13")
                .put("exchange.max-response-size", "1kB")
                .build();

        ExchangeClientConfig expected = new ExchangeClientConfig()
                .setExchangeMaxBufferSize(new DataSize(1, Unit.GIGABYTE))
                .setExchangeConcurrentRequestMultiplier(13)
                .setExchangeMaxResponseSize(new DataSize(1, Unit.KILOBYTE));

        assertFullMapping(properties, expected);
    }
}
