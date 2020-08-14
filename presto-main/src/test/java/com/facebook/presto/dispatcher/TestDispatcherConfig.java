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
package com.facebook.presto.dispatcher;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestDispatcherConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(DispatcherConfig.class)
                .setRemoteQueryInfoMaxErrorDuration(new Duration(5, MINUTES))
                .setRemoteQueryInfoUpdateInterval(new Duration(3, SECONDS))
                .setRemoteQueryInfoRefreshMaxWait(new Duration(0, SECONDS))
                .setRemoteQueryInfoMaxCallbackThreads(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("dispatcher.remote-query-info.min-error-duration", "2m")
                .put("dispatcher.remote-query-info.update-interval", "10s")
                .put("dispatcher.remote-query-info.refresh-max-wait", "6s")
                .put("dispatcher.remote-query.max-callback-threads", "500")
                .build();

        DispatcherConfig expected = new DispatcherConfig()
                .setRemoteQueryInfoMaxErrorDuration(new Duration(2, MINUTES))
                .setRemoteQueryInfoUpdateInterval(new Duration(10, SECONDS))
                .setRemoteQueryInfoRefreshMaxWait(new Duration(6, SECONDS))
                .setRemoteQueryInfoMaxCallbackThreads(500);

        assertFullMapping(properties, expected);
    }
}