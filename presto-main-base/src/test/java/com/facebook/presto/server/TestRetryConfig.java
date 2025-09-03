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
package com.facebook.presto.server;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;

public class TestRetryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(RetryConfig.class)
                .setRetryEnabled(true)
                .setRequireHttps(false)
                .setAllowedRetryDomains(null)
                .setCrossClusterRetryErrorCodes("REMOTE_TASK_ERROR"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("retry.enabled", "false")
                .put("retry.allowed-domains", "*.foo.bar,*.baz.qux")
                .put("retry.require-https", "true")
                .put("retry.cross-cluster-error-codes", "QUERY_QUEUE_FULL")
                .build();

        RetryConfig expected = new RetryConfig()
                .setRetryEnabled(false)
                .setRequireHttps(true)
                .setAllowedRetryDomains("*.foo.bar,*.baz.qux")
                .setCrossClusterRetryErrorCodes("QUERY_QUEUE_FULL");

        assertFullMapping(properties, expected);
    }
}
