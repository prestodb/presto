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
package com.facebook.presto.verifier.resolver;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestTooManyOpenPartitionsFailureResolverConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(TooManyOpenPartitionsFailureResolverConfig.class)
                .setMaxBucketsPerWriter(100)
                .setClusterSizeExpiration(new Duration(30, MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("failure-resolver.max-buckets-per-writer", "50")
                .put("failure-resolver.cluster-size-expiration", "10m")
                .build();
        TooManyOpenPartitionsFailureResolverConfig expected = new TooManyOpenPartitionsFailureResolverConfig()
                .setMaxBucketsPerWriter(50)
                .setClusterSizeExpiration(new Duration(10, MINUTES));

        assertFullMapping(properties, expected);
    }
}
