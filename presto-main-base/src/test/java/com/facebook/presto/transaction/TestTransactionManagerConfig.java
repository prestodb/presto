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
package com.facebook.presto.transaction;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestTransactionManagerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(TransactionManagerConfig.class)
                .setIdleCheckInterval(new Duration(1, TimeUnit.MINUTES))
                .setIdleTimeout(new Duration(5, TimeUnit.MINUTES))
                .setCompanionCatalogs("")
                .setMaxFinishingConcurrency(1));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("transaction.idle-check-interval", "1s")
                .put("transaction.idle-timeout", "10s")
                .put("transaction.max-finishing-concurrency", "100")
                .put("transaction.companion-catalogs", "cat1=cat2,cat2=cat3")
                .build();

        TransactionManagerConfig expected = new TransactionManagerConfig()
                .setIdleCheckInterval(new Duration(1, TimeUnit.SECONDS))
                .setIdleTimeout(new Duration(10, TimeUnit.SECONDS))
                .setCompanionCatalogs("cat1=cat2,cat2=cat3")
                .setMaxFinishingConcurrency(100);

        assertFullMapping(properties, expected);
    }
}
