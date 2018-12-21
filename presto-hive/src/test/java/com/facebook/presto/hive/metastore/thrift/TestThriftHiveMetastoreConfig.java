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
package com.facebook.presto.hive.metastore.thrift;

import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.testng.Assert.assertEquals;

public class TestThriftHiveMetastoreConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ThriftHiveMetastoreConfig.class)
                .setBackoffScaleFactor(2.0)
                .setMaxBackoffDelay(new Duration(1, TimeUnit.SECONDS))
                .setMaxRetries(10)
                .setMaxRetryTime(new Duration(30, TimeUnit.SECONDS))
                .setMinBackoffDelay(new Duration(1, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitParameterSetup()
    {
        ThriftHiveMetastoreConfig config = new ThriftHiveMetastoreConfig()
                .setBackoffScaleFactor(3.0)
                .setMaxBackoffDelay(new Duration(4, TimeUnit.SECONDS))
                .setMinBackoffDelay(new Duration(2, TimeUnit.SECONDS))
                .setMaxRetries(15)
                .setMaxRetryTime(new Duration(60, TimeUnit.SECONDS));
        assertEquals(3.0, config.getBackoffScaleFactor());
        assertEquals(new Duration(4, TimeUnit.SECONDS), config.getMaxBackoffDelay());
        assertEquals(new Duration(2, TimeUnit.SECONDS), config.getMinBackoffDelay());
        assertEquals(15, config.getMaxRetries());
        assertEquals(new Duration(60, TimeUnit.SECONDS), config.getMaxRetryTime());
    }
}
