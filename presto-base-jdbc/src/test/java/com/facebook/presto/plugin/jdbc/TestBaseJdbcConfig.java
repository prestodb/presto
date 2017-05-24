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
package com.facebook.presto.plugin.jdbc;

import com.google.common.collect.ImmutableMap;

import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;

import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestBaseJdbcConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(BaseJdbcConfig.class)
                .setConnectionUrl(null)
                .setConnectionUser(null)
                .setConnectionPassword(null)
                .setConnectionAutoReconnect(true)
                .setConnectionMaxReconnects(3)
                .setConnectionTimeout(new Duration(3, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("connection-url", "jdbc:h2:mem:config")
                .put("connection-user", "user")
                .put("connection-password", "password")
                .put("connection-autoReconnect", "false")
                .put("connection-maxReconnects", "4")
                .put("connection-timeout", "4s")
                .build();

        BaseJdbcConfig expected = new BaseJdbcConfig()
                .setConnectionUrl("jdbc:h2:mem:config")
                .setConnectionUser("user")
                .setConnectionPassword("password")
                .setConnectionAutoReconnect(false)
                .setConnectionMaxReconnects(4)
                .setConnectionTimeout(new Duration(4, TimeUnit.SECONDS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
