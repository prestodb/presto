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
package com.facebook.presto.verifier.framework;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.verifier.framework.QueryConfigurationOverrides.SessionPropertiesOverrideStrategy.NO_ACTION;
import static com.facebook.presto.verifier.framework.QueryConfigurationOverrides.SessionPropertiesOverrideStrategy.SUBSTITUTE;

public class TestQueryConfigurationOverridesConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(QueryConfigurationOverridesConfig.class)
                .setCatalogOverride(null)
                .setSchemaOverride(null)
                .setUsernameOverride(null)
                .setPasswordOverride(null)
                .setSessionPropertiesOverrideStrategy(NO_ACTION)
                .setSessionPropertiesOverride(null)
                .setSessionPropertiesToRemove(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("catalog-override", "_catalog")
                .put("schema-override", "_schema")
                .put("username-override", "_username")
                .put("password-override", "_password")
                .put("session-properties-override-strategy", "SUBSTITUTE")
                .put("session-properties-override", "{\"key\": \"value\"}")
                .put("session-properties-removal", "key2,key3")
                .build();
        QueryConfigurationOverridesConfig expected = new QueryConfigurationOverridesConfig()
                .setCatalogOverride("_catalog")
                .setSchemaOverride("_schema")
                .setUsernameOverride("_username")
                .setPasswordOverride("_password")
                .setSessionPropertiesOverrideStrategy(SUBSTITUTE)
                .setSessionPropertiesOverride("{\"key\": \"value\"}")
                .setSessionPropertiesToRemove("key2,key3");

        assertFullMapping(properties, expected);
    }
}
