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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.mapJsonCodec;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.verifier.framework.QueryConfigurationOverrides.SessionPropertiesOverrideStrategy.NO_ACTION;
import static com.facebook.presto.verifier.framework.QueryConfigurationOverrides.SessionPropertiesOverrideStrategy.OVERRIDE;
import static com.facebook.presto.verifier.framework.QueryConfigurationOverrides.SessionPropertiesOverrideStrategy.SUBSTITUTE;

@Test(singleThreaded = true)
public class TestQueryConfiguration
{
    private static final String CATALOG = "catalog";
    private static final String SCHEMA = "schema";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    private static final String CATALOG_OVERRIDE = "override_catalog";
    private static final String SCHEMA_OVERRIDE = "override_schema";
    private static final String USERNAME_OVERRIDE = "override_username";
    private static final String PASSWORD_OVERRIDE = "override_password";

    private static final Map<String, String> SESSION_PROPERTIES = ImmutableMap.of("property_1", "value_1", "property_2", "value_2");
    private static final Map<String, String> SESSION_PROPERTIES_OVERRIDE = ImmutableMap.of("property_1", "value_x", "property_3", "value_3");
    private static final String SESSION_PROPERTIES_OVERRIDE_CONFIG = mapJsonCodec(String.class, String.class).toJson(SESSION_PROPERTIES_OVERRIDE);

    private static final QueryConfiguration CONFIGURATION_1 = new QueryConfiguration(CATALOG, SCHEMA, Optional.of(USERNAME), Optional.of(PASSWORD), Optional.of(SESSION_PROPERTIES));
    private static final QueryConfiguration CONFIGURATION_2 = new QueryConfiguration(CATALOG, SCHEMA, Optional.empty(), Optional.empty(), Optional.empty());
    private static final QueryConfiguration CONFIGURATION_FULL_OVERRIDE = new QueryConfiguration(
            CATALOG_OVERRIDE,
            SCHEMA_OVERRIDE,
            Optional.of(USERNAME_OVERRIDE),
            Optional.of(PASSWORD_OVERRIDE),
            Optional.of(SESSION_PROPERTIES_OVERRIDE));

    private QueryConfigurationOverridesConfig overrides;

    @BeforeMethod
    public void setup()
    {
        overrides = new QueryConfigurationOverridesConfig()
                .setCatalogOverride(CATALOG_OVERRIDE)
                .setSchemaOverride(SCHEMA_OVERRIDE)
                .setUsernameOverride(USERNAME_OVERRIDE)
                .setPasswordOverride(PASSWORD_OVERRIDE)
                .setSessionPropertiesOverride(SESSION_PROPERTIES_OVERRIDE_CONFIG);
    }

    @Test
    public void testEmptyOverrides()
    {
        assertEquals(CONFIGURATION_1.applyOverrides(new QueryConfigurationOverridesConfig()), CONFIGURATION_1);
        assertEquals(CONFIGURATION_2.applyOverrides(new QueryConfigurationOverridesConfig()), CONFIGURATION_2);
    }

    @Test
    public void testOverrides()
    {
        assertEquals(
                CONFIGURATION_1.applyOverrides(overrides),
                new QueryConfiguration(
                        CATALOG_OVERRIDE,
                        SCHEMA_OVERRIDE,
                        Optional.of(USERNAME_OVERRIDE),
                        Optional.of(PASSWORD_OVERRIDE),
                        Optional.of(SESSION_PROPERTIES)));
        assertEquals(CONFIGURATION_2.applyOverrides(overrides),
                new QueryConfiguration(
                        CATALOG_OVERRIDE,
                        SCHEMA_OVERRIDE,
                        Optional.of(USERNAME_OVERRIDE),
                        Optional.of(PASSWORD_OVERRIDE),
                        Optional.empty()));
    }

    @Test
    public void testSessionPropertyOverride()
    {
        overrides.setSessionPropertiesOverrideStrategy(OVERRIDE);
        assertEquals(CONFIGURATION_1.applyOverrides(overrides), CONFIGURATION_FULL_OVERRIDE);
        assertEquals(CONFIGURATION_2.applyOverrides(overrides), CONFIGURATION_FULL_OVERRIDE);
    }

    @Test
    public void testSessionPropertySubstitute()
    {
        overrides.setSessionPropertiesOverrideStrategy(SUBSTITUTE);
        QueryConfiguration substituted = new QueryConfiguration(
                CATALOG_OVERRIDE,
                SCHEMA_OVERRIDE,
                Optional.of(USERNAME_OVERRIDE),
                Optional.of(PASSWORD_OVERRIDE),
                Optional.of(ImmutableMap.of("property_1", "value_x", "property_2", "value_2", "property_3", "value_3")));

        assertEquals(CONFIGURATION_1.applyOverrides(overrides), substituted);
        assertEquals(CONFIGURATION_2.applyOverrides(overrides), CONFIGURATION_FULL_OVERRIDE);
    }

    @Test
    public void testSessionPropertyRemovalWithOverrides()
    {
        overrides.setSessionPropertiesToRemove("property_1, property_2");
        overrides.setSessionPropertiesOverrideStrategy(OVERRIDE);

        QueryConfiguration removed = new QueryConfiguration(
                CATALOG_OVERRIDE,
                SCHEMA_OVERRIDE,
                Optional.of(USERNAME_OVERRIDE),
                Optional.of(PASSWORD_OVERRIDE),
                Optional.of(ImmutableMap.of("property_3", "value_3")));

        assertEquals(CONFIGURATION_1.applyOverrides(overrides), removed);
    }

    @Test
    public void testSessionPropertySubstituteAndRemove()
    {
        overrides.setSessionPropertiesToRemove("property_2");
        overrides.setSessionPropertiesOverrideStrategy(SUBSTITUTE);
        QueryConfiguration removed = new QueryConfiguration(
                CATALOG_OVERRIDE,
                SCHEMA_OVERRIDE,
                Optional.of(USERNAME_OVERRIDE),
                Optional.of(PASSWORD_OVERRIDE),
                Optional.of(SESSION_PROPERTIES_OVERRIDE));

        assertEquals(CONFIGURATION_1.applyOverrides(overrides), removed);
    }

    @Test
    public void testSessionPropertyRemoval()
    {
        overrides.setSessionPropertiesToRemove("property_2");
        overrides.setSessionPropertiesOverrideStrategy(NO_ACTION);
        QueryConfiguration removed = new QueryConfiguration(
                CATALOG_OVERRIDE,
                SCHEMA_OVERRIDE,
                Optional.of(USERNAME_OVERRIDE),
                Optional.of(PASSWORD_OVERRIDE),
                Optional.of(ImmutableMap.of("property_1", "value_1")));

        assertEquals(CONFIGURATION_1.applyOverrides(overrides), removed);
    }
}
